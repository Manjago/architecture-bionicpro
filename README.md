# BionicPRO — Спринт 9: SSO + OLAP + CDC

> Проектная работа 9 спринта курса "Архитектор ПО PRO"

## Обзор

В этом спринте решаем четыре задачи:

| # | Задание | Суть | Статус |
|---|---------|------|--------|
| 1 | Повышение безопасности | BFF + PKCE + LDAP + MFA + Яндекс ID | 🔧 В работе |
| 2 | Сервис отчётов | Airflow ETL → ClickHouse → Report API | ⏳ |
| 3 | Снижение нагрузки на БД | S3 + CDN (Nginx) кэширование отчётов | ⏳ |
| 4 | Оперативность CRM | CDC через Debezium → Kafka → ClickHouse | ⏳ |

## Структура репозитория

```
architecture-bionicpro/
├── bionicpro-auth/          ← Задание 1: BFF-сервис (Java)
├── frontend/                ← Обновлённый фронтенд (без keycloak-js)
├── keycloak/                ← Конфигурация Keycloak (realm-export)
├── ldap/                    ← Конфигурация OpenLDAP (config.ldif)
├── diagrams/                ← PlantUML-диаграммы
├── airflow/                 ← Задание 2: DAG для ETL
├── report-service/          ← Задание 2: API /reports (Java)
├── olap-db/                 ← ClickHouse: init-скрипты, витрины
├── crm-db/                  ← CRM: init-скрипт + данные (CSV)
├── nginx/                   ← Задание 3: Nginx reverse proxy + cache
├── debezium/                ← Задание 4: CDC connector config
├── docker-compose.yaml      ← Полная конфигурация развёртывания
└── README.md                ← Этот файл
```

---

## Задание 1. Повышение безопасности системы

### Проблема

Исходная архитектура использовала OAuth 2.0 Code Grant напрямую из фронтенда (`keycloak-js`). Токены (`access_token`, `refresh_token`) хранились в браузере — в `localStorage` или памяти JavaScript. Это привело к взлому: XSS-атака позволила украсть токены и скачать персональные данные пользователей.

### Решение: BFF + PKCE

Внедряем **Backend for Frontend (BFF)** паттерн — новый сервис `bionicpro-auth`, который:

1. Реализует PKCE (Proof Key for Code Exchange) поверх Code Grant
2. Получает и хранит токены **на сервере** (фронтенд их никогда не видит)
3. Выдаёт фронтенду только сессионную cookie (`HttpOnly`, `Secure`, `SameSite=Strict`)
4. Проксирует запросы к API, подставляя `Authorization: Bearer <token>`
5. Автоматически обновляет `access_token` через `refresh_token`
6. Ротирует `session_id` при каждом запросе (защита от session fixation)

### Диаграммы

Все диаграммы — в формате PlantUML (исходники в `diagrams/`). Рендер: https://www.planttext.com/

#### C4 Container Diagram (Target Architecture)

Целевая архитектура после выполнения всех заданий спринта 9.

[Исходник PlantUML](diagrams/C4_BionicPRO_Target.puml)

![C4 Container Diagram](diagrams/C4_BionicPRO_Target.png)

#### Login Flow (BFF + PKCE + MFA)

Полный flow логина: PKCE → Keycloak (пароль + OTP) → BFF получает токены → создание сессии → HttpOnly cookie.

[Исходник PlantUML](diagrams/Auth_Login_Flow.puml)

![Login Flow](diagrams/Auth_Login_Flow.png)

#### API Request Flow (Session Rotation + Token Refresh)

Запрос к защищённому ресурсу: проверка сессии → ротация session_id → refresh токена → проксирование к API.

[Исходник PlantUML](diagrams/Auth_API_Request_Flow.puml)

![API Request Flow](diagrams/Auth_API_Request_Flow.png)

#### Logout Flow

Завершение сессии: удаление серверной сессии → Keycloak SSO logout → очистка куки.

[Исходник PlantUML](diagrams/Auth_Logout_Flow.puml)

![Logout Flow](diagrams/Auth_Logout_Flow.png)

### C4 Container Diagram: ключевые изменения

По сравнению с исходной архитектурой:

| Компонент | Было | Стало |
|-----------|------|-------|
| Аутентификация | `keycloak-js` на фронте, токены в браузере | `bionicpro-auth` (BFF), токены на сервере |
| Сессии | Нет серверных сессий | Redis / In-Memory (session_id → tokens) |
| Identity | Keycloak standalone | Keycloak + LDAP (User Federation) + Яндекс ID (Brokering) |
| MFA | Нет | TOTP (Google Authenticator / FreeOTP) |
| Аналитика | PostgreSQL (перегружен) | ClickHouse (OLAP) + витрины |
| Данные | Batch-only | Airflow (batch) + Kafka/Debezium (CDC) |
| Отчёты | Нет | Report Service → S3 → Nginx (CDN cache) |

Все оригинальные компоненты (Программа в чипе протеза, Приложение для донастройки, Интернет-магазин, CRM, cli tool) **сохранены** на диаграмме.

### Задача 1.1 — Архитектурное решение

→ См. `diagrams/C4_BionicPRO_Target.puml`

### Задача 1.2 — PKCE

Client `reports-frontend` в Keycloak переведён с `publicClient: true` на confidential + PKCE S256. `redirect_uri` указывает на BFF (`localhost:8000/auth/callback`), не на фронтенд.

→ См. `diagrams/Auth_Login_Flow.puml` (шаги 4-19)

### Задача 1.3 — Безопасное хранение токенов (bionicpro-auth)

→ См. `bionicpro-auth/` — Java-сервис, реализующий:
- Получение токенов от Keycloak (PKCE flow)
- Хранение токенов в серверной сессии
- HttpOnly cookie для фронтенда
- Автоматический refresh access_token
- Ротация session_id при каждом запросе
- Проксирование запросов к API с Bearer Token

→ См. `diagrams/Auth_API_Request_Flow.puml` (session rotation + token refresh)

### Задача 1.4 — LDAP

→ См. `ldap/config.ldif` — пользователи и роли для OpenLDAP  
→ Keycloak User Federation настроен на `ldap://openldap:389`  
→ Group-to-Role маппинг: LDAP группы → Keycloak realm roles

### Задача 1.5 — MFA (OTP)

→ Keycloak OTP Policy: TOTP, SHA1, 6 digits, 30 sec  
→ Required Action: Configure OTP (обязателен для всех пользователей)  
→ Browser Flow: OTP Form = Required

### Задача 1.6 — Яндекс ID

→ Keycloak Identity Provider: OpenID Connect → Яндекс ID  
→ Authorization URL: `https://oauth.yandex.ru/authorize`  
→ Consent screen + сохранение профиля

### Проверка (deliverables задания 1)

- [x] Диаграмма архитектуры (PlantUML вместо draw.io)
- [ ] Код PKCE flow в `bionicpro-auth/`
- [ ] Код BFF-сервиса (токены + сессии)
- [ ] Обновлённый фронтенд (без `keycloak-js`)
- [ ] `keycloak/keycloak-results-export.json`
- [ ] OAuth 2.0 от Яндекс ID

---

*Задания 2-4 будут дополнены по мере выполнения.*
