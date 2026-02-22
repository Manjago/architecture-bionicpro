# BionicPRO — Спринт 9: SSO + OLAP + CDC

> Проектная работа 9 спринта курса "Архитектор ПО PRO"

## Обзор

В этом спринте решаем четыре задачи:

| # | Задание | Суть | Статус |
|---|---------|------|--------|
| 1 | Повышение безопасности | BFF + PKCE + LDAP + MFA + Яндекс ID | ✅ Готово |
| 2 | Сервис отчётов | Airflow ETL → ClickHouse → Report API | ⏳ |
| 3 | Снижение нагрузки на БД | S3 + CDN (Nginx) кэширование отчётов | ⏳ |
| 4 | Оперативность CRM | CDC через Debezium → Kafka → ClickHouse | ⏳ |

## Структура репозитория

```
architecture-bionicpro/
├── bionicpro-auth/          ← Задание 1: BFF-сервис (Java 25, Javalin)
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/main/java/bionicpro/auth/
│       ├── AuthServer.java              ← Точка входа, роутинг, конфиг из env
│       ├── handler/
│       │   ├── LoginHandler.java        ← GET /auth/login (PKCE + redirect)
│       │   ├── CallbackHandler.java     ← GET /auth/callback (token exchange)
│       │   ├── LogoutHandler.java       ← POST /auth/logout
│       │   └── ProxyHandler.java        ← GET/POST /api/** (session + proxy)
│       ├── session/
│       │   ├── SessionStore.java        ← Interface (→ Redis в проде)
│       │   ├── InMemorySessionStore.java← ConcurrentHashMap + cleanup
│       │   └── SessionData.java         ← Record: tokens + user info
│       ├── keycloak/
│       │   └── KeycloakClient.java      ← Token exchange, refresh, JWT parse
│       └── util/
│           ├── PkceUtil.java            ← code_verifier / code_challenge / SecureRandom ID
│           └── CookieUtil.java          ← HttpOnly + SameSite=Strict cookies
├── frontend/                ← Обновлённый фронтенд (без keycloak-js)
│   ├── src/
│   │   ├── App.tsx                      ← Убран ReactKeycloakProvider
│   │   └── components/
│   │       └── ReportPage.tsx           ← credentials: 'include' вместо Bearer token
│   └── package.json                     ← Удалены keycloak-js, @react-keycloak/web
├── keycloak/
│   ├── realm-export.json                ← Исходный конфиг (не трогаем)
│   └── keycloak-results-export.json     ← Наш результат (BFF + PKCE + LDAP + MFA + Яндекс)
├── ldap/
│   └── config.ldif                      ← Исправлен баг uid=alex → uid=alex.johnson
├── diagrams/                ← PlantUML-диаграммы
│   ├── C4_BionicPRO_Target.puml / .png
│   ├── Auth_Login_Flow.puml / .png
│   ├── Auth_API_Request_Flow.puml / .png
│   └── Auth_Logout_Flow.puml / .png
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
| Сессии | Нет серверных сессий | In-Memory `ConcurrentHashMap` (session_id → tokens). В проде → Redis |
| Identity | Keycloak standalone | Keycloak + LDAP (User Federation) + Яндекс ID (Brokering) |
| MFA | Нет | TOTP (Google Authenticator / FreeOTP) |
| Аналитика | PostgreSQL (перегружен) | ClickHouse (OLAP) + витрины |
| Данные | Batch-only | Airflow (batch) + Kafka/Debezium (CDC) |
| Отчёты | Нет | Report Service → S3 → Nginx (CDN cache) |

Все оригинальные компоненты (Программа в чипе протеза, Приложение для донастройки, Интернет-магазин, CRM, cli tool) **сохранены** на диаграмме.

---

### Задача 1.1 — Архитектурное решение

→ `diagrams/C4_BionicPRO_Target.puml`

Целевая C4 Container Diagram включает все существующие компоненты + новые: `bionicpro-auth` (BFF), Session Store, ClickHouse, Airflow, Kafka/Debezium, Report Service, S3/MinIO, Nginx, OpenLDAP, Яндекс ID.

---

### Задача 1.2 — PKCE

→ `diagrams/Auth_Login_Flow.puml` (шаги 4–19)
→ `bionicpro-auth/.../util/PkceUtil.java`
→ `bionicpro-auth/.../handler/LoginHandler.java` + `CallbackHandler.java`

Client `reports-frontend` в Keycloak переведён с `publicClient: true` на **confidential** + PKCE S256:

```json
{
  "clientId": "reports-frontend",
  "publicClient": false,
  "secret": "bff-client-secret-change-me",
  "redirectUris": ["http://localhost:8000/auth/callback"],
  "directAccessGrantsEnabled": false,
  "attributes": { "pkce.code.challenge.method": "S256" }
}
```

`redirect_uri` указывает на BFF (`localhost:8000/auth/callback`), **не** на фронтенд.

PKCE реализация (`PkceUtil.java`):
- `code_verifier` — 32 байта `SecureRandom` → Base64URL (43 символа)
- `code_challenge` — `BASE64URL(SHA-256(code_verifier))`
- `state` — 32 байта `SecureRandom` → hex (64 символа), CSRF-защита

Login flow: `LoginHandler` генерирует PKCE-пару, сохраняет `code_verifier` на сервере (привязанный к `state`), и редиректит пользователя на Keycloak с `code_challenge`. Callback: `CallbackHandler` обменивает `code + code_verifier + client_secret` на токены.

---

### Задача 1.3 — Безопасное хранение токенов (bionicpro-auth)

→ `bionicpro-auth/` — полный исходный код Java-сервиса
→ `diagrams/Auth_API_Request_Flow.puml` (session rotation + token refresh)

**Стек:** Java 25, Javalin 6.4, Jackson, Logback. Maven. Без Spring.

**Эндпоинты:**

| Метод | Путь | Handler | Описание |
|-------|------|---------|----------|
| GET | `/auth/login` | `LoginHandler` | Генерирует PKCE, редиректит на Keycloak |
| GET | `/auth/callback` | `CallbackHandler` | Обменивает code на токены, создаёт сессию, Set-Cookie |
| POST | `/auth/logout` | `LogoutHandler` | Удаляет сессию, Keycloak SSO logout |
| GET/POST | `/api/**` | `ProxyHandler` | Валидация + ротация + refresh + проксирование |
| GET | `/health` | inline | `{"status":"UP","sessions":N}` |

**Безопасность сессий:**
- Session ID — `SecureRandom(32 bytes)` → hex (256 бит энтропии). Не UUID.
- Cookie: `BIONIC_SESSION=<id>; HttpOnly; SameSite=Strict; Path=/; Max-Age=1800`
- Ротация: при каждом запросе `getAndRemove(oldId)` → обработка → `put(newId)`. Атомарно.
- Refresh: `ProxyHandler` проверяет `accessTokenExpiresAt`, вызывает Keycloak `/token` с `grant_type=refresh_token`
- Cleanup: фоновый поток каждые 5 минут удаляет истёкшие сессии

**Хранилище:** `InMemorySessionStore` (`ConcurrentHashMap`) реализует интерфейс `SessionStore`. Замена на Redis — одна имплементация, остальной код не меняется.

**Smoke-тест пройден:**

```bash
$ curl -s http://localhost:8000/health | jq .
{"sessions":0,"status":"UP"}

$ curl -v http://localhost:8000/auth/login 2>&1 | grep "Location"
< Location: http://localhost:8080/realms/reports-realm/.../auth?...&code_challenge=...&code_challenge_method=S256

$ curl -s http://localhost:8000/api/reports/me | jq .
{"error":"Not authenticated. Please login."}
```

---

### Задача 1.4 — LDAP

→ `ldap/config.ldif`
→ `keycloak/keycloak-results-export.json` (секция `components`)

**Исправлен баг** в исходном `config.ldif`: DN записи Alex'а был `uid=alex`, а атрибут `uid` — `alex.johnson`. Группа `prothetic_user` ссылалась на `uid=alex.johnson` → member не находился. Исправлено: `uid=alex.johnson` и в DN, и в атрибуте.

Добавлена группа `administrator` (отсутствовала в оригинале).

Keycloak User Federation настроен на `ldap://openldap:389`:
- Edit Mode: `READ_ONLY`
- Group-to-Role маппинг: LDAP-группы (`cn=prothetic_user,ou=Groups,...`) → Keycloak realm roles
- Attribute mappers: `uid→username`, `mail→email`, `cn→firstName`, `sn→lastName`

---

### Задача 1.5 — MFA (OTP)

→ `keycloak/keycloak-results-export.json` (секции `otpPolicy*`, `requiredActions`, `users[].requiredActions`)

Настройка:
- OTP Policy: TOTP, HmacSHA1, 6 digits, 30 sec, Look Ahead Window = 1
- Required Action `CONFIGURE_TOTP` включён как Default Action
- У всех пользователей в `requiredActions` добавлен `CONFIGURE_TOTP`
- При первом логине Keycloak покажет QR-код для Google Authenticator / FreeOTP

---

### Задача 1.6 — Яндекс ID

→ `keycloak/keycloak-results-export.json` (секция `identityProviders`)

Identity Provider настроен как OpenID Connect:
- Authorization URL: `https://oauth.yandex.ru/authorize`
- Token URL: `https://oauth.yandex.ru/token`
- UserInfo URL: `https://login.yandex.ru/info`
- Scope: `login:email login:info`
- `clientId` / `clientSecret` — плейсхолдеры (`YANDEX_CLIENT_ID_PLACEHOLDER`). Для активации необходимо зарегистрировать приложение на https://oauth.yandex.ru/ и подставить реальные значения.

---

### Изменения во фронтенде

Из `package.json` удалены зависимости:
- `keycloak-js` (^21.1.0)
- `@react-keycloak/web` (^3.4.0)

`App.tsx` — убран `ReactKeycloakProvider`, чистый рендер без знания о Keycloak.

`ReportPage.tsx` — полностью переписан:

| Аспект | Было (keycloak-js) | Стало (BFF) |
|--------|---------------------|-------------|
| Логин | `keycloak.login()` | `window.location.href = '/auth/login'` |
| Токен | `keycloak.token` в `Authorization` header | `credentials: 'include'` (cookie автоматически) |
| Логаут | `keycloak.logout()` | `POST /auth/logout` (form submit) |
| Знание о Keycloak | Да (URL, realm, clientId) | Нет. Фронтенд знает только BFF URL |

---

### Изменения в docker-compose.yaml

- Добавлен сервис `bionicpro-auth` (build из `./bionicpro-auth`, порт 8000)
- Keycloak импортирует `keycloak-results-export.json` (вместо `realm-export.json`)
- Frontend: env vars `REACT_APP_KEYCLOAK_*` заменены на `REACT_APP_BFF_URL`

---

### Deliverables задания 1

- [x] C4 Container Diagram целевой архитектуры (`diagrams/C4_BionicPRO_Target.puml`)
- [x] Sequence Diagram: Login Flow с PKCE + MFA (`diagrams/Auth_Login_Flow.puml`)
- [x] Sequence Diagram: API Request с ротацией и refresh (`diagrams/Auth_API_Request_Flow.puml`)
- [x] Sequence Diagram: Logout Flow (`diagrams/Auth_Logout_Flow.puml`)
- [x] Код PKCE flow (`bionicpro-auth/.../PkceUtil.java`, `LoginHandler.java`, `CallbackHandler.java`)
- [x] Код BFF-сервиса: токены на сервере, HttpOnly cookie, ротация, refresh (`bionicpro-auth/`)
- [x] Обновлённый фронтенд без `keycloak-js` (`frontend/`)
- [x] Keycloak realm config: confidential client + PKCE + LDAP + MFA + Яндекс ID (`keycloak/keycloak-results-export.json`)
- [x] LDAP config с исправленным багом (`ldap/config.ldif`)
- [x] Обновлённый `docker-compose.yaml`
- [ ] Интеграционный тест: полный auth flow (Keycloak + BFF + Frontend). Требует `docker-compose up` и регистрации приложения в Яндекс ID.

---

*Задания 2–4 будут дополнены по мере выполнения.*