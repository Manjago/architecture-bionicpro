-- =============================================================================
-- Задание 4: CDC pipeline — Kafka → ClickHouse
-- Файл: olap-db/init-cdc.sql
--
-- Выполняется ПОСЛЕ init.sql (задание 2).
-- Не трогает существующие таблицы (emg_sensor_data, user_reports).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Kafka queue: сырые JSON-события из топика crm.public.customers
-- -----------------------------------------------------------------------------
-- KafkaEngine — виртуальная таблица-очередь.
-- Каждая строка = одно Debezium-событие в формате JSONAsString.
-- SELECT из неё забирает сообщения и коммитит offset.
-- Напрямую не читаем — только через MaterializedView.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS crm_customers_queue (
    raw String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'crm.public.customers',
    kafka_group_name = 'clickhouse-crm-cdc',
    kafka_format = 'JSONAsString',
    kafka_max_block_size = 1048576,
    kafka_poll_timeout_ms = 1000;


-- -----------------------------------------------------------------------------
-- 2. Target table: CRM-клиенты (CDC-реплика)
-- -----------------------------------------------------------------------------
-- ReplacingMergeTree(_ts) — при merge оставляет только самую свежую
-- версию строки с одинаковым ORDER BY (id).
--
-- Колонка _is_deleted: при DELETE Debezium (с delete.handling.mode=rewrite)
-- отправляет событие с полем __deleted=true. Фильтруем при чтении.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS crm_customers (
    id          UInt32,
    name        String,
    email       String,
    age         UInt8,
    gender      String,
    country     String,
    address     String,
    phone       String,
    _op         String,
    _is_deleted UInt8,
    _ts         DateTime64(3)
) ENGINE = ReplacingMergeTree(_ts)
ORDER BY id;


-- -----------------------------------------------------------------------------
-- 3. MaterializedView: JSON из Kafka → crm_customers
-- -----------------------------------------------------------------------------
-- Автоматически срабатывает при появлении новых сообщений в KafkaEngine.
-- Парсит JSON, извлекает поля из payload.after (для INSERT/UPDATE)
-- или payload.before (для DELETE — after будет null).
--
-- Формат Debezium (schemas.enable=false):
-- {
--   "payload": {
--     "before": {...} | null,
--     "after":  {...} | null,
--     "op": "c" | "u" | "d" | "r",
--     "ts_ms": 1708123456789,
--     "source": {...}
--   }
-- }
--
-- С delete.handling.mode=rewrite: при DELETE поле after содержит данные
-- + дополнительное поле "__deleted": "true".
-- -----------------------------------------------------------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS crm_customers_mv TO crm_customers AS
SELECT
    -- Для DELETE after может быть null → берём из before
    coalesce(
        JSONExtractUInt(raw, 'payload', 'after', 'id'),
        JSONExtractUInt(raw, 'payload', 'before', 'id')
    ) AS id,

    JSONExtractString(raw, 'payload', 'after', 'name')    AS name,
    JSONExtractString(raw, 'payload', 'after', 'email')   AS email,

    toUInt8(coalesce(
        JSONExtractUInt(raw, 'payload', 'after', 'age'),
        0
    )) AS age,

    JSONExtractString(raw, 'payload', 'after', 'gender')  AS gender,
    JSONExtractString(raw, 'payload', 'after', 'country') AS country,
    JSONExtractString(raw, 'payload', 'after', 'address') AS address,
    JSONExtractString(raw, 'payload', 'after', 'phone')   AS phone,

    -- Тип операции: c=create, u=update, d=delete, r=read(snapshot)
    JSONExtractString(raw, 'payload', 'op') AS _op,

    -- delete.handling.mode=rewrite: Debezium добавляет __deleted в after
    if(
        JSONExtractString(raw, 'payload', 'after', '__deleted') = 'true'
        OR JSONExtractString(raw, 'payload', 'op') = 'd',
        1, 0
    ) AS _is_deleted,

    fromUnixTimestamp64Milli(
        JSONExtractUInt(raw, 'payload', 'ts_ms')
    ) AS _ts

FROM crm_customers_queue;


-- -----------------------------------------------------------------------------
-- 4. Витрина: объединение CRM (CDC) + телеметрия (batch)
-- -----------------------------------------------------------------------------
-- VIEW (не MATERIALIZED VIEW) — каждый SELECT пересчитывает на лету.
-- Это осознанный выбор:
--   - CRM-данные обновляются в near real-time через CDC
--   - MV пересчитывался бы только при INSERT в источник, а нам нужен
--     JOIN двух таблиц, одна из которых (emg_sensor_data) заполняется Airflow
--   - Для учебного проекта VIEW достаточно; в проде → периодический INSERT
--
-- Структура витрины совпадает с user_reports из задания 2 + добавлено
-- поле source='cdc' для отладки.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW user_reports_cdc AS
SELECT
    c.id                                        AS user_id,
    c.name                                      AS customer_name,
    c.email                                     AS customer_email,
    e.prosthesis_type                           AS prosthesis_type,
    count()                                     AS total_signals,
    round(avg(e.signal_amplitude), 2)           AS avg_amplitude,
    round(avg(e.signal_frequency), 0)           AS avg_frequency,
    round(avg(e.signal_duration), 0)            AS avg_duration,
    min(e.signal_time)                          AS min_signal_time,
    max(e.signal_time)                          AS max_signal_time,
    now()                                       AS report_updated
FROM crm_customers FINAL AS c
INNER JOIN emg_sensor_data AS e
    ON c.id = e.user_id
WHERE c._is_deleted = 0
GROUP BY
    c.id,
    c.name,
    c.email,
    e.prosthesis_type;
