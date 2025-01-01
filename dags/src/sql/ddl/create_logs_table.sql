-- Создание схемы LOGS и таблицы logging
CREATE SCHEMA IF NOT EXISTS "LOGS";

CREATE TABLE IF NOT EXISTS "LOGS"."logging" (
    log_level VARCHAR(10) DEFAULT "INFO" NOT NULL,
    log_date DATE DEFAULT NOW() NOT NULL,
    log_message TEXT
);