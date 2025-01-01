-- Создание схемы logs и таблицы logging
CREATE SCHEMA IF NOT EXISTS logs;

CREATE TABLE IF NOT EXISTS logs.logging (
    log_level VARCHAR(10) DEFAULT 'INFO' NOT NULL,
    log_date DATE DEFAULT NOW() NOT NULL,
    log_message TEXT
);