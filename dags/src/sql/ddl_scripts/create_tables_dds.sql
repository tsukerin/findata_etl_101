-- Создание схемы ds и импортируемых таблиц

CREATE SCHEMA IF NOT EXISTS ds;

CREATE TABLE IF NOT EXISTS ds.ft_balance_f(
    on_date DATE NOT NULL,
    account_rk INTEGER NOT NULL,
    currency_rk INTEGER,
    balance_out FLOAT
);

CREATE TABLE IF NOT EXISTS ds.ft_posting_f(
    oper_date DATE NOT NULL,
    credit_account_rk INTEGER NOT NULL,
    debet_account_rk INTEGER NOT NULL,
    credit_amount FLOAT,
    debet_amount FLOAT
);

CREATE TABLE IF NOT EXISTS ds.md_account_d(
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE NOT NULL,
    account_rk INTEGER NOT NULL,
    account_number VARCHAR(20) NOT NULL,
    char_type VARCHAR(1) NOT NULL,
    currency_rk INTEGER NOT NULL,
    currency_code VARCHAR(3) NOT NULL
);

CREATE TABLE IF NOT EXISTS ds.md_currency_d(
    currency_rk INTEGER NOT NULL,
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE,
    currency_code VARCHAR(3),
    code_iso_char VARCHAR(3)
);

CREATE TABLE IF NOT EXISTS ds.md_exchange_rate_d(
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE,
    currency_rk INTEGER NOT NULL,
    reduced_cource FLOAT,
    code_iso_num VARCHAR(3)
);

CREATE TABLE IF NOT EXISTS ds.md_ledger_account_s(
    chapter CHAR(1),
    chapter_name VARCHAR(16),
    section_number INTEGER,
    section_name VARCHAR(22),
    subsection_name VARCHAR(21),
    ledger1_account INTEGER,
    ledger1_account_name VARCHAR(47),
    ledger_account INTEGER NOT NULL,
    ledger_account_name VARCHAR(153),
    characteristic CHAR(1),
    is_resident INTEGER,
    is_reserve INTEGER,
    is_reserved INTEGER,
    is_loan INTEGER,
    is_reserved_assets INTEGER,
    is_overdue INTEGER,
    is_interest INTEGER,
    pair_account VARCHAR(5),
    start_date DATE NOT NULL,
    end_date DATE,
    is_rub_only INTEGER,
    min_term VARCHAR(1),
    min_term_measure VARCHAR(1),
    max_term VARCHAR(1),
    max_term_measure VARCHAR(1),
    ledger_acc_full_name_translit VARCHAR(1),
    is_revaluation VARCHAR(1),
    is_correct VARCHAR(1)
);

-- Создание таблицы dm_f101_round_f_v2 для импорта данных
CREATE SCHEMA IF NOT EXISTS dm;

CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f_v2 (
    from_date DATE,
    to_date DATE,
    chapter CHAR(1),
    ledger_account CHAR(5),
    characteristic CHAR(1),
    balance_in_rub NUMERIC(23,8),
    r_balance_in_rub NUMERIC(23,8),
    balance_in_val NUMERIC(23,8),
    r_balance_in_val NUMERIC(23,8),
    balance_in_total NUMERIC(23,8),
    r_balance_in_total NUMERIC(23,8),
    turn_deb_rub NUMERIC(23,8),
    r_turn_deb_rub NUMERIC(23,8),
    turn_deb_val NUMERIC(23,8),
    r_turn_deb_val NUMERIC(23,8),
    turn_deb_total NUMERIC(23,8),
    r_turn_deb_total NUMERIC(23,8),
    turn_cre_rub NUMERIC(23,8),
    r_turn_cre_rub NUMERIC(23,8),
    turn_cre_val NUMERIC(23,8),
    r_turn_cre_val NUMERIC(23,8),
    turn_cre_total NUMERIC(23,8),
    r_turn_cre_total NUMERIC(23,8),
    balance_out_rub NUMERIC(23,8),
    r_balance_out_rub NUMERIC(23,8),
    balance_out_val NUMERIC(23,8),
    r_balance_out_val NUMERIC(23,8),
    balance_out_total NUMERIC(23,8),
    r_balance_out_total NUMERIC(23,8)
);