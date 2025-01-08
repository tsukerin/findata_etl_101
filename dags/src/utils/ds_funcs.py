from src.utils.logging import *
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def insert_into_ft_balance_f():
    try:
        df = pd.read_csv(f'/dags/src/files/ft_balance_f.csv', sep=';', encoding_errors='replace')
        df.columns = df.columns.str.lower()

        postgres_hook = PostgresHook('local-postgres', options={'autocommit': True})
        engine = postgres_hook.get_sqlalchemy_engine()

        with engine.connect() as conn:
            conn.execute(
                """
                CREATE TEMP TABLE temp_ft_balance_f (LIKE ds.ft_balance_f);
                """)

            df.to_sql('temp_ft_balance_f', conn, if_exists='append', index=False)

            conn.execute(
                """
                MERGE INTO ds.ft_balance_f AS target
                USING temp_ft_balance_f AS source
                ON target.on_date = source.on_date AND target.account_rk = source.account_rk
                WHEN MATCHED THEN
                    UPDATE SET 
                        currency_rk = source.currency_rk,
                        balance_out = source.balance_out
                WHEN NOT MATCHED THEN
                    INSERT VALUES (
                        source.on_date, 
                        source.account_rk, 
                        source.currency_rk,
                        source.balance_out
                    );
                """)
            
            conn.execute(f'DROP TABLE temp_ft_balance_f;')
    except Exception as e:
        log_ds_error('ft_balance_f', str(e))

def insert_into_ft_posting_f():
    try:
        df = pd.read_csv(f'/dags/src/files/ft_posting_f.csv', sep=';', encoding_errors='replace')
        df.columns = df.columns.str.lower()

        postgres_hook = PostgresHook('local-postgres')
        engine = postgres_hook.get_sqlalchemy_engine()

        with engine.connect() as conn:
        
            conn.execute(
                """
                TRUNCATE ds.ft_posting_f;
                """)
            
            df.to_sql('ft_posting_f', conn, schema='ds', if_exists='append', index=False)
    except Exception as e:
        log_ds_error('ft_posting_f', str(e))

def insert_into_md_account_d():
    try:
        df = pd.read_csv(f'/dags/src/files/md_account_d.csv', sep=';', encoding_errors='replace')
        df.columns = df.columns.str.lower()

        postgres_hook = PostgresHook('local-postgres')
        engine = postgres_hook.get_sqlalchemy_engine()

        with engine.connect() as conn:
            conn.execute(
                """
                CREATE TEMP TABLE temp_md_account_d (LIKE ds.md_account_d);
                """)

            df.to_sql('temp_md_account_d', conn, if_exists='append', index=False)

            conn.execute(
                """
                MERGE INTO ds.md_account_d AS target
                USING temp_md_account_d AS source
                ON target.data_actual_date = source.data_actual_date AND target.account_rk = source.account_rk
                WHEN MATCHED THEN
                    UPDATE SET 
                        data_actual_end_date = source.data_actual_end_date,
                        account_number = source.account_number,
                        char_type = source.char_type,
                        currency_rk = source.currency_rk,
                        currency_code = source.currency_code
                WHEN NOT MATCHED THEN
                    INSERT VALUES (
                        source.data_actual_date, 
                        source.data_actual_end_date, 
                        source.account_rk, 
                        source.account_number,
                        source.char_type,
                        source.currency_rk,
                        source.currency_code
                    );
                """)
            
            conn.execute(f'DROP TABLE temp_md_account_d;')
    except Exception as e:
        log_ds_error('ft_balance_f', str(e))

def insert_into_md_currency_d():
    try:
        df = pd.read_csv(f'/dags/src/files/md_currency_d.csv', sep=';', encoding_errors='replace')
        df.columns = df.columns.str.lower()
        df['code_iso_char'] = df['code_iso_char'].apply(lambda x: 'NaN' if pd.isna(x) or len(x) != 3 else str(x))
        df['currency_code'] = df['currency_code'].fillna(0).astype(int)

        postgres_hook = PostgresHook('local-postgres')
        engine = postgres_hook.get_sqlalchemy_engine()

        with engine.connect() as conn:
            conn.execute(
                """
                CREATE TEMP TABLE temp_md_currency_d (LIKE ds.md_currency_d);
                """)

            df.to_sql('temp_md_currency_d', conn, if_exists='append', index=False)

            conn.execute(
                """
                MERGE INTO ds.md_currency_d AS target
                USING temp_md_currency_d AS source
                ON target.data_actual_date = source.data_actual_date AND target.currency_rk = source.currency_rk
                WHEN MATCHED THEN
                    UPDATE SET 
                        data_actual_end_date = source.data_actual_end_date,
                        currency_code = source.currency_code,
                        code_iso_char = source.code_iso_char
                WHEN NOT MATCHED THEN
                    INSERT VALUES (
                        source.currency_rk, 
                        source.data_actual_date, 
                        source.data_actual_end_date, 
                        source.currency_code,
                        source.code_iso_char
                    );
                """)
            
            conn.execute(f'DROP TABLE temp_md_currency_d;')
    except Exception as e:
        log_ds_error('md_currency_d', str(e))

def insert_into_md_exchange_rate_d():
    try:
        df = pd.read_csv(f'/dags/src/files/md_exchange_rate_d.csv', sep=';', encoding_errors='replace')
        df.columns = df.columns.str.lower()

        postgres_hook = PostgresHook('local-postgres')
        engine = postgres_hook.get_sqlalchemy_engine()

        with engine.connect() as conn:
            conn.execute(
                """
                CREATE TEMP TABLE temp_md_exchange_rate_d (LIKE ds.md_exchange_rate_d);
                """)

            df.to_sql('temp_md_exchange_rate_d', conn, if_exists='append', index=False)
    
            conn.execute(
                """
                MERGE INTO ds.md_exchange_rate_d AS target
                USING (
                SELECT DISTINCT * FROM temp_md_exchange_rate_d
                ) AS source
                ON target.data_actual_date = source.data_actual_date AND target.currency_rk = source.currency_rk
                WHEN MATCHED THEN
                    UPDATE SET 
                        data_actual_end_date = source.data_actual_end_date,
                        reduced_cource = source.reduced_cource,
                        code_iso_num = source.code_iso_num
                WHEN NOT MATCHED THEN
                    INSERT VALUES (
                        source.data_actual_date,
                        source.data_actual_end_date,
                        source.currency_rk, 
                        source.reduced_cource, 
                        source.code_iso_num
                    );
                """)
            
            conn.execute(f'DROP TABLE temp_md_exchange_rate_d;')
    except Exception as e:
        log_ds_error('md_exchange_rate_d', str(e))

def insert_into_md_ledger_account_s():
    try:
        df = pd.read_csv(f'/dags/src/files/md_ledger_account_s.csv', sep=';', encoding_errors='replace')
        df.columns = df.columns.str.lower()

        postgres_hook = PostgresHook('local-postgres')
        engine = postgres_hook.get_sqlalchemy_engine()

        with engine.connect() as conn:
            conn.execute(
                """
                CREATE TEMP TABLE temp_md_ledger_account_s (LIKE ds.md_ledger_account_s);
                """)

            df.to_sql('temp_md_ledger_account_s', conn, if_exists='append', index=False)
            
            conn.execute(
                """
            MERGE INTO ds.md_ledger_account_s AS target
            USING temp_md_ledger_account_s AS source
            ON target.ledger_account = source.ledger_account AND target.start_date = source.start_date
            WHEN MATCHED THEN
                UPDATE SET 
                    chapter = source.chapter,
                    chapter_name = source.chapter_name,
                    section_number = source.section_number,
                    section_name = source.section_name,
                    subsection_name = source.subsection_name,
                    ledger1_account = source.ledger1_account,
                    ledger1_account_name = source.ledger1_account_name,
                    ledger_account_name = source.ledger_account_name,
                    characteristic = source.characteristic,
                    is_resident = source.is_resident,
                    is_reserve = source.is_reserve,
                    is_reserved = source.is_reserved,
                    is_loan = source.is_loan,
                    is_reserved_assets = source.is_reserved_assets,
                    is_overdue = source.is_overdue,
                    is_interest = source.is_interest,
                    pair_account = source.pair_account,
                    end_date = source.end_date,
                    is_rub_only = source.is_rub_only,
                    min_term = source.min_term,
                    min_term_measure = source.min_term_measure,
                    max_term = source.max_term,
                    max_term_measure = source.max_term_measure,
                    ledger_acc_full_name_translit = source.ledger_acc_full_name_translit,
                    is_revaluation = source.is_revaluation,
                    is_correct = source.is_correct
            WHEN NOT MATCHED THEN
                INSERT VALUES (
                    source.chapter, 
                    source.chapter_name, 
                    source.section_number, 
                    source.section_name, 
                    source.subsection_name,
                    source.ledger1_account, 
                    source.ledger1_account_name, 
                    source.ledger_account, 
                    source.ledger_account_name, 
                    source.characteristic, 
                    source.is_resident, 
                    source.is_reserve, 
                    source.is_reserved, 
                    source.is_loan, 
                    source.is_reserved_assets, 
                    source.is_overdue, 
                    source.is_interest, 
                    source.pair_account, 
                    source.start_date, 
                    source.end_date, 
                    source.is_rub_only, 
                    source.min_term, 
                    source.min_term_measure, 
                    source.max_term, 
                    source.max_term_measure, 
                    source.ledger_acc_full_name_translit, 
                    source.is_revaluation, 
                    source.is_correct
                );
                """)
            
            conn.execute(f'DROP TABLE temp_md_ledger_account_s;')
    except Exception as e:
        log_ds_error('md_ledger_account_s', str(e))

