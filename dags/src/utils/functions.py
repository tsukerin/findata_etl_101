from src.utils.logging import *
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def insert_into_ft_balance_f():
    try:
        df = pd.read_csv(f'/src/files/ft_balance_f.csv', sep=';', encoding_errors='replace')

        postgres_hook = PostgresHook('local-postgres')
        engine = postgres_hook.get_sqlalchemy_engine()

        with engine.connect() as conn:
            conn.execute(
                """
                CREATE TEMP TABLE "TEMP_FT_BALANCE_F" AS
                SELECT * FROM "DS"."FT_BALANCE_F";
                """)

            df.to_sql('TEMP_FT_BALANCE_F', conn, if_exists='append', index=False)

            
            conn.execute(
                """
                MERGE INTO "DS"."FT_BALANCE_F" AS target
                USING "TEMP_FT_BALANCE_F" AS source
                ON target."ON_DATE" = source."ON_DATE" AND target."ACCOUNT_RK" = source."ACCOUNT_RK"
                WHEN MATCHED THEN
                    UPDATE SET 
                        "CURRENCY_RK" = source."CURRENCY_RK",
                        "BALANCE_OUT" = source."BALANCE_OUT"
                WHEN NOT MATCHED THEN
                    INSERT VALUES (
                        source."ON_DATE", 
                        source."ACCOUNT_RK", 
                        source."CURRENCY_RK",
                        source."BALANCE_OUT"
                    );
                """)
            
            conn.execute(f'DROP TABLE "TEMP_FT_BALANCE_F";')
    except Exception as e:
        log_error('ft_balance_f', str(e))

def insert_into_ft_posting_f():
    try:
        df = pd.read_csv(f'/src/files/ft_posting_f.csv', sep=';', encoding_errors='replace')

        postgres_hook = PostgresHook('local-postgres')
        engine = postgres_hook.get_sqlalchemy_engine()

        with engine.connect() as conn:
        
            conn.execute(
                """
                TRUNCATE "DS"."FT_POSTING_F";
                """)
            
            df.to_sql('FT_POSTING_F', conn, schema='DS', if_exists='append', index=False)
    except Exception as e:
        log_error('ft_posting_f', str(e))

def insert_into_md_account_d():
    try:
        df = pd.read_csv(f'/src/files/md_account_d.csv', sep=';', encoding_errors='replace')

        postgres_hook = PostgresHook('local-postgres')
        engine = postgres_hook.get_sqlalchemy_engine()

        with engine.connect() as conn:
            conn.execute(
                """
                CREATE TEMP TABLE "TEMP_MD_ACCOUNT_D" AS
                SELECT * FROM "DS"."MD_ACCOUNT_D";
                """)

            df.to_sql('TEMP_MD_ACCOUNT_D', conn, if_exists='append', index=False)

            
            conn.execute(
                """
                MERGE INTO "DS"."MD_ACCOUNT_D" AS target
                USING "TEMP_MD_ACCOUNT_D" AS source
                ON target."DATA_ACTUAL_DATE" = source."DATA_ACTUAL_DATE" AND target."ACCOUNT_RK" = source."ACCOUNT_RK"
                WHEN MATCHED THEN
                    UPDATE SET 
                        "DATA_ACTUAL_END_DATE" = source."DATA_ACTUAL_END_DATE",
                        "ACCOUNT_NUMBER" = source."ACCOUNT_NUMBER",
                        "CHAR_TYPE" = source."CHAR_TYPE",
                        "CURRENCY_RK" = source."CURRENCY_RK",
                        "CURRENCY_CODE" = source."CURRENCY_CODE"
                WHEN NOT MATCHED THEN
                    INSERT VALUES (
                        source."DATA_ACTUAL_DATE", 
                        source."DATA_ACTUAL_END_DATE", 
                        source."ACCOUNT_RK", 
                        source."ACCOUNT_NUMBER",
                        source."CHAR_TYPE",
                        source."CURRENCY_RK",
                        source."CURRENCY_CODE"
                    );
                """)
            
            conn.execute(f'DROP TABLE "TEMP_MD_ACCOUNT_D";')
    except Exception as e:
        log_error('ft_balance_f', str(e))

def insert_into_md_currency_d():
    try:
        df = pd.read_csv(f'/src/files/md_currency_d.csv', sep=';', encoding_errors='replace')
        df['CODE_ISO_CHAR'] = df['CODE_ISO_CHAR'].apply(lambda x: 'NaN' if pd.isna(x) or len(x) != 3 else str(x))
        df['CURRENCY_CODE'] = df['CURRENCY_CODE'].fillna(0).astype(int)

        postgres_hook = PostgresHook('local-postgres')
        engine = postgres_hook.get_sqlalchemy_engine()

        with engine.connect() as conn:
            conn.execute(
                """
                CREATE TEMP TABLE "TEMP_MD_CURRENCY_D" AS
                SELECT * FROM "DS"."MD_CURRENCY_D";
                """)

            df.to_sql('TEMP_MD_CURRENCY_D', conn, if_exists='append', index=False)

            conn.execute(
                """
                MERGE INTO "DS"."MD_CURRENCY_D" AS target
                USING "TEMP_MD_CURRENCY_D" AS source
                ON target."DATA_ACTUAL_DATE" = source."DATA_ACTUAL_DATE" AND target."CURRENCY_RK" = source."CURRENCY_RK"
                WHEN MATCHED THEN
                    UPDATE SET 
                        "DATA_ACTUAL_END_DATE" = source."DATA_ACTUAL_END_DATE",
                        "CURRENCY_CODE" = source."CURRENCY_CODE",
                        "CODE_ISO_CHAR" = source."CODE_ISO_CHAR"
                WHEN NOT MATCHED THEN
                    INSERT VALUES (
                        source."CURRENCY_RK", 
                        source."DATA_ACTUAL_DATE", 
                        source."DATA_ACTUAL_END_DATE", 
                        source."CURRENCY_CODE",
                        source."CODE_ISO_CHAR"
                    );
                """)
            
            conn.execute(f'DROP TABLE "TEMP_MD_CURRENCY_D";')
    except Exception as e:
        log_error('md_currency_d', str(e))

def insert_into_md_exchange_rate_d():
    try:
        df = pd.read_csv(f'/src/files/md_exchange_rate_d.csv', sep=';', encoding_errors='replace')

        postgres_hook = PostgresHook('local-postgres')
        engine = postgres_hook.get_sqlalchemy_engine()

        with engine.connect() as conn:
            conn.execute(
                """
                CREATE TEMP TABLE "TEMP_MD_EXCHANGE_RATE_D" AS
                SELECT * FROM "DS"."MD_EXCHANGE_RATE_D";
                """)

            df.to_sql('TEMP_MD_EXCHANGE_RATE_D', conn, if_exists='append', index=False)
    
            conn.execute(
                """
                MERGE INTO "DS"."MD_EXCHANGE_RATE_D" AS target
                USING "TEMP_MD_EXCHANGE_RATE_D" AS source
                ON target."DATA_ACTUAL_DATE" = source."DATA_ACTUAL_DATE" AND target."CURRENCY_RK" = source."CURRENCY_RK"
                WHEN MATCHED THEN
                    UPDATE SET 
                        "DATA_ACTUAL_END_DATE" = source."DATA_ACTUAL_END_DATE",
                        "REDUCED_COURCE" = source."REDUCED_COURCE",
                        "CODE_ISO_NUM" = source."CODE_ISO_NUM"
                WHEN NOT MATCHED THEN
                    INSERT VALUES (
                        source."DATA_ACTUAL_DATE",
                        source."DATA_ACTUAL_END_DATE",
                        source."CURRENCY_RK", 
                        source."REDUCED_COURCE", 
                        source."CODE_ISO_NUM"
                    );
                """)
            
            conn.execute(f'DROP TABLE "TEMP_MD_EXCHANGE_RATE_D";')
    except Exception as e:
        log_error('md_exchange_rate_d', str(e))

def insert_into_md_ledger_account_s():
    try:
        df = pd.read_csv(f'/src/files/md_ledger_account_s.csv', sep=';', encoding_errors='replace')

        postgres_hook = PostgresHook('local-postgres')
        engine = postgres_hook.get_sqlalchemy_engine()

        with engine.connect() as conn:
            conn.execute(
                """
                CREATE TEMP TABLE "TEMP_MD_LEDGER_ACCOUNT_S" AS
                SELECT * FROM "DS"."MD_LEDGER_ACCOUNT_S";
                """)

            df.to_sql('TEMP_MD_LEDGER_ACCOUNT_S', conn, if_exists='append', index=False)
            
            conn.execute(
                """
            MERGE INTO "DS"."MD_LEDGER_ACCOUNT_S" AS target
            USING "TEMP_MD_LEDGER_ACCOUNT_S" AS source
            ON target."LEDGER_ACCOUNT" = source."LEDGER_ACCOUNT" AND target."START_DATE" = source."START_DATE"
            WHEN MATCHED THEN
                UPDATE SET 
                    "CHAPTER" = source."CHAPTER",
                    "CHAPTER_NAME" = source."CHAPTER_NAME",
                    "SECTION_NUMBER" = source."SECTION_NUMBER",
                    "SECTION_NAME" = source."SECTION_NAME",
                    "SUBSECTION_NAME" = source."SUBSECTION_NAME",
                    "LEDGER1_ACCOUNT" = source."LEDGER1_ACCOUNT",
                    "LEDGER1_ACCOUNT_NAME" = source."LEDGER1_ACCOUNT_NAME",
                    "LEDGER_ACCOUNT_NAME" = source."LEDGER_ACCOUNT_NAME",
                    "CHARACTERISTIC" = source."CHARACTERISTIC",
                    "IS_RESIDENT" = source."IS_RESIDENT",
                    "IS_RESERVE" = source."IS_RESERVE",
                    "IS_RESERVED" = source."IS_RESERVED",
                    "IS_LOAN" = source."IS_LOAN",
                    "IS_RESERVED_ASSETS" = source."IS_RESERVED_ASSETS",
                    "IS_OVERDUE" = source."IS_OVERDUE",
                    "IS_INTEREST" = source."IS_INTEREST",
                    "PAIR_ACCOUNT" = source."PAIR_ACCOUNT",
                    "END_DATE" = source."END_DATE",
                    "IS_RUB_ONLY" = source."IS_RUB_ONLY",
                    "MIN_TERM" = source."MIN_TERM",
                    "MIN_TERM_MEASURE" = source."MIN_TERM_MEASURE",
                    "MAX_TERM" = source."MAX_TERM",
                    "MAX_TERM_MEASURE" = source."MAX_TERM_MEASURE",
                    "LEDGER_ACC_FULL_NAME_TRANSLIT" = source."LEDGER_ACC_FULL_NAME_TRANSLIT",
                    "IS_REVALUATION" = source."IS_REVALUATION",
                    "IS_CORRECT" = source."IS_CORRECT"
            WHEN NOT MATCHED THEN
                INSERT VALUES (
                    source."CHAPTER", 
                    source."CHAPTER_NAME", 
                    source."SECTION_NUMBER", 
                    source."SECTION_NAME", 
                    source."SUBSECTION_NAME",
                    source."LEDGER1_ACCOUNT", 
                    source."LEDGER1_ACCOUNT_NAME", 
                    source."LEDGER_ACCOUNT", 
                    source."LEDGER_ACCOUNT_NAME", 
                    source."CHARACTERISTIC", 
                    source."IS_RESIDENT", 
                    source."IS_RESERVE", 
                    source."IS_RESERVED", 
                    source."IS_LOAN", 
                    source."IS_RESERVED_ASSETS", 
                    source."IS_OVERDUE", 
                    source."IS_INTEREST", 
                    source."PAIR_ACCOUNT", 
                    source."START_DATE", 
                    source."END_DATE", 
                    source."IS_RUB_ONLY", 
                    source."MIN_TERM", 
                    source."MIN_TERM_MEASURE", 
                    source."MAX_TERM", 
                    source."MAX_TERM_MEASURE", 
                    source."LEDGER_ACC_FULL_NAME_TRANSLIT", 
                    source."IS_REVALUATION", 
                    source."IS_CORRECT"
                );
                """)
            
            conn.execute(f'DROP TABLE "TEMP_MD_LEDGER_ACCOUNT_S";')
    except Exception as e:
        log_error('md_ledger_account_s', str(e))
