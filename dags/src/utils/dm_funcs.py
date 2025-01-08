from airflow.providers.postgres.hooks.postgres import PostgresHook
from src.utils.logging import *
import time
import pandas as pd

def exec_procedure_fill_account_turnover_f(year, month, days):
    log_dm_notify('INFO', 'Выполнение процедуры fill_account_turnover_f')

    try:
        postgres_hook = PostgresHook('local-postgres')
        engine = postgres_hook.get_sqlalchemy_engine()

        for day in range(1, days+1):
            with engine.connect() as conn:
                    conn.execute(f'''
                        CALL dm.fill_account_turnover_f (%s);
                    ''', (f"{year}-{month:02d}-{day}",))
                    time.sleep(1)
                    conn.execute("COMMIT;")
    except Exception as e:
        log_dm_error('fill_account_turnover_f', str(e))


def exec_procedure_fill_account_balance_f(year, month, days):
    log_dm_notify('INFO', 'Выполнение процедуры fill_account_balance_f')

    try:
        postgres_hook = PostgresHook('local-postgres')
        engine = postgres_hook.get_sqlalchemy_engine()

        for day in range(1, days+1):
            with engine.connect() as conn:
                    conn.execute(f'''
                        CALL dm.fill_account_balance_f (%s);
                    ''', (f"{year}-{month:02d}-{day}",))
                    time.sleep(1)
                    conn.execute("COMMIT;")
    except Exception as e:
        log_dm_error('fill_account_balance_f', str(e))

def export_f101_round_f():
    try:
        postgres_hook = PostgresHook('local-postgres')
        engine = postgres_hook.get_sqlalchemy_engine()

        with engine.connect() as conn:
            data = conn.execute('''
                SELECT 
                    from_date,
                    to_date,
                    chapter,
                    ledger_account,
                    characteristic, 
                    balance_in_rub, 
                    r_balance_in_rub, 
                    balance_in_val, 
                    r_balance_in_val, 
                    balance_in_total, 
                    r_balance_in_total, 
                    turn_deb_rub, 
                    r_turn_deb_rub, 
                    turn_deb_val, 
                    r_turn_deb_val, 
                    turn_deb_total, 
                    r_turn_deb_total, 
                    turn_cre_rub, 
                    r_turn_cre_rub, 
                    turn_cre_val, 
                    r_turn_cre_val, 
                    turn_cre_total, 
                    r_turn_cre_total, 
                    balance_out_rub, 
                    r_balance_out_rub, 
                    balance_out_val, 
                    r_balance_out_val, 
                    balance_out_total,
                    r_balance_out_total
                FROM dm.dm_f101_round_f
                ''')
            
            df = pd.read_sql(data, conn)

            df.to_csv('dags/src/files/f101_round_f.csv', index=False)
    except Exception as e:
        log_dm_error('Ошибка при экспорте dm_f101_round_f', str(e))

def insert_into_f101_round_f():
    try:
        df = pd.read_csv(f'/dags/src/files/f101_round_f.csv', sep=';', encoding_errors='replace')
        df.columns = df.columns.str.lower()

        postgres_hook = PostgresHook('local-postgres', options={'autocommit': True})
        engine = postgres_hook.get_sqlalchemy_engine()

        with engine.connect() as conn:
            conn.execute(
                """
                CREATE TEMP TABLE temp_f101_round_f (LIKE dm.f101_round_f);
                """)

            df.to_sql('temp_f101_round_f', conn, if_exists='append', index=False)

            conn.execute(
                """
                MERGE INTO dm.f101_round_f AS target
                USING temp_f101_round_f AS source
                ON target.from_date = source.from_date 
                    AND target.to_date = source.to_date
                    AND target.ledger_account = source.ledger_account
                WHEN MATCHED THEN
                    UPDATE SET 
                        chapter = source.chapter,
                        characteristic = source.characteristic, 
                        balance_in_rub = source.balance_in_rub, 
                        r_balance_in_rub = source.r_balance_in_rub, 
                        balance_in_val = source.balance_in_val, 
                        r_balance_in_val = source.r_balance_in_val, 
                        balance_in_total = source.balance_in_total, 
                        r_balance_in_total = source.r_balance_in_total, 
                        turn_deb_rub = source.turn_deb_rub, 
                        r_turn_deb_rub = source.r_turn_deb_rub, 
                        turn_deb_val = source.turn_deb_val, 
                        r_turn_deb_val = source.r_turn_deb_val, 
                        turn_deb_total = source.turn_deb_total, 
                        r_turn_deb_total = source.r_turn_deb_total, 
                        turn_cre_rub = source.turn_cre_rub, 
                        r_turn_cre_rub = source.r_turn_cre_rub, 
                        turn_cre_val = source.turn_cre_val, 
                        r_turn_cre_val = source.r_turn_cre_val, 
                        turn_cre_total = source.turn_cre_total, 
                        r_turn_cre_total = source.r_turn_cre_total, 
                        balance_out_rub = source.balance_out_rub, 
                        r_balance_out_rub = source.r_balance_out_rub, 
                        balance_out_val = source.balance_out_val, 
                        r_balance_out_val = source.r_balance_out_val, 
                        balance_out_total = source.balance_out_total,
                        r_balance_out_total = source.r_balance_out_total
                WHEN NOT MATCHED THEN
                    INSERT VALUES (
                        source.from_date,
                        source.to_date,
                        source.chapter,
                        source.ledger_account,
                        source.characteristic, 
                        source.balance_in_rub, 
                        source.r_balance_in_rub, 
                        source.balance_in_val, 
                        source.r_balance_in_val, 
                        source.balance_in_total, 
                        source.r_balance_in_total, 
                        source.turn_deb_rub, 
                        source.r_turn_deb_rub, 
                        source.turn_deb_val, 
                        source.r_turn_deb_val, 
                        source.turn_deb_total, 
                        source.r_turn_deb_total, 
                        source.turn_cre_rub, 
                        source.r_turn_cre_rub, 
                        source.turn_cre_val, 
                        source.r_turn_cre_val, 
                        source.turn_cre_total, 
                        source.r_turn_cre_total, 
                        source.balance_out_rub, 
                        source.r_balance_out_rub, 
                        source.balance_out_val, 
                        source.r_balance_out_val, 
                        source.balance_out_total,
                        source.r_balance_out_total
                    );
                """)
            conn.execute(f'DROP TABLE temp_f101_round_f;')
    except Exception as e:
        log_ds_error('f101_round_f', str(e))