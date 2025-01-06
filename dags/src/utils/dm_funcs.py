from airflow.providers.postgres.hooks.postgres import PostgresHook
from src.utils.logging import *
import time

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
