from airflow.providers.postgres.hooks.postgres import PostgresHook
from src.utils.logging import *

def exec_procedure_fill_account_turnover_f(year, month, days):
    try:
        postgres_hook = PostgresHook('local-postgres')
        engine = postgres_hook.get_sqlalchemy_engine()

        with engine.connect() as conn:
            for day in range(1, days+1):
                conn.execute(f'''
                    CALL dm.fill_account_turnover_f (%s);
                ''', (f"{year}-{month:02d}-{day:02d}",))

    except Exception as e:
        log_dm_error('fill_account_turnover_f', str(e))
