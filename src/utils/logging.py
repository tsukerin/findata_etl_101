from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

def log_error(table, error_message):
    postgres_hook = PostgresHook('local-postgres')
    engine = postgres_hook.get_sqlalchemy_engine()

    query = f"""
    INSERT INTO "LOGS".LOGGING (log_level, log_date, log_message) 
    VALUES ('ERROR', '{datetime.now()}', 'Произошла ошибка в таблице {table}: {error_message}')
    """
    with engine.connect() as conn:
        conn.execute(query)

def log_notify(type, log_message):
    postgres_hook = PostgresHook('local-postgres')
    engine = postgres_hook.get_sqlalchemy_engine()

    query = f"""
    INSERT INTO "LOGS".LOGGING (log_level, log_date, log_message) 
    VALUES ('{type}', '{datetime.now()}', '{log_message}')
    """
    with engine.connect() as conn:
        conn.execute(query)