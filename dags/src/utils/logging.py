from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Логирование для DS слоя

def log_ds_error(table, error_message):
    postgres_hook = PostgresHook('local-postgres')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as conn:
        conn.execute(
            f"""
                INSERT INTO logs.logs_ds (log_level, log_date, log_message) 
                    VALUES ('ERROR', '{datetime.now()}', 'Произошла ошибка в таблице {table}: {error_message}')
            """)

def log_ds_notify(log_type, log_message):
    postgres_hook = PostgresHook('local-postgres')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as conn:
        if log_type == 'SUCCESS':
            result = conn.execute('''
            SELECT log_level
            FROM logs.logs_ds
            ORDER BY log_date DESC
            LIMIT 3
            ''')
            has_error = False
            for row in result:
                if row['log_level'] == 'ERROR':
                    has_error = True
                    break
            if has_error:
                log_type = 'FAILED'
                log_message = 'При выполнении загрузки данных произошла ошибка!'
            else:
                log_type = 'SUCCESS'

        conn.execute(
            f"""
            INSERT INTO logs.logs_ds (log_level, log_date, log_message) 
                VALUES ('{log_type}', '{datetime.now()}', '{log_message}')
            """)

# Логирование для DM слоя

def log_dm_error(procedure, error_message):
    postgres_hook = PostgresHook('local-postgres')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as conn:
        conn.execute(
            f"""
                INSERT INTO logs.logs_dm (log_level, log_date, log_message) 
                    VALUES ('ERROR', '{datetime.now()}', 'Произошла ошибка в процедуре {procedure}: {error_message}')
            """)
        
def log_dm_notify(log_type, log_message):
    postgres_hook = PostgresHook('local-postgres')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as conn:
        if log_type == 'SUCCESS':
            result = conn.execute('''
            SELECT log_level
            FROM logs.logs_dm
            ORDER BY log_date DESC
            LIMIT 3
            ''')
            has_error = False
            for row in result:
                if row['log_level'] == 'ERROR':
                    has_error = True
                    break
            if has_error:
                log_type = 'FAILED'
                log_message = 'При выполнении расчета витрин произошла ошибка!'
            else:
                log_type = 'SUCCESS'

        conn.execute(
            f"""
            INSERT INTO logs.logs_dm (log_level, log_date, log_message) 
                VALUES ('{log_type}', '{datetime.now()}', '{log_message}')
            """)
