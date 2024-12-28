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
    
    with engine.connect() as conn:
        if type == 'SUCCESS':
            result = conn.execute('''
            SELECT log_level
            FROM "LOGS".logging
            ORDER BY log_date DESC
            LIMIT 4
            ''')
            has_error = False
            for row in result:
                if row['log_level'] == 'ERROR':
                    has_error = True
                    break
            if has_error:
                type = 'FAILED'
                log_message = 'При выполнении загрузки данных произошла ошибка!'
            else:
                type = 'SUCCESS'

        
        conn.execute(
            f"""
            INSERT INTO "LOGS".LOGGING (log_level, log_date, log_message) 
                VALUES ('{type}', '{datetime.now()}', '{log_message}')
            """)