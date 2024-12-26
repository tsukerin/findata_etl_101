from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

import pandas as pd
from datetime import datetime
import time

def dummy_load(seconds):
    time.sleep(seconds)

def insert_data(table_name):
    try:
        df = pd.read_csv(f'/data/files/{table_name}.csv', sep=';', encoding_errors='replace')
        if table_name == 'md_currency_d':
            df['CODE_ISO_CHAR'] = df['CODE_ISO_CHAR'].apply(lambda x: 'NaN' if pd.isna(x) or len(x) != 3 else str(x))
            df['CURRENCY_CODE'] = df['CURRENCY_CODE'].fillna(0).astype(int)

        postgres_hook = PostgresHook('local-postgres')
        engine = postgres_hook.get_sqlalchemy_engine()

        df.to_sql(table_name.upper(), engine, schema='DS', if_exists='append', index=False)
    except Exception as e:
        log_error(table_name, str(e))

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

default_args = {
    'owner': 'tsukerin',
    'start_date': datetime.now(),
    'retries': 2
}

with DAG(dag_id='insert_data',
        default_args=default_args,
        description='Создание необходимых таблиц и загрузка в них данных',
        template_searchpath='/data/',
        schedule_interval='0 0 * * *',
        catchup=False
) as dag:

    start_task = PythonOperator(
        task_id='start_task',
        python_callable=log_notify,
        op_args=['INFO', 'Началось импортирование данных. Создание таблиц...']
    )

    create_tables = SQLExecuteQueryOperator(
        task_id='create_tables',
        conn_id='local-postgres',
        sql='sql/create_tables.sql',
    )

    loading = PythonOperator(
        task_id='loading',
        python_callable=dummy_load,
        op_args=[5]
    )

    tables_created = PythonOperator(
        task_id='tables_created',
        python_callable=log_notify,
        op_args=['INFO', 'Таблицы созданы успешно. Импортирование данных...']
    )

    loading2 = PythonOperator(
        task_id='loading2',
        python_callable=dummy_load,
        op_args=[5]
    )

    ft_balance_f = PythonOperator(
        task_id="ft_balance_f",
        python_callable=insert_data,
        op_kwargs={'table_name': 'ft_balance_f'}
    )

    ft_posting_f = PythonOperator(
        task_id="ft_posting_f",
        python_callable=insert_data,
        op_kwargs={'table_name': 'ft_posting_f'}
    )

    md_account_d = PythonOperator(
        task_id="md_account_d",
        python_callable=insert_data,
        op_kwargs={'table_name': 'md_account_d'}
    )

    md_currency_d = PythonOperator(
        task_id="md_currency_d",
        python_callable=insert_data,
        op_kwargs={'table_name': 'md_currency_d'}
    )

    md_exchange_rate_d = PythonOperator(
        task_id="md_exchange_rate_d",
        python_callable=insert_data,
        op_kwargs={'table_name': 'md_exchange_rate_d'}
    )

    md_ledger_account_s = PythonOperator(
        task_id="md_ledger_account_s",
        python_callable=insert_data,
        op_kwargs={'table_name': 'md_ledger_account_s'}
    )

    end_task = PythonOperator(
        task_id='end_task',
        python_callable=log_notify,
        op_args=['SUCCESS', 'Загрузка данных успешно завершена!']
    )

    start_task >> loading >> tables_created >> loading2 >> [ft_balance_f, ft_posting_f, md_account_d, md_currency_d, md_exchange_rate_d, md_ledger_account_s] >> end_task
