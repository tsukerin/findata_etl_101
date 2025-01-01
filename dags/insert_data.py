from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

import pandas as pd
from datetime import datetime, timedelta
import time

from src.utils.logging import *
from src.utils.ds_funcs import *

def dummy_load(seconds):
    time.sleep(seconds)

default_args = {
    'owner': 'tsukerin',
    'start_date': datetime.now(),
    'retries': 2
}

with DAG(dag_id='insert_data',
        default_args=default_args,
        description='Создание необходимых таблиц и загрузка в них данных',
        template_searchpath='/dags/src/',
        schedule_interval='0 0 * * *',
        catchup=False
) as dag:

    dag_init = SQLExecuteQueryOperator(
        task_id='dag_init',
        conn_id='local-postgres',
        sql='sql/ddl_scripts/create_logs_table.sql',
    )

    start_task = PythonOperator(
        task_id='start_task',
        python_callable=log_notify,
        op_args=['INFO', 'Началось импортирование данных. Создание таблиц...']
    )

    create_tables = SQLExecuteQueryOperator(
        task_id='create_tables',
        conn_id='local-postgres',
        sql='sql/ddl_scripts/create_tables_dds.sql',
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
        python_callable=insert_into_ft_balance_f
    )

    ft_posting_f = PythonOperator(
        task_id="ft_posting_f",
        python_callable=insert_into_ft_posting_f
    )

    md_account_d = PythonOperator(
        task_id="md_account_d",
        python_callable=insert_into_md_account_d
    )

    md_currency_d = PythonOperator(
        task_id="md_currency_d",
        python_callable=insert_into_md_currency_d
    )

    md_exchange_rate_d = PythonOperator(
        task_id="md_exchange_rate_d",
        python_callable=insert_into_md_exchange_rate_d
    )

    md_ledger_account_s = PythonOperator(
        task_id="md_ledger_account_s",
        python_callable=insert_into_md_ledger_account_s
    )

    end_task = PythonOperator(
        task_id='end_task',
        python_callable=log_notify,
        op_args=['SUCCESS', 'Загрузка данных успешно завершена!']
    )

    dag_init >> start_task >> create_tables >> loading >> tables_created >> loading2 >> [ft_balance_f, ft_posting_f, md_account_d, md_currency_d, md_exchange_rate_d, md_ledger_account_s] >> end_task
