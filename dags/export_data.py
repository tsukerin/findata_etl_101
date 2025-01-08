from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

import time
from datetime import datetime

from src.utils.logging import *
from src.utils.dm_funcs import *

def dummy_load(seconds):
    time.sleep(seconds)

default_args = {
    'owner': 'tsukerin',
    'start_date': datetime.now()
}

with DAG(
        dag_id='export_data',
        default_args=default_args,
        description='Экспорт витрины в CSV',
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
        python_callable=log_dm_notify,
        op_args=['INFO', 'Начался экспорт витрины в CSV таблицу...']
    )

    loading = PythonOperator(
        task_id="loading",
        python_callable=dummy_load,
        op_args=[5],
    )

    export = PythonOperator(
        task_id='export',
        python_callable=export_f101_round_f,
    )

    end_task = PythonOperator(
        task_id='end_task',
        python_callable=log_dm_notify,
        op_args=['SUCCESS', 'Экспорт в CSV выполнен успешно! Файл находится в директории: /dags/src/files']
    )

    dag_init >> start_task >> loading >> export >> end_task