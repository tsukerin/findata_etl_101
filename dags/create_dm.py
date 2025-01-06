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
        dag_id='create_dm',
        default_args=default_args,
        description='Расчет витрин в слое "DM"',
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
        op_args=['INFO', 'Начался расчет витрин. Создание таблиц...']
    )

    loading = PythonOperator(
        task_id='loading',
        python_callable=dummy_load,
        op_args=[5]
    )

    create_tables = SQLExecuteQueryOperator(
        task_id='create_tables',
        conn_id='local-postgres',
        sql='sql/ddl_scripts/create_tables_dm.sql',
    )

    tables_created = PythonOperator(
        task_id='tables_created',
        python_callable=log_dm_notify,
        op_args=['INFO', 'Таблицы созданы успешно. Идет создание процедур...']
    )

    loading2 = PythonOperator(
        task_id='loading2',
        python_callable=dummy_load,
        op_args=[5]
    )

    create_fill_account_turnover_f = SQLExecuteQueryOperator(
        task_id='create_fill_account_turnover_f',
        conn_id='local-postgres',
        sql='sql/stored_procedures/fill_account_turnover_f.sql',
    )

    create_fill_account_balance_f = SQLExecuteQueryOperator(
        task_id='create_fill_account_balance_f',
        conn_id='local-postgres',
        sql='sql/stored_procedures/fill_account_balance_f.sql',
    )

    stored_procedures_created = PythonOperator(
        task_id='stored_procedures_created',
        python_callable=log_dm_notify,
        op_args=['INFO', 'Процедуры созданы. Идет расчет витрин...']
    )

    fill_account_turnover_f = PythonOperator(
        task_id='fill_account_turnover_f',
        python_callable=exec_procedure_fill_account_turnover_f,
        op_args=[2018, 1, 31] 
    )

    fill_account_balance_f = PythonOperator(
        task_id='fill_account_balance_f',
        python_callable=exec_procedure_fill_account_balance_f,
        op_args=[2018, 1, 31] 
    )

    end_task = PythonOperator(
        task_id='end_task',
        python_callable=log_dm_notify,
        op_args=['SUCCESS', 'Расчет витрин данных выполнен успешно!']
    )

    dag_init >> start_task >> loading >> create_tables >> tables_created >> loading2 >> create_fill_account_turnover_f >> create_fill_account_balance_f >> stored_procedures_created >> fill_account_turnover_f >> fill_account_balance_f >> end_task