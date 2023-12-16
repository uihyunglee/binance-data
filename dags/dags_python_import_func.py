from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from common.common_func import get_sftp

with DAG(
    dag_id="dags_python_operator",
    schedule = "30 6 * * *",
    start_date = pendulum.datetime(2023, 11, 1, tz='Asia/Seoul'),
    catchup = False
) as dag:
    
    def get_sftp_func():
        get_sftp()
    
    py_t1 = PythonOperator(
        task_id = 'get_sftp_func',
        python_callable=get_sftp_func
    )

    py_t1