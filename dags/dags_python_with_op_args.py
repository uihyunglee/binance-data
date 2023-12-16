from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from common.common_func2 import regist

with DAG(
    dag_id="example_python_operator",
    schedule="0 9 * * *",
    start_date=pendulum.datetime(2023, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example"],
) as dag:
    
    regist_t1 = PythonOperator(
        task_id = 'regist_t1',
        python_callable=regist,
        op_args=['hongcana','M','010','KB']
    )

    regist_t1