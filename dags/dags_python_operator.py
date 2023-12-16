from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
import random

with DAG(
    dag_id="dags_python_operator",
    schedule = "30 6 * * *",
    start_date = pendulum.datetime(2023, 11, 1, tz='Asia/Seoul'),
    catchup = False
) as dag:
    
    def select_coin():
        coin = ['BTC','ETH','SOL','XRP']
        rand_int = random.randint(0,3) # 0에서 3까지 임의 int return
        print(coin[rand_int])

    py_t1 = PythonOperator(
        task_id = 'select_coin',
        python_callable=select_coin
    )

    py_t1
