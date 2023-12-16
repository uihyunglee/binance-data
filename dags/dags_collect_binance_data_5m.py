from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator

import updater_binance


with DAG(
    dag_id="dags_collect_binance_data_5m",
    schedule = "*/5 * * * *", # 매시간 5분마다 실행
    start_date = pendulum.datetime(2023, 11, 1, tz='Asia/Seoul'),
    catchup = True,
    tags=["data-collection"]
) as dag:
    
    binance_data_5m = PythonOperator(
        task_id = 'binance_data_5m',
        python_callable=updater_binance.start_collect,
        op_args=[[],['BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'EGLDUSDT'], ['BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'EGLDUSDT']]
    )

    binance_data_5m