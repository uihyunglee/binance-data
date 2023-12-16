from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator

import updater_binance


with DAG(
    dag_id="dags_collect_binance_data",
    schedule = "0 9 * * *", # 한국시간 매일 9시 0분마다 실행
    start_date = pendulum.datetime(2023, 11, 1, tz='Asia/Seoul'),
    catchup = False,
    tags=["data-collection"]
) as dag:
    
    collect_binance_data = PythonOperator(
        task_id = 'collect_binance_data',
        python_callable=updater_binance.start_collect,
        op_args=[['5m','15m','30m','1h','4h','1d'],['BTCUSDT'],['BTCUSDT','SOLUSDT','EGLDUSDT','BLURUSDT','AXSUSDT']]
    )

    collect_binance_data