from airflow import DAG
import pendulum
from airflow.decorators import task

with DAG(
    dag_id="example_python_operator",
    schedule="0 9 * * *",
    start_date=pendulum.datetime(2023, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example"],
) as dag:

    # [START howto_operator_python]
    @task(task_id="python_task_1")
    def print_context(some_input):
        print(some_input)

    python_task_1 = print_context("task decorator 실행")