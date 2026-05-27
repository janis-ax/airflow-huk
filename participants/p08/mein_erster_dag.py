import pendulum
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="p08_hello_world",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["workshop", "p08"],
) as dag:
    BashOperator(
        task_id="hello",
        bash_command="echo 'Hallo Juergen!'",
    )