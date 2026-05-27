from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="p08_hello_world",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["workshop", "p08"],
) as dag:
    BashOperator(
        task_id="hello",
        bash_command="echo 'Hallo Juergen!'",
    )