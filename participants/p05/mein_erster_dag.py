from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="p05_hello_world",
    start_date=datetime(2026, 5, 20),
    schedule='@hourly',
    catchup=True,
    tags=["workshop", "p05"],
) as dag:
    BashOperator(
        task_id="hello",
        bash_command="echo 'Hallo Airflow!'",
    )
