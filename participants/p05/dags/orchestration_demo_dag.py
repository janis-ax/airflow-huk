from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="p05_dag_04_a",
    start_date=datetime(2026, 5, 27),
    schedule='@hourly',
    catchup=False,
    tags=["workshop", "p05"],
) as dag:
    BashOperator(
        task_id="dag_04_a",
        bash_command="echo 'Hallo Airflow aus Aufgabe 4 DAG A!'",
    )
