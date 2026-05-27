from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator



@dag(
    dag_id="p06_hello_world",
    start_date=pendulum.datetime(2026, 5, 20, tz="UTC"),  # timezone-aware
    schedule="@daily",
    catchup=True,
    tags=["workshop", "p06","leif"],
)
def mein_erster_dag():
    @task
    def print_hello():
        print("Hello Airflow")

    show_date = BashOperator(
        task_id="show_date",
        bash_command='echo "Logical execution date is {{ ds }}"',
    )

    @task
    def final_step():
        print("Pipeline complete")

    print_hello() >> show_date >> final_step()

dag = mein_erster_dag()
    