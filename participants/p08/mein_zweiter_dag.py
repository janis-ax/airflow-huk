from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator


@dag(
    dag_id="p08_02_hello_world",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),  # timezone-aware
    schedule="@daily",
    catchup=True,
    tags=["workshop", "p08"],
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