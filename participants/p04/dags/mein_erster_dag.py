from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator


@dag(
    dag_id="p04_mein_erster_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),  # timezone-aware
    schedule="@daily",
    catchup=False,
    tags=["p04", "workshop", "basics"],
)
def mein_erster_dag():
    start = BashOperator(
        task_id="start",
        bash_command='echo "Start"',
    )

    @task
    def process():
        print("Processing...")

    end = BashOperator(
        task_id="end",
        bash_command='echo "End"',
    )

    start >> process() >> end

dag = mein_erster_dag()