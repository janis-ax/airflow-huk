from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator


@dag(
    dag_id="p04_orchestration_demo_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),  # timezone-aware
    schedule="@daily",
    catchup=False,
    tags=["p04"],
)
def orchestration_demo_dag():
    start = BashOperator(
        task_id="start",
        bash_command='echo "Start"',
    )

    @task
    def processB():
        print("Processing B...")

     @task
    def processC():
        print("Processing C...")

    end = BashOperator(
        task_id="end",
        bash_command='echo "End"',
    )

    start >> [processB(), processC()] >> end

dag = orchestration_demo_dag()