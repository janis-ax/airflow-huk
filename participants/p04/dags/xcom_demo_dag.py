from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator


@dag(
    dag_id="p04_xcom_demo_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),  # timezone-aware
    schedule="@daily",
    catchup=False,
    tags=["p04"],
)
def xcom_demo_dag():
    @task
    def produce_data():
        return {"user": "ish", "score": 42}

    @task
    def consume_data(**context):
        data = context["ti"].xcom_pull(task_ids='produce_data')
        print(data)
        print(f"User {data['user']} hat Score {data['score']}")

    @task
    def log_date():
        print("Run {{ logical_date | ds }}")

    produce_data() >> consume_data() >> log_date()

dag = xcom_demo_dag()