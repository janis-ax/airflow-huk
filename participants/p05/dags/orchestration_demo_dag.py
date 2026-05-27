from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from janis.libs.calculator import add, subtract, multiply, divide

@dag(
    dag_id="p05_dag_04",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),  # timezone-aware
    schedule="@daily",
    catchup=False,
    tags=["workshop", "basics"],
)
def mein_erster_dag():
    @task
    def print_hello():
        print("Hello Airflow")

    show_date = BashOperator(
        task_id="p05_show_date",
        bash_command='echo "Logical execution date is {{ ds }}"',
    )

    @task
    def addiere():
        print(add(1,2))

    @task
    def final_step():
        print("Pipeline complete")

    print_hello() >> show_date >> addiere() >> final_step()


dag = mein_erster_dag()