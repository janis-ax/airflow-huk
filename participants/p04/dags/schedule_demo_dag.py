from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator


@dag(
    dag_id="p04_schedule_demo_dag",
    start_date=pendulum.now().substract(days=7)
    schedule="@hourly",
    catchup=False,
    tags=["p04"],
)
def schedule_demo_dag():
    show_ds = BashOperator(
        task_id="show_date",
        bash_command='echo "Logical execution date is {{ ds }}"',
    )

    show_ds


dag = schedule_demo_dag()