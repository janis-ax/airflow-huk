from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

# Import aus dem gleichen Ordner (janis/ liegt beim DAG-Parsen auf sys.path).
from biweekly_saturday_timetable import OddWeekSaturday9amTimetable


@dag(
    dag_id="janis_biweekly_saturday",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Berlin"),
    schedule=OddWeekSaturday9amTimetable(),
    catchup=False,
    tags=["workshop", "janis", "timetable"],
)
def janis_biweekly_saturday():
    @task
    def log_window(**context) -> None:
        start = context["data_interval_start"]
        end = context["data_interval_end"]
        print(f"Biweekly Saturday run: {start.isoformat()} -> {end.isoformat()}")

    say_hello = BashOperator(
        task_id="say_hello",
        bash_command=(
            'echo "Run at {{ logical_date }} "'
            '"(window {{ data_interval_start }} - {{ data_interval_end }})"'
        ),
    )

    log_window() >> say_hello


dag = janis_biweekly_saturday()
