"""
Mini-DAG: Hello World mit deferrable Wait via DateTimeTrigger.
Voraussetzung: Triggerer-Prozess muss laufen.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.standard.triggers.temporal import DateTimeTrigger
from airflow.sdk import BaseOperator, Context
from airflow.sdk.timezone import utcnow


class WaitDateTimeTriggerOperator(BaseOperator):
    """Deferrable Operator, der auf einen DateTimeTrigger wartet."""

    def __init__(self, wait_minutes: int = 5, **kwargs) -> None:
        super().__init__(**kwargs)
        self.wait_minutes = wait_minutes

    def execute(self, context: Context) -> None:
        fire_at = utcnow() + timedelta(minutes=self.wait_minutes)
        self.log.info("Deferring until %s", fire_at)
        self.defer(
            trigger=DateTimeTrigger(moment=fire_at),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event=None) -> None:
        self.log.info("DateTimeTrigger fired: %s", event)


@dag(
    dag_id="hello_world_defer",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["workshop", "deferrable", "trigger"],
)
def hello_world_defer():
    wait_5_min = WaitDateTimeTriggerOperator(
        task_id="wait_5_min",
        wait_minutes=5,
    )

    @task
    def hello_world():
        print("Hello World")

    wait_5_min >> hello_world()


hello_world_defer()
