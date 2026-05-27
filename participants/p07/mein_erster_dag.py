from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
import pendulum




@dag(
    dag_id="p07_mein_erster_dag",
    start_date=pendulum.datetime(2026, 5, 20, tz="UTC"),  # timezone-aware
    schedule="@hourly",
    catchup=False,
    tags=["p07", "workshop", "task1"],
)
def mein_erster_dag():
    A = BashOperator(
        task_id="start_task",
        bash_command="echo 'A'"
    )

    @task(task_id="process_task")
    def B():
        raise Exception

    C = BashOperator(
        task_id="end_task",
        bash_command="echo 'C'"
    )
    D = BashOperator(
        task_id="end_task",
        bash_command="echo 'D'"
    )
    A >> B
    A >> C
    [B, C] >> D

dag = mein_erster_dag()