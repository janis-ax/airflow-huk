from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
import pendulum




@dag(
    dag_id="p07_mein_erster_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),  # timezone-aware
    schedule="@daily",
    catchup=False,
    tags=["p07", "workshop", "task1"],
)
def mein_erster_dag():
    start_task = BashOperator(
        task_id="start_task",
        bash_command="echo 'Start'"
    )

    @task(task_id="process_task")
    def process():
        print("Processing...")

    end_task = BashOperator(
        task_id="end_task",
        bash_command="echo 'End'"
    )
    start >> process() >> end

dag = mein_erster_dag()