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
        task_id="A",
        bash_command="echo 'A'"
    )

    @task(task_id="B")
    def B():
        raise Exception

    C = BashOperator(
        task_id="C",
        bash_command="echo 'C'"
    )
    D = BashOperator(
        task_id="D",
        bash_command="echo 'D'",
        trugger_rule='all_done',
    )
    A >> B()
    A >> C
    [B(), C] >> D



dag = mein_erster_dag()


