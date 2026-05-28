from airflow import DAG
from airflow.sdk import dag, task, Param, get_current_context
from airflow.providers.standard.operators.bash import BashOperator
import pendulum



@dag(
    dag_id="p07_task_8",
    start_date=pendulum.datetime(2026, 5, 20, tz="UTC"),  # timezone-aware
    schedule=None,
    catchup=False,
    tags=["p07", "workshop", "task2"],
    params={"country", "DE"}
)
def mein_dritter_dag():

    @task(task_id="read_country_p07")
    def read_country(**ctx):
        return ctx["params"]["country"]

    build_path = BashOperator(
        task_id="build_path_p07",
        bash_command="echo {{ ti.xcom_pull(task_ids=\'read_country_p07\') }}"
    )


    read_country() >> build_path

dag = mein_dritter_dag()