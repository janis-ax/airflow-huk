from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
import pendulum




@dag(
    dag_id="p07_mein_zweiter_dag",
    start_date=pendulum.datetime(2026, 5, 20, tz="UTC"),  # timezone-aware
    schedule="@hourly",
    catchup=True,
    tags=["p07", "workshop", "task2"],
)
def mein_zweiter_dag():

    @task(task_id="produce_data", do_xcom_push=True)
    def produce_data():
        return {"user": "ish", "score": 42}

    @task(task_id="consume_data", do_xcom_push=True)
    def consume_data(**context):
        user = context["ti"].xcom_pull(task_ids="produce_data", key="user")
        score = context["ti"].xcom_pull(task_ids="produce_data", key="score")
        print(f"User {user} has score {score}")

    log_date = BashOperator(
        task_id="log_date",
        bash_command='echo "Run {{ logical_date | ds }}"',
    )
    produce_data() >> consume_data() >> log_date()

dag = mein_zweiter_dag()