from airflow import DAG
from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
import pendulum




@dag(
    dag_id="p07_mein_zweiter_dag",
    start_date=pendulum.datetime(2026, 5, 20, tz="UTC"),  # timezone-aware
    schedule="@once",
    catchup=False,
    tags=["p07", "workshop", "task2"],
)
def mein_zweiter_dag():

    @task(task_id="produce_data_p07")
    def produce_data():
        return {"user": "ish", "score": 42}

    @task(task_id="consume_data_p07")
    def consume_data(**context):
        data = context["ti"].xcom_pull(task_ids="produce_data_p07")
        print(f"User {data.user} has score {data.score}")

    log_date = BashOperator(
        task_id="log_date_p07",
        bash_command='echo "Processing date {{ logical_date | ds }} ({{ data_interval_start }} - {{ data_interval_end }})"'
    )

    produce_data() >> consume_data() >> log_date

dag = mein_zweiter_dag()