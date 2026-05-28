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

    @task(task_id="produce_data")
    def produce_data():
        return {"user": "ish", "score": 42}

    @task(task_id="consume_data")
    def consume_data(**inp):
        print(inp)
        #user = data["user"]
        #score = data["score"]
        #print(f"User {user} has score {score}")

    log_date = BashOperator(
        task_id="log_date",
        bash_command='echo "Processing date {{ logical_date | ds }} ({{ data_interval_start }} - {{ data_interval_end }})"'
    )
    data = produce_data()
    consume_data(data) >> log_date

dag = mein_zweiter_dag()