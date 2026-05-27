import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="Hello p03",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@hourly",
    catchup=False,
) as dag:
    BashOperator(
        task_id="say_hello",
        bash_command='echo "Hello p03!"',
    )