import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="p03_hello_world",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=['p03']

) as dag:
    BashOperator(
        task_id="say_hello",
        bash_command='echo "Hello p03!"',
    )