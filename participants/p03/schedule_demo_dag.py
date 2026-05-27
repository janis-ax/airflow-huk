import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="p03_demo_dag",
    start_date=pendulum.now(tz="UTC").subtract(days=7),
    schedule='@hourly',
    catchup=True,
    tags=['p03']

) as dag:
    BashOperator(
        task_id="say_hello",
        bash_command='echo "Hello p03!"',
    )