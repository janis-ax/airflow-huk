import pendulum
from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="p03_xcom_demo_dag",
    start_date=pendulum.now(tz="UTC").subtract(days=7),
    schedule='@hourly',
    catchup=False,
    tags=['p03']

) as dag:
    @task
    def produce_data():
        return {'user': 'ish', 'score': 42}
    @task
    def consume_data(d):
        print(f'User {d["user"]} hat den score {d["score"]}')

    @task
    def log_date():
        return BashOperator(task_id='log_date', bash_command='echo "Run {{logical_date | ds }}"')

    produce_data >> consume_data >> log_date