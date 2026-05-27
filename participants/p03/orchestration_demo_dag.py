import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="p03_orchestrierung_demo_dag",
    start_date=pendulum.now(tz="UTC").subtract(days=7),
    schedule='@hourly',
    catchup=False,
    tags=['p03']

) as dag:
    A=BashOperator(
        task_id="a",
        bash_command='echo "Hello p03!"',
    )
    B=BashOperator(
        task_id="b",
        bash_command='exit 1',
    )
    C=BashOperator(
        task_id="c",
        bash_command='echo "Hello p03!"',
    )
    D=BashOperator(
        task_id="d",
        bash_command='exit 1',
        trigger_rule='all_done'
    )
    A >> B
    A >> C
    [B,C] >> D