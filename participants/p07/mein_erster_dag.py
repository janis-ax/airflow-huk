from airflow import DAG
from airflow.operators.bash import BashOperator

@task(task_id="process_task")
def process():
    print("Processing...")

with DAG(
     dag_id="mein_erster_dag",
     start_date=datetime.datetime(2024, 1, 1),
     schedule="@daily",
     catchup=False,
     tags=["workshop", "p07"],
 ) as dag:
    start_task = BashOperator(
        task_id="start_task",
        bash_command='echo "Start"'
    )

    # process_task = process()

    end_task = BashOperator(
        task_id="end_task",
        bash_command='echo "End"'
    )
     