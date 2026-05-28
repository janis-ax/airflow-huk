"""
Aufgabe 17: Wetter-Pipeline
Wiederholung: DAG-Grundlagen, Scheduling, XCom, Templates und Params.
"""

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator


@dag(
    dag_id="p04_wetter_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="0 8 * * *",
    catchup=False,
    params={"city": "Berlin"},
    tags=["p04"],
)
def wetter_pipeline():

    @task
    def fetch_temperature(**context):
        """Simuliert das Abrufen einer Temperatur fuer eine Stadt."""
        city = context["params"]["city"]
        data_interval_end = context["data_interval_end"]

        result = {
            "city": city,
            "temp": 22.5,
            "date": str(data_interval_end),
        }
        print(f"Temperatur abgerufen: {result}")
        return result

    @task
    def check_temperature(**context):
        """Prueft ob die Temperatur warm oder kalt ist."""
        weather_data = context["ti"].xcom_pull(task_ids='fetch_temperature')
        temp = weather_data["temp"]
        city = weather_data["city"]

        if temp >= 20:
            bewertung = "warm"
        else:
            bewertung = "kalt"

        print(f"{city}: {temp} Grad = {bewertung}")
        return bewertung

    BashOperator(
        task_id="log_result",
        bash_command=(
            'echo "{{ logical_date | ds }}: '
            '{{ ti.xcom_pull(task_ids=\'check_temperature\') }}"'
        ),
    )

    fetch_temperature >> check_temperature >> log_result


wetter_pipeline()
