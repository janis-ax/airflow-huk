"""
Aufgabe 18: Parallele Datenverarbeitung
Wiederholung: Orchestrierung, Trigger Rules und Pools.

Voraussetzung: Pool 'verarbeitung_pool' mit 2 Slots anlegen (Admin > Pools).
"""

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


@dag(
    dag_id="p03_parallel_processing",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["workshop", "wiederholung", "pools", "trigger_rules", "p03"],
)
def parallel_processing():

    start = EmptyOperator(task_id="start")

    @task(pool="verarbeitung_pool")
    def laden_csv():
        print("CSV geladen")

    @task(pool="verarbeitung_pool")
    def laden_json():
        print("JSON geladen")

    @task(pool="verarbeitung_pool")
    def laden_xml():
        print("XML geladen")

    @task(trigger_rule="all_done")
    def zusammenfuehren():
        print("Alle Quellen zusammengefuehrt")

    report = BashOperator(
        task_id="report",
        bash_command='echo "Report erstellt am {{ logical_date | ds }}"',
    )

    csv = laden_csv()
    json_data = laden_json()
    xml = laden_xml()

    start >> [csv, json_data, xml] >> zusammenfuehren() >> report


parallel_processing()
