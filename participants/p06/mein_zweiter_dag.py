from __future__ import annotations

import pendulum
from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


@dag(
    dag_id="p06_k8s_demo",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["workshop", "kubernetes","p06","leif"],
)
def p06_k8s_hello_pod():
    KubernetesPodOperator(
        task_id="hello_pod",
        name="hello-pod",
        image="python:3.11-slim",
        cmds=["python", "-c"],
        arguments=["import os; print(os.environ['ENV'])"],
        # Stackable: Airflow läuft selbst im Cluster -> ServiceAccount-Token reicht,
        # keine eigene kubeconfig nötig.
        in_cluster=True,
        # Pod im selben Namespace wie der Airflow-Worker starten.
        namespace='airflow',
        # Pod nach erfolgreichem Lauf wegräumen, bei Fehler stehen lassen zum Debuggen.
        on_finish_action="delete_succeeded_pod",
        get_logs=True,
        log_events_on_failure=True,
        env_vars={
            "ENV": "hello_prod",
            "RUN_DATE": "{{ logical_date | ds }}"
        },
    )


dag = p06_k8s_hello_pod()