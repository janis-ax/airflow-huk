from __future__ import annotations

import pendulum
from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


@dag(
    dag_id="p05_k8s_hello_pod",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["workshop", "kubernetes", p05],
)
def p05_k8s_hello_pod():
    KubernetesPodOperator(
        task_id="p05_hello_pod",
        name="hello-pod",
        image="alpine:3.18",
        cmds=["/bin/sh", "-c"],
        arguments=[
            'echo "Hello from Kubernetes Pod – {{ ds }}" && sleep 5',
        ],
        # Stackable: Airflow läuft selbst im Cluster -> ServiceAccount-Token reicht,
        # keine eigene kubeconfig nötig.
        in_cluster=True,
        # Pod im selben Namespace wie der Airflow-Worker starten.
        namespace='airflow',
        # Pod nach erfolgreichem Lauf wegräumen, bei Fehler stehen lassen zum Debuggen.
        on_finish_action="delete_succeeded_pod",
        get_logs=True,
        log_events_on_failure=True,
    )


dag = p05_k8s_hello_pod()
