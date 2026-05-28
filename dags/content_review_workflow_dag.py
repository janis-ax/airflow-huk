"""
Aufgabe 19: Content Review Workflow (HITL)
Human-in-the-Loop Workflow zur Erfassung, Pruefung und Veroeffentlichung von Content.

Voraussetzung: Airflow 3.1+
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.standard.operators.hitl import (
    ApprovalOperator,
    HITLBranchOperator,
    HITLEntryOperator,
)
from airflow.sdk import Param
from airflow.sdk.execution_time.hitl import HITLUser


@dag(
    dag_id="content_review_workflow",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["workshop", "hitl", "content-review"],
)
def content_review_workflow():
    content_eingabe = HITLEntryOperator(
        task_id="content_eingabe",
        subject="Textentwurf und Zielgruppe eingeben",
        body="Bitte fuellen Sie das Formular aus und senden Sie Ihren Entwurf ab.",
        params={
            "text_entwurf": Param(
                "",
                type="string",
                description="Ihr Textentwurf",
            ),
            "zielgruppe": Param(
                "",
                type="string",
                description="Zielgruppe (z.B. Fachpublikum, Endkunden)",
            ),
        },
    )

    @task
    def zusammenfassung_erstellen(eingabe: dict) -> dict:
        """Verarbeitet die HITL-Eingabe und gibt eine Zusammenfassung per XCom zurueck."""
        text = eingabe["params_input"]["text_entwurf"]
        zielgruppe = eingabe["params_input"]["zielgruppe"]
        woerter = len(text.split())
        kurzfassung = text[:150] + ("..." if len(text) > 150 else "")
        zusammenfassung = (
            f"Zusammenfassung fuer '{zielgruppe}' ({woerter} Woerter): {kurzfassung}"
        )

        ergebnis = {
            "text_entwurf": text,
            "zielgruppe": zielgruppe,
            "zusammenfassung": zusammenfassung,
            "woerter": woerter,
        }
        print(ergebnis)
        return ergebnis

    content_genehmigung = ApprovalOperator(
        task_id="content_genehmigung",
        subject="Content Review – Freigabe erforderlich",
        body="""## Zu pruefender Content

**Zielgruppe:** {{ ti.xcom_pull(task_ids='zusammenfassung_erstellen')['zielgruppe'] }}

**Textentwurf:**
> {{ ti.xcom_pull(task_ids='zusammenfassung_erstellen')['text_entwurf'] }}

**Generierte Zusammenfassung:**
{{ ti.xcom_pull(task_ids='zusammenfassung_erstellen')['zusammenfassung'] }}

Bitte genehmigen oder ablehnen.""",
        execution_timeout=timedelta(minutes=30),
        assigned_users=[
            HITLUser(id="reviewer", name="Content Reviewer"),
        ],
        params={
            "review_kommentar": Param(
                "",
                type="string",
                description="Optionaler Review-Kommentar",
            ),
        },
    )

    verzweigung = HITLBranchOperator(
        task_id="verzweigung",
        subject="Content-Aktion waehlen",
        body="""Der Content wurde genehmigt.

**Zusammenfassung:** {{ ti.xcom_pull(task_ids='zusammenfassung_erstellen')['zusammenfassung'] }}

Waehlen Sie die naechste Aktion.""",
        options=["veroeffentlichen", "ueberarbeiten"],
        defaults=["veroeffentlichen"],
        execution_timeout=timedelta(minutes=30),
    )

    @task(task_id="veroeffentlichen")
    def veroeffentlichen(**context):
        data = context["ti"].xcom_pull(task_ids="zusammenfassung_erstellen")
        print(f"Veroeffentliche Content fuer Zielgruppe '{data['zielgruppe']}':")
        print(data["text_entwurf"])

    @task(task_id="ueberarbeiten")
    def ueberarbeiten(**context):
        data = context["ti"].xcom_pull(task_ids="zusammenfassung_erstellen")
        approval = context["ti"].xcom_pull(task_ids="content_genehmigung")
        kommentar = approval.get("params_input", {}).get("review_kommentar", "")
        print(f"Content zur Ueberarbeitung (Zielgruppe: {data['zielgruppe']})")
        if kommentar:
            print(f"Review-Kommentar: {kommentar}")

    @task(task_id="bei_ablehnung_ueberarbeiten", trigger_rule="all_done")
    def bei_ablehnung_ueberarbeiten(**context):
        """Bei Ablehnung uebernimmt dieser Task die Ueberarbeitung."""
        approval = context["ti"].xcom_pull(task_ids="content_genehmigung")
        if not approval or "Reject" not in approval.get("chosen_options", []):
            return

        data = context["ti"].xcom_pull(task_ids="zusammenfassung_erstellen")
        kommentar = approval.get("params_input", {}).get("review_kommentar", "")
        print(f"Content abgelehnt – Ueberarbeitung fuer '{data['zielgruppe']}' erforderlich")
        if kommentar:
            print(f"Review-Kommentar: {kommentar}")

    verarbeitet = zusammenfassung_erstellen(content_eingabe.output)
    publish = veroeffentlichen()
    revise = ueberarbeiten()
    reject_handler = bei_ablehnung_ueberarbeiten()

    verarbeitet >> content_genehmigung >> verzweigung
    verzweigung >> publish
    verzweigung >> revise
    content_genehmigung >> reject_handler


content_review_workflow()
