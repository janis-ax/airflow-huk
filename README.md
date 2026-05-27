# Airflow Workshop – Teilnehmer-Guide

Diese Anleitung erklärt, wie du in deiner VSCode-Web-Session (`code-server`)
DAGs schreibst und sie in das Workshop-Repository pushst, damit Airflow sie
automatisch (über `git-sync`) lädt.

## 1. VSCode öffnen und einloggen

1. Öffne deine persönliche URL (vom Trainer erhalten), z. B.
   `https://p01-airflow.cluster.nifi.dev`
2. Logge dich mit dem dir mitgeteilten Passwort ein.

## 2. Terminal öffnen

In VSCode:

- Menü: **Terminal → New Terminal**
- oder Shortcut: **Strg + `** (Backtick)

Das Terminal öffnet sich in deinem Workspace `~/workspace`.

## 3. Git einmalig einrichten

```bash
git config --global user.name  "Dein Name"
git config --global user.email "dein.name@example.com"
```

## 4. Workshop-Repository klonen

Du hast vom Trainer einen **GitHub Personal Access Token (PAT)** erhalten.
Diesen verwendest du einmalig zum Klonen:

```bash
cd ~/workspace
git clone https://x-access-token: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@github.com/janis-ax/airflow-huk.git

```


> **Hinweis:** Der Token bleibt nur in deinem persönlichen Workspace
> gespeichert. Andere Teilnehmer sehen ihn nicht.

## 5. Eigenen DAG anlegen

Lege deine DAG-Dateien in deinem eigenen Unterordner an, damit sich nichts
mit anderen Teilnehmern überschneidet:

```bash
mkdir -p participants/p01
nano participants/p01/mein_erster_dag.py
```

Beispiel-DAG:

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="p01_hello_world",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["workshop", "p01"],
) as dag:
    BashOperator(
        task_id="hello",
        bash_command="echo 'Hallo Airflow!'",
    )
```

## 6. Änderungen committen und pushen

```bash
git add .
git commit -m "p01: Mein erster DAG"
git push
```

Airflow zieht das Repository alle **20 Sekunden** automatisch.
Nach kurzer Zeit erscheint dein DAG in der Airflow-UI unter
`https://airflow.cluster.nifi.dev`.

## 7. Nützliche Tipps

| Aktion | Befehl |
|--------|--------|
| Aktuellen Status anzeigen | `git status` |
| Letzte Commits anzeigen | `git log --oneline -10` |
| Änderungen anderer Teilnehmer holen | `git pull` |
| Datei aus dem letzten Commit wiederherstellen | `git checkout -- <datei>` |

## 8. Hilfe?

Frag direkt im Workshop-Chat oder den Trainer. Viel Spaß beim Bauen!