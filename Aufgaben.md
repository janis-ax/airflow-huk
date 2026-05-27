# Airflow Workshop – Aufgabenübersicht (1–16) – Airflow 3 Clean

## Aufgabe 1 – Mein erster DAG

**Datei:** `dags/mein_erster_dag.py`
**Ziel:** Erstelle deinen ersten Airflow-3 DAG.

**Anforderungen:**

* `dag_id`: `mein_erster_dag`
* `start_date`: `2024-01-01` *(timezone-aware via pendulum)*
* `schedule`: `@daily`
* `catchup`: `False`

**Tasks:**

1. `start` (BashOperator) – `echo "Start"`
2. `process` (Python/TaskFlow) – `print("Processing...")`
3. `end` (BashOperator) – `echo "End"`

**Dependencies:**

```python
start >> process >> end
```

---

## Aufgabe 2 – DAG Graph erkunden (UI)

**Schritte:**

1. UI öffnen: [https://airflow-workshop.dglabor-oci.de](https://airflow-workshop.dglabor-oci.de)
2. DAG suchen: `mein_erster_dag`
3. Views ansehen: Graph / Grid / Gantt / Code
4. DAG manuell triggern
5. Task-Ausführung beobachten (Logs/States)
6. Runs & Status im Grid prüfen

---

## Aufgabe 3 – Scheduling & Catchup verstehen

**Datei:** `dags/schedule_demo_dag.py`

**Teilaufgabe 1: schedule-Presets ausprobieren**

* Setze `schedule='@hourly'`, `start_date` = 7 Tage in der Vergangenheit, `catchup=False`
* Aktiviere den DAG und beobachte: Wie viele Runs werden erzeugt?

**Teilaufgabe 2: catchup=True testen**

* Ändere `catchup` auf `True` und reaktiviere den DAG
* Beobachte im Grid View: Wie viele historische Runs werden nachgeholt?

**Teilaufgabe 3: Cron-Ausdruck einsetzen**

* Setze `schedule='30 9 * * 1-5'` (Werktags um 09:30)
* Tipp: [crontab.guru](https://crontab.guru) zum Testen von Cron-Ausdrücken

---

## Aufgabe 4 – Fan-In mit Trigger Rule

**Datei:** `dags/orchestration_demo_dag.py`

**Struktur:** `A → B`, `A → C`, `[B, C] → D`

**Schritte:**

* Setze für Task D: `trigger_rule='all_done'`
* Simuliere Fehler in Task B (`raise Exception`)
* Beobachte: Läuft Task D trotzdem durch?

---

## Aufgabe 5 – Pool anlegen und nutzen

**Schritte:**

* In der UI: Admin → Pools → Pool `workshop_pool` mit 2 Slots anlegen
* Weise 3 Tasks dem Pool zu: `pool='workshop_pool'`
* Beobachte im Gantt/Grid: Laufen wirklich maximal 2 Tasks gleichzeitig?

---

## Aufgabe 6 – XCom Dataflow

**Datei:** `dags/xcom_demo_dag.py`
**Ziel:** XCom schreiben/lesen (in Airflow 3 am besten über TaskFlow).

**Tasks:**

* `produce_data`: `return {"user": "ish", "score": 42}`
* `consume_data`: bekommt Dict als Input, loggt `"User <user> hat Score <score>"`
* `log_date` (Bash): `echo "Run {{ logical_date | ds }}"`

**Dependencies:**

```python
produce_data >> consume_data >> log_date
```

---

## Aufgabe 7 – Template Demo (Macros / Context)

**Datei:** `dags/template_demo_dag.py`
**Ziel:** Templates & Macros nutzen.

**Task (BashOperator):**

```bash
echo "Processing date {{ logical_date | ds }} ({{ data_interval_start }} - {{ data_interval_end }})"
```

**Schritte:**

* Deploy + Trigger
* Logs prüfen
* Optional: mehrere Runs vergleichen

---

## Aufgabe 8 – Params + dag_run.conf + XCom + Templates

**Datei:** `dags/data_exchange_demo_dag.py`
**Ziel:** Runtime-Params → XCom → Templates kombinieren.

**DAG:**

* `schedule`: `None`
* `catchup`: `False`
* `params`: `{ "country": "DE" }`

**Tasks:**

* `read_country`: liest aus

  1. `(dag_run.conf or {}).get("country")`
  2. `params.country`
     → `return country`
* `build_path` (Bash):

  ```bash
  echo "/data/{{ logical_date | ds }}/{{ ti.xcom_pull(task_ids='read_country') }}/input.csv"
  ```
* `final_log` (Bash):

  ```bash
  echo "Done for {{ (dag_run.conf or {}).get('country', params.country) }}"
  ```

**Dependencies:**

```python
read_country >> build_path >> final_log
```

**Schritte:**

* Run ohne Conf
* Run mit Conf: `{"country": "KR"}`
* Logs vergleichen

---

## Aufgabe 9 – Hello Pod in Kubernetes

**Datei:** `dags/k8s_hello_pod.py`
**Ziel:** Erster KubernetesPodOperator-DAG.

**Task (KubernetesPodOperator):**

* `image`: `alpine:3.18`
* `cmds`: `["/bin/sh", "-c"]`
* `arguments`:

  ```bash
  echo "Hello from Kubernetes Pod – {{ logical_date | ds }}" && sleep 5
  ```

**Schritte:** Triggern → Logs prüfen

---

## Aufgabe 10 – Pod mit Env Vars & Resources

**Datei:** `dags/k8s_configured_pod.py`
**Ziel:** Env Vars + Ressourcenrequests/limits.

**Task:**

* `image`: `python:3.11-slim`
* Env Vars:

  * `ENV=prod`
  * `RUN_DATE={{ logical_date | ds }}`
* Ressourcen (Beispiel):

  * Requests: `250m`, `256Mi`
  * Limits: `500m`, `512Mi`

---

## Aufgabe 11 – Failing Task analysieren

**Datei:** `dags/failing_task_demo_dag.py`
**Ziel:** Logs/Stacktrace lesen, Fehler fixen, erneut testen.

**Schritte:**

* Run starten → Failure ansehen
* Logs & Stacktrace lesen
* Ursache fixen
* Neu deployen → Run erneut testen

---

## Aufgabe 12 – Lint & Tests

**Ziel:** Linting + Tests lokal ausführen.

```bash
ruff check dags/ utils/
black --check dags/ utils/
isort --check-only dags/ utils/
pytest tests/
airflow dags test mein_erster_dag 2024-01-01
```

---

## Aufgabe 13 – Anti-Pattern DAG refactoren

**Datei:** `dags/anti_pattern_demo_dag.py`

**Analysepunkte:**

* Business-Logik im DAG
* Inkonstantes Naming
* Große XComs
* Hardcodierte Pfade/Secrets
* Zu komplexer Single-Task-Monolith

**Ergebnis:** Liste der Optimierungen + Refactor-Vorschlag (z.B. 3 Tasks + utils + params + Connection)

---

## Aufgabe 14 – Konfiguration & Connections

**Datei:** `dags/config_and_connection_demo_dag.py`
**Ziel:** `dag_run.conf` + `params` + `Variable` + `Connection` sauber priorisieren (ohne Secrets zu loggen).

**Voraussetzungen:**

* Variable: `raw_data_path`
* Connection: `my_example_db`

**Task loggt:**

* Pfadquelle (conf/params/Variable)
* Connection Details (safe, ohne Passwort)

---

## Aufgabe 15 – Sensor DAG

**Datei:** `dags/sensor_demo_dag.py`
**Ziel:** FileSensor (ideal: deferrable) + Verarbeitung.

**Tasks:**

* `wait_for_file` (FileSensor)

  * `poke_interval=60`
  * `timeout=1800`
* `process_data` (Bash)

**Dependencies:**

```python
wait_for_file >> process_data
```

---

## Aufgabe 16 – Langläufer-Task optimieren

### Teil 1 – Analyse

* `heavy_task` läuft sehr lange
* Auswirkungen in Gantt/Grid prüfen

### Teil 2 – Optimierte Version (statisch, leicht verständlich)

**Datei:** `dags/long_running_optimized_dag.py`

**Tasks:**

* `start`
* `chunk_1`, `chunk_2`, `chunk_3` (z.B. sleep 120)
* `finish`
* `execution_timeout=300` (pro Chunk)

**Dependencies:**

```python
start >> [chunk_1, chunk_2, chunk_3] >> finish
```

*(Optional Bonus später: Dynamic Task Mapping für beliebig viele Chunks.)*

---