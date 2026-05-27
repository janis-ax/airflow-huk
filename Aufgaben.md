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
**Ziel:** Zusammenspiel von `schedule`, `start_date`, `catchup`.

**Anforderungen:**

* `dag_id`: `schedule_demo_dag`
* `start_date`: `2024-01-01` *(timezone-aware)*
* `schedule`: `@daily`
* `catchup`: `True`

**Task:**

* `show_ds` (BashOperator)

  ```bash
  echo "Logical date is {{ logical_date | ds }}"
  ```

**Schritte:**

* Deployen
* Runs prüfen (`catchup=True` erzeugt Backfills)
* `logical_date` vs “run time” vergleichen
* Dann `catchup=False` setzen und Verhalten vergleichen

⚠️ Hinweis fürs Workshop-Cluster: Wenn `start_date` sehr weit zurückliegt, erzeugt `catchup=True` sehr viele Runs.

---

## Aufgabe 4 – Orchestration / Trigger Rules

**Datei:** `dags/orchestration_demo_dag.py`
**Ziel:** Fan-Out, Fan-In, Trigger Rules üben.

**Anforderungen:**

* `dag_id`: `orchestration_demo_dag`
* `schedule`: `@daily`
* `catchup`: `False`

**Tasks:**

* `start` (BashOperator)
* `branch_a` (Python/TaskFlow, immer success)
* `branch_b` (Python/TaskFlow, random Fehler)
* `join` (BashOperator, Trigger Rule: `all_success`)
* `notify_failure` (BashOperator, Trigger Rule: `one_failed`)

**Dependencies:**

```python
start >> [branch_a, branch_b]
[branch_a, branch_b] >> join
[branch_a, branch_b] >> notify_failure
```

---

## Aufgabe 5 – Pools & Concurrency (begrenzen)

**Datei:** `dags/pool_demo_dag.py`
**Ziel:** Parallelität via Pool Slots steuern.

**Anforderungen:**

* `dag_id`: `pool_demo_dag`
* `schedule`: `@daily`
* `catchup`: `False`

**Tasks:**

* `start` (Bash)
* `worker_1`…`worker_5` (Bash, alle `pool="workshop_pool"`)

**Dependencies:**

```python
start >> [worker_1, worker_2, worker_3, worker_4, worker_5]
```

**Schritte:**

* Pool `workshop_pool` in UI anlegen
* Slot-Anzahl variieren und beobachten (Gantt/Grid)

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

