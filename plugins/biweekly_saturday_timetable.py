"""Plugin-Shim: registriert die Timetable fuer Scheduler/Webserver.

Die Implementierung liegt in janis/biweekly_saturday_timetable.py. Dieser Shim liegt
im konfigurierten plugins_folder und importiert die Klasse von dort.
"""

from __future__ import annotations

import sys
from pathlib import Path

from airflow.plugins_manager import AirflowPlugin

_janis_dir = Path(__file__).resolve().parent.parent / "janis"
if str(_janis_dir) not in sys.path:
    sys.path.insert(0, str(_janis_dir))

from biweekly_saturday_timetable import OddWeekSaturday9amTimetable


class BiweeklySaturdayPlugin(AirflowPlugin):
    name = "biweekly_saturday_plugin"
    timetables = [OddWeekSaturday9amTimetable]
