"""Custom Timetable: Samstags um 09:00 (Europe/Berlin), aber nur in ungeraden ISO-Wochen.
"""

from __future__ import annotations

import pendulum
from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import (
    DagRunInfo,
    DataInterval,
    TimeRestriction,
    Timetable,
)

LOCAL_TZ = pendulum.timezone("Europe/Berlin")
# pendulum/Python: Montag=0, Dienstag=1, ... Samstag=5, Sonntag=6
SATURDAY = 5
INTERVAL_DAYS = 14


class OddWeekSaturday9amTimetable(Timetable):
    """Triggert einen DAG-Run samstags um 09:00 (Europe/Berlin) in ungeraden ISO-Wochen."""

    description = "Samstags 09:00 (Europe/Berlin) in ungeraden ISO-Wochen"

    def _next_odd_saturday_9am(
        self, after: pendulum.DateTime
    ) -> pendulum.DateTime:
        """Erster Samstag 09:00 in einer ungeraden ISO-Woche, der echt nach `after` liegt."""
        # In Lokalzeit rechnen, damit 09:00 auch ueber Sommer-/Winterzeit hinweg
        # tatsaechlich 09:00 Ortszeit bleibt (DST-sicher).
        local = after.in_timezone(LOCAL_TZ)

        # 1) Uhrzeit auf 09:00 desselben Tages setzen.
        candidate = local.replace(hour=9, minute=0, second=0, microsecond=0)

        # 2) Auf den naechsten Samstag schieben.
        #    (SATURDAY - weekday()) % 7 ergibt die Anzahl Tage bis zum kommenden
        #    Samstag: 0 wenn heute schon Samstag ist, sonst 1..6.
        days_ahead = (SATURDAY - candidate.weekday()) % 7
        candidate = candidate.add(days=days_ahead)

        # 3) Falls wir dadurch auf `after` selbst oder davor gelandet sind
        #    (z.B. heute ist Samstag, aber schon nach 09:00), eine Woche weiter.
        if candidate <= local:
            candidate = candidate.add(days=7)

        # 4) Solange die ISO-Wochennummer gerade ist, jeweils eine Woche weiter.
        #    isocalendar() liefert (ISO-Jahr, ISO-Woche, ISO-Wochentag);
        #    Index [1] ist die Wochennummer. Ungerade Woche => % 2 == 1.
        #    Diese Schleife laeuft hoechstens einmal, ist aber an Jahreswechseln
        #    der entscheidende Unterschied zur reinen 14-Tage-Logik.
        while candidate.isocalendar()[1] % 2 == 0:
            candidate = candidate.add(days=7)

        return candidate

    def infer_manual_data_interval(
        self, *, run_after: pendulum.DateTime
    ) -> DataInterval:
        # Fuer manuelle Runs: Fenster der 14 Tage vor dem Trigger-Zeitpunkt.
        return DataInterval(
            start=run_after.subtract(days=INTERVAL_DAYS),
            end=run_after,
        )

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        if last_automated_data_interval is not None:
            # Es gab schon einen Run: ab dessen Ende den naechsten ungeraden
            # Samstag suchen. Start = altes Ende => luecklenlose, zusammenhaengende
            # Fenster (i.d.R. 14 Tage, am Jahreswechsel ggf. nur 7).
            last_end = last_automated_data_interval.end
            next_run = self._next_odd_saturday_9am(last_end)
            start = last_end
        else:
            # Allererster Run: nicht vor dem erlaubten Startdatum.
            if restriction.earliest is None:
                return None
            # subtract(microseconds=1), damit ein `earliest`, das exakt auf einem
            # passenden Samstag 09:00 liegt, noch eingeschlossen wird (>=-Semantik).
            next_run = self._next_odd_saturday_9am(
                restriction.earliest.subtract(microseconds=1)
            )
            # Ohne catchup keine Vergangenheit nachholen: bis in die Gegenwart
            # springen und dabei die Samstags-/Ungerade-Woche-Kadenz beibehalten.
            if not restriction.catchup:
                now = pendulum.now("UTC")
                while next_run < now:
                    next_run = self._next_odd_saturday_9am(next_run)
            # Erstes Fenster nominal 14 Tage zurueck (es gibt noch kein Vor-Run-Ende).
            start = next_run.subtract(days=INTERVAL_DAYS)

        # Enddatum-Restriktion respektieren.
        if restriction.latest is not None and next_run > restriction.latest:
            return None

        return DagRunInfo.interval(start=start, end=next_run)


class BiweeklySaturdayPlugin(AirflowPlugin):
    name = "biweekly_saturday_plugin"
    timetables = [OddWeekSaturday9amTimetable]
