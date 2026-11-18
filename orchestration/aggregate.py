"""Tâche « Aggregate » du DAG quotidien (AQ-31).

Appelle le batch quotidien écrit au Sprint 2 (streaming.storage.aggregate_day)
puis VÉRIFIE le résultat : la somme des mesures comptées dans daily_aggregates
doit être égale au nombre de mesures de la journée dans measurements.
C'est un contrôle de réconciliation : aucune mesure perdue ni comptée deux fois
pendant l'agrégation.
"""
from datetime import date

from streaming.storage import aggregate_day


class AggregationError(Exception):
    """Levée si les agrégats ne correspondent pas aux mesures."""


RECONCILE_SQL = """
SELECT
    (SELECT COUNT(*) FROM measurements
      WHERE measured_at >= %(day)s::timestamp AT TIME ZONE 'UTC'
        AND measured_at < (%(day)s::date + 1)::timestamp AT TIME ZONE 'UTC'),
    (SELECT COALESCE(SUM(measurement_count), 0) FROM daily_aggregates WHERE day = %(day)s)
"""


def run_daily_aggregate(conn, day):
    """Calcule les agrégats de la journée et vérifie la réconciliation.

    Renvoie {"day": ..., "sensors": nb de capteurs, "measurements": nb de mesures}.
    """
    if isinstance(day, str):
        day = date.fromisoformat(day)
    sensors = aggregate_day(conn, day)
    with conn.cursor() as cur:
        cur.execute(RECONCILE_SQL, {"day": day})
        in_measurements, in_aggregates = cur.fetchone()
    print(f"Agrégats du {day} : {sensors} capteur(s), {in_aggregates} mesures agrégées "
          f"pour {in_measurements} mesures en base.")
    if in_measurements != in_aggregates:
        raise AggregationError(
            f"Réconciliation impossible le {day} : {in_measurements} mesures en base, "
            f"{in_aggregates} dans daily_aggregates."
        )
    return {"day": day.isoformat(), "sensors": sensors, "measurements": int(in_measurements)}
