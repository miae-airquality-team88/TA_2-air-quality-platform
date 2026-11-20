"""Conditions attendues par les sensors du DAG quotidien (AQ-33, AQ-34).

1. new_data_sql() — pour le SqlSensor « wait_new_data » (AQ-33) :
   requête qui renvoie VRAI quand la journée contient assez de mesures.
   Le SqlSensor d'Airflow l'exécute à intervalles réguliers jusqu'à ce que
   la première valeur du résultat soit vraie.

2. is_streaming_done() — pour le PythonSensor « wait_streaming_done » (AQ-34) :
   vrai quand le consumer streaming a traité toutes les données de la journée,
   d'après son signal d'activité (table pipeline_heartbeats).
"""
from datetime import date, datetime, time, timedelta, timezone

MIN_NEW_ROWS = 100


def new_data_sql(day_expr, min_rows=MIN_NEW_ROWS):
    """Requête du SqlSensor : vrai si la journée `day_expr` contient au moins `min_rows` mesures.

    `day_expr` est une date 'AAAA-MM-JJ' ou un modèle Jinja d'Airflow qui produit
    cette date au moment de l'exécution (ex. "{{ data_interval_start | ds }}").
    """
    return (
        f"SELECT COUNT(*) >= {int(min_rows)} FROM measurements "
        f"WHERE measured_at >= '{day_expr}'::timestamp AT TIME ZONE 'UTC' "
        f"AND measured_at < ('{day_expr}'::date + 1)::timestamp AT TIME ZONE 'UTC'"
    )


HEARTBEAT_SQL = """
SELECT status, last_seen, consumer_lag, pending_rows
FROM pipeline_heartbeats WHERE component = %s
"""


def is_streaming_done(conn, day, component="aq-streaming"):
    """Vrai si le consumer a traité toutes les données de la journée `day`.

    Conditions, d'après le dernier signal d'activité du consumer :
      1. le signal a été envoyé APRÈS la fin de la journée (minuit UTC) :
         le consumer tournait encore quand la journée s'est terminée ;
      2. son retard Kafka est nul (consumer_lag = 0) : tout a été lu ;
      3. aucune ligne n'attend d'être écrite en base (pending_rows = 0).
    Renvoie (vrai/faux, explication).
    """
    if isinstance(day, str):
        day = date.fromisoformat(day)
    day_end = datetime.combine(day + timedelta(days=1), time(0), tzinfo=timezone.utc)
    with conn.cursor() as cur:
        cur.execute(HEARTBEAT_SQL, (component,))
        row = cur.fetchone()
    if row is None:
        return False, "aucun signal du consumer (n'a jamais tourné ?)"
    status, last_seen, lag, pending = row
    if last_seen < day_end:
        return False, f"dernier signal à {last_seen:%Y-%m-%d %H:%M} UTC, avant la fin de la journée"
    if lag is None or lag > 0:
        return False, f"retard Kafka = {lag} message(s) restant à lire"
    if pending:
        return False, f"{pending} ligne(s) en attente d'écriture en base"
    return True, f"streaming à jour (consumer {status}, dernier signal {last_seen:%H:%M:%S} UTC)"


def streaming_done_for_sensor(day):
    """Version pour le PythonSensor d'Airflow : ouvre la connexion, affiche la raison."""
    from common.db import get_connection

    conn = get_connection()
    try:
        done, reason = is_streaming_done(conn, day)
    finally:
        conn.close()
    print(reason)
    return done
