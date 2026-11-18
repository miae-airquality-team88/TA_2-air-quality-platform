"""Conditions attendues par les sensors du DAG quotidien (AQ-33, AQ-34).

1. new_data_sql() — pour le SqlSensor « wait_new_data » (AQ-33) :
   requête qui renvoie VRAI quand la journée contient assez de mesures.
   Le SqlSensor d'Airflow l'exécute à intervalles réguliers jusqu'à ce que
   la première valeur du résultat soit vraie.

2. is_streaming_done() — pour le PythonSensor « wait_streaming_done » (AQ-34) :
   vrai quand le consumer streaming a traité toutes les données de la journée,
   d'après son signal d'activité (table pipeline_heartbeats).
"""

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
