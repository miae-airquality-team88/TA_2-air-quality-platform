"""Fonctions appelées par les tâches des DAGs (AQ-41).

Les deux DAGs (quotidien et backfill) exécutent les mêmes étapes. Ce module les
regroupe pour qu'elles soient écrites UNE seule fois : chaque fonction ouvre la
connexion à PostgreSQL, appelle le code métier du package orchestration/, puis
ferme la connexion.

Les imports sont faits à l'intérieur des fonctions : Airflow relit les fichiers
des DAGs toutes les 30 s, ils doivent rester légers.
"""


def _with_connection(func, *args, **kwargs):
    from common.db import get_connection

    conn = get_connection()
    try:
        return func(conn, *args, **kwargs)
    finally:
        conn.close()


def run_extract(day):
    """Extraction des mesures de la journée vers data/extracts/ (AQ-27)."""
    from common.config import EXTRACTS_DIR
    from orchestration.extract import extract_day

    return _with_connection(extract_day, day, EXTRACTS_DIR)


def run_validate_quality(day, run_id=None):
    """Contrôles qualité de la journée (AQ-29). Lève une erreur si un contrôle échoue."""
    from orchestration.quality import validate_day

    return _with_connection(validate_day, day, run_id=run_id)


def run_aggregate(day):
    """Agrégats quotidiens + réconciliation (AQ-31)."""
    from orchestration.aggregate import run_daily_aggregate

    return _with_connection(run_daily_aggregate, day)
