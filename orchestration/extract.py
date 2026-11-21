"""Tâche « Extract » du DAG quotidien (AQ-27).

Extrait de PostgreSQL toutes les mesures d'une journée et les enregistre dans
un fichier CSV : data/extracts/measurements_AAAA-MM-JJ.csv

Pourquoi un fichier ? C'est un INSTANTANÉ figé des données de la journée.
L'entraînement (AQ-32) lit ces fichiers : on sait ainsi exactement sur quelles
données chaque modèle a été entraîné (traçabilité, reproductibilité), même si
la base évolue ensuite.
"""
from datetime import date
from pathlib import Path

import pandas as pd

from streaming.storage import MEASUREMENT_COLUMNS

EXTRACT_SQL = f"""
SELECT {", ".join(MEASUREMENT_COLUMNS)}
FROM measurements
WHERE measured_at >= %(day)s::timestamp AT TIME ZONE 'UTC'
  AND measured_at < (%(day)s::date + 1)::timestamp AT TIME ZONE 'UTC'
ORDER BY measured_at, sensor_id
"""


def extract_path(out_dir, day):
    return Path(out_dir) / f"measurements_{day}.csv"


def extract_day(conn, day, out_dir):
    """Exporte les mesures de la journée en CSV. Renvoie un résumé (dict) pour la suite du DAG."""
    if isinstance(day, str):
        day = date.fromisoformat(day)
    with conn.cursor() as cur:
        cur.execute(EXTRACT_SQL, {"day": day})
        rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=MEASUREMENT_COLUMNS)

    path = extract_path(out_dir, day)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)

    summary = {
        "day": day.isoformat(),
        "rows": len(df),
        "valid_rows": int(df["is_valid"].sum()) if len(df) else 0,
        "sensors": int(df["sensor_id"].nunique()),
        "sources": sorted(df["source"].unique().tolist()),
        "path": str(path),
    }
    print(f"Extraction du {day} : {summary['rows']} mesures, {summary['sensors']} capteur(s) "
          f"-> {path}")
    return summary
