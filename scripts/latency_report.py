"""Rapport de latence du pipeline streaming (AQ-23).

Mesure, pour les messages écrits en base sur une période récente, le temps
passé dans chaque étape du pipeline :

    kafka_timestamp ──(1)──► processed_at ──(2)──► inserted_at
    (écriture dans           (fin du traitement      (écriture dans
     raw_data)                par le consumer)        PostgreSQL)

    (1) attente dans Kafka + traitement Python
    (2) attente du lot + écriture en base
    (1)+(2) = latence de bout en bout Kafka -> PostgreSQL

Statistiques : moyenne, médiane (p50), p95 (95 % des messages sont plus
rapides que cette valeur), maximum. Le p95 décrit mieux l'expérience réelle
que la moyenne, sensible aux valeurs extrêmes.

Usage (depuis la racine du dépôt) :
    python -m scripts.latency_report                 # 60 dernières minutes
    python -m scripts.latency_report --minutes 15
    python -m scripts.latency_report --csv docs/metrics/latence.csv
"""
import argparse
import csv
from pathlib import Path

from common.db import get_connection

STAGES = {
    "kafka_vers_db": ("kafka_timestamp", "inserted_at"),
    "kafka_vers_traitement": ("kafka_timestamp", "processed_at"),
    "traitement_vers_db": ("processed_at", "inserted_at"),
}

LATENCY_SQL = """
SELECT
    source,
    COUNT(*)                                                                AS messages,
    ROUND(AVG(ms)::numeric, 1)                                              AS moyenne_ms,
    ROUND((PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY ms))::numeric, 1)   AS p50_ms,
    ROUND((PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ms))::numeric, 1)   AS p95_ms,
    ROUND(MAX(ms)::numeric, 1)                                              AS max_ms
FROM (
    SELECT source,
           EXTRACT(EPOCH FROM ({end} - {start})) * 1000 AS ms
    FROM measurements
    WHERE {start} IS NOT NULL
      AND {end} IS NOT NULL
      AND inserted_at >= clock_timestamp() - make_interval(mins => %(minutes)s)
) t
GROUP BY source
ORDER BY source
"""

COLUMNS = ["etape", "source", "messages", "moyenne_ms", "p50_ms", "p95_ms", "max_ms"]


def latency_stats(conn, minutes=60):
    """Renvoie une liste de dicts : une ligne par (étape, source)."""
    rows = []
    with conn.cursor() as cur:
        for stage, (start, end) in STAGES.items():
            cur.execute(LATENCY_SQL.format(start=start, end=end), {"minutes": minutes})
            for record in cur.fetchall():
                values = [float(v) if v is not None and i > 1 else v
                          for i, v in enumerate(record)]
                rows.append(dict(zip(COLUMNS, [stage, *values])))
    return rows


def print_table(rows):
    widths = {c: max(len(c), *(len(str(r[c])) for r in rows)) for c in COLUMNS}
    print("  ".join(c.ljust(widths[c]) for c in COLUMNS))
    print("  ".join("-" * widths[c] for c in COLUMNS))
    for r in rows:
        print("  ".join(str(r[c]).ljust(widths[c]) for c in COLUMNS))


def main():
    parser = argparse.ArgumentParser(description="Latence Kafka -> PostgreSQL")
    parser.add_argument("--minutes", type=int, default=60, help="période analysée")
    parser.add_argument("--csv", type=Path, default=None, help="enregistrer aussi en CSV")
    args = parser.parse_args()

    conn = get_connection()
    try:
        rows = latency_stats(conn, args.minutes)
    finally:
        conn.close()

    if not rows:
        print(f"Aucun message en base sur les {args.minutes} dernières minutes. "
              "Lance le producer et le consumer, puis réessaie.")
        return
    print(f"Latence du pipeline sur les {args.minutes} dernières minutes :\n")
    print_table(rows)
    if args.csv:
        args.csv.parent.mkdir(parents=True, exist_ok=True)
        with args.csv.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=COLUMNS)
            writer.writeheader()
            writer.writerows(rows)
        print(f"\nEnregistré dans {args.csv}")


if __name__ == "__main__":
    main()
