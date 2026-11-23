"""Chargement en base de l'historique réel OpenAQ (AQ-37).

Enchaîne trois briques déjà écrites par l'équipe :
  1. téléchargement des mesures de la station          (AQ-14, ingestion/openaq_fetch.py)
  2. transformation au format du contrat de données     (AQ-16, ingestion/openaq_replay.py)
  3. traitement par le pipeline streaming + écriture    (Sprint 2 : validation, fenêtres,
     en base                                             règles, stockage)

Différence avec le rejeu AQ-16 : on n'envoie pas les messages dans Kafka, on les
passe directement au pipeline et en base. C'est plus rapide pour 30 jours
d'historique, et le résultat en base est identique.

Idempotence : kafka_partition = -2 (marque « historique OpenAQ ») et kafka_offset
calculé à partir de l'heure de mesure : relancer le script ne crée aucun doublon.

Usage (depuis la racine du dépôt, clé OPENAQ_API_KEY dans le .env) :
    python -m ingestion.openaq_history                      # période du backfill
    python -m ingestion.openaq_history --from 2026-11-01 --to 2026-11-15
    python -m ingestion.openaq_history --csv data/openaq/location_3847229.csv   # sans Internet
"""
import argparse
import json
import os
from datetime import date, timedelta

import pandas as pd

from common.config import DATA_DIR, OPENAQ_LOCATION_ID

OPENAQ_PARTITION = -2


def default_period():
    """Même période que le backfill (variables BACKFILL_* du .env)."""
    return (os.getenv("BACKFILL_START_DATE", "2026-10-27"),
            os.getenv("BACKFILL_END_DATE", "2026-11-25"))


def openaq_offset(timestamp_iso):
    """Identifiant fixe d'une mesure OpenAQ : son heure en secondes depuis 1970."""
    return int(pd.Timestamp(timestamp_iso).timestamp())


def load_messages(conn, messages, batch_size=1000):
    """Passe les messages (format du contrat) dans le pipeline et les écrit en base."""
    from streaming.consumer import StreamPipeline
    from streaming.storage import Storage

    pipeline = StreamPipeline()
    storage = Storage(conn, batch_size=batch_size, max_wait_s=3600)
    for message in messages:
        processed, anomalies = pipeline.process(json.dumps(message).encode("utf-8"))
        storage.add(processed, anomalies, OPENAQ_PARTITION, openaq_offset(message["timestamp"]))
        storage.flush_if_due()
    storage.flush()
    return pipeline


def per_day(messages):
    """Nombre de messages par journée (pour le bilan)."""
    counts = {}
    for m in messages:
        counts[m["timestamp"][:10]] = counts.get(m["timestamp"][:10], 0) + 1
    return dict(sorted(counts.items()))


def fetch_csv(date_from, date_to):
    """Télécharge l'historique avec les fonctions d'AQ-14 et renvoie le chemin du CSV."""
    from ingestion.openaq_fetch import OUT_DIR, fetch_location, fetch_sensor

    end = (date.fromisoformat(date_to) + timedelta(days=1)).isoformat()   # date_to incluse
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    location = fetch_location(OPENAQ_LOCATION_ID)
    (OUT_DIR / f"location_{OPENAQ_LOCATION_ID}.json").write_text(
        json.dumps(location, indent=2, ensure_ascii=False), encoding="utf-8")
    rows = []
    for sensor in location.get("sensors", []):
        print(f"Téléchargement du capteur {sensor['id']} ({sensor['parameter']['name']})…")
        rows.extend(fetch_sensor(sensor["id"], date_from, end, endpoint="hours"))
    path = DATA_DIR / "openaq" / f"location_{OPENAQ_LOCATION_ID}.csv"
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def main():
    start, end = default_period()
    parser = argparse.ArgumentParser(description="Historique réel OpenAQ -> PostgreSQL")
    parser.add_argument("--from", dest="date_from", default=start)
    parser.add_argument("--to", dest="date_to", default=end)
    parser.add_argument("--csv", default=None,
                        help="utiliser un CSV déjà téléchargé au lieu d'appeler l'API")
    args = parser.parse_args()

    from common.db import get_connection
    from ingestion.openaq_replay import load_coordinates, rows_to_messages

    csv_path = args.csv or fetch_csv(args.date_from, args.date_to)
    df = pd.read_csv(csv_path)
    messages = [m for m in rows_to_messages(df, load_coordinates())
                if args.date_from <= m["timestamp"][:10] <= args.date_to]
    if not messages:
        raise SystemExit("Aucune mesure OpenAQ sur cette période.")

    conn = get_connection()
    try:
        pipeline = load_messages(conn, messages)
    finally:
        conn.close()

    counts = per_day(messages)
    print(f"\n{len(messages)} mesures OpenAQ chargées sur {len(counts)} journée(s) :")
    for day, n in counts.items():
        print(f"  {day} : {n} mesures")
    print("Bilan du pipeline :", pipeline.summary())


if __name__ == "__main__":
    main()
