"""Rejeu de l'historique OpenAQ dans le topic Kafka raw_data (AQ-16).

Lit le CSV produit par AQ-14 (une ligne par mesure), le transforme en messages
AU MÊME FORMAT que le simulateur (contrat schemas/sensor_message.json), puis les
envoie avec send_message() du producer (AQ-11), en accéléré.

La suite du pipeline traite ainsi des données réelles sans savoir d'où elles
viennent : seul le champ source = "openaq" les distingue (lignée des données).

Choix de transformation :
- seuls les 6 paramètres du contrat sont gardés (voir PARAM_MAP) ;
- no2 et o3 sont convertis en µg/m³ s'ils sont fournis en ppm ou ppb (à 25 °C) ;
- une ligne par horodatage ; une mesure absente vaut None ;
- si plusieurs valeurs existent pour la même heure, on garde leur moyenne.

Usage (depuis la racine du dépôt, Kafka démarré, CSV présent) :
    python -m ingestion.openaq_replay --limit 50
    python -m ingestion.openaq_replay --interval 0.2
"""
import argparse
import json
import time

import pandas as pd

from common.config import DATA_DIR, OPENAQ_LOCATION_ID

CSV_PATH = DATA_DIR / "openaq" / f"location_{OPENAQ_LOCATION_ID}.csv"
META_PATH = DATA_DIR / "openaq" / f"location_{OPENAQ_LOCATION_ID}.json"

# Nom du paramètre chez OpenAQ -> nom dans notre contrat
PARAM_MAP = {
    "pm25": "pm2_5",
    "pm10": "pm10",
    "no2": "no2",
    "o3": "o3",
    "temperature": "temperature",
    "relativehumidity": "humidity",
}
CONTRACT_FIELDS = ["pm2_5", "pm10", "no2", "o3", "temperature", "humidity"]

# Facteurs de conversion vers µg/m³ (à 25 °C)
PPM_TO_UGM3 = {"no2": 1880.0, "o3": 1960.0}
PPB_TO_UGM3 = {"no2": 1.88, "o3": 1.96}


def load_coordinates():
    """Lit les coordonnées de la station dans le JSON produit par AQ-14."""
    meta = json.loads(META_PATH.read_text(encoding="utf-8"))
    coords = meta["coordinates"]
    return {"lat": round(float(coords["latitude"]), 5),
            "lon": round(float(coords["longitude"]), 5)}


def to_iso_utc(value):
    """Convertit une date (texte) en 'AAAA-MM-JJTHH:MM:SSZ' en UTC."""
    ts = pd.Timestamp(value)
    ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def convert_units(df):
    """Convertit no2 et o3 en µg/m³ lorsqu'ils sont en ppm ou ppb."""
    unit = df["unit"].astype(str).str.lower()
    for factors, unit_name in [(PPM_TO_UGM3, "ppm"), (PPB_TO_UGM3, "ppb")]:
        mask = unit.eq(unit_name) & df["field"].isin(factors)
        df.loc[mask, "value"] = df.loc[mask, "value"] * df.loc[mask, "field"].map(factors)
    return df


def rows_to_messages(df, location, location_id=OPENAQ_LOCATION_ID):
    """Transforme le CSV « long » (une ligne par mesure) en messages du contrat."""
    df = df[df["parameter"].isin(PARAM_MAP)].copy()
    if df.empty:
        return []
    df["field"] = df["parameter"].map(PARAM_MAP)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = convert_units(df)
    df["timestamp"] = df["datetime_utc"].map(to_iso_utc)

    # Pivot : une ligne par horodatage, une colonne par mesure
    wide = df.pivot_table(index="timestamp", columns="field", values="value",
                          aggfunc="mean").sort_index()

    messages = []
    for timestamp, row in wide.iterrows():
        measurements = {}
        for field in CONTRACT_FIELDS:
            value = row.get(field)
            missing = value is None or pd.isna(value)
            measurements[field] = None if missing else round(float(value), 2)
        messages.append({
            "sensor_id": f"OPENAQ_{location_id}",
            "timestamp": timestamp,
            "location": location,
            "measurements": measurements,
            "status": "active",
            "source": "openaq",
            "injected_anomaly": None,
        })
    return messages


def main():
    parser = argparse.ArgumentParser(description="Rejeu OpenAQ -> Kafka (topic raw_data)")
    parser.add_argument("--interval", type=float, default=0.2,
                        help="secondes entre deux messages (accéléré)")
    parser.add_argument("--limit", type=int, default=None, help="nombre maximum de messages")
    args = parser.parse_args()

    # Import ici : la partie transformation reste utilisable sans Kafka
    from ingestion.producer import build_producer, send_message, stats

    if not CSV_PATH.exists():
        raise SystemExit(f"Fichier introuvable : {CSV_PATH}. Lance d'abord AQ-14 "
                         "(python -m ingestion.openaq_fetch ...).")

    messages = rows_to_messages(pd.read_csv(CSV_PATH), load_coordinates())
    if args.limit:
        messages = messages[: args.limit]
    print(f"{len(messages)} messages à rejouer ({messages[0]['timestamp'] if messages else '-'} "
          f"-> {messages[-1]['timestamp'] if messages else '-'})")

    producer = build_producer(client_id="aq-openaq-replay")
    try:
        for i, message in enumerate(messages, start=1):
            send_message(producer, message)
            if i % 50 == 0:
                print(f"{i}/{len(messages)} envoyés")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("Arrêt demandé…")
    finally:
        producer.flush(10)
        print(f"Terminé : {stats['sent']} confirmés, {stats['failed']} échecs.")


if __name__ == "__main__":
    main()
