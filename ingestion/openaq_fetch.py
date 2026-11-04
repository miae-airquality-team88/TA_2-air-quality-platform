"""Téléchargement de l'historique de la station OpenAQ du projet (AQ-14).

Station du sujet : https://explore.openaq.org/locations/3847229
API utilisée : OpenAQ v3 (https://docs.openaq.org), clé gratuite dans .env (OPENAQ_API_KEY).

Usage (depuis la racine du dépôt) :
    python -m ingestion.openaq_fetch --date-from 2026-10-01 --date-to 2026-11-01
    python -m ingestion.openaq_fetch --date-from 2026-10-01 --date-to 2026-11-01 --endpoint hours

Fichiers produits (dossier data/, non versionné) :
    data/openaq/location_<id>.json   métadonnées : nom, coordonnées, capteurs
    data/openaq/location_<id>.csv    une ligne par mesure :
                                     datetime_utc, sensor_id, parameter, value, unit
"""
import argparse
import json
import time

import pandas as pd
import requests

from common.config import DATA_DIR, OPENAQ_API_KEY, OPENAQ_LOCATION_ID

BASE_URL = "https://api.openaq.org/v3"
OUT_DIR = DATA_DIR / "openaq"
PAGE_LIMIT = 1000


def get(path, params=None):
    """Appel GET à l'API OpenAQ avec la clé. Attend et réessaie si la limite est atteinte."""
    if not OPENAQ_API_KEY:
        raise SystemExit("OPENAQ_API_KEY manquante : ajoute ta clé dans le fichier .env")
    for _ in range(3):
        response = requests.get(f"{BASE_URL}{path}", params=params, timeout=60,
                                headers={"X-API-Key": OPENAQ_API_KEY})
        if response.status_code == 429:  # trop de requêtes : on patiente une minute
            print("Limite de l'API atteinte, pause de 60 s…")
            time.sleep(60)
            continue
        response.raise_for_status()
        return response.json()
    raise SystemExit("L'API OpenAQ refuse les requêtes (limite atteinte). Réessaie plus tard.")


def fetch_location(location_id):
    """Renvoie les métadonnées de la station (nom, coordonnées, liste des capteurs)."""
    results = get(f"/locations/{location_id}")["results"]
    if not results:
        raise SystemExit(f"Station {location_id} introuvable.")
    return results[0]


def parse_measurements(results, sensor_id):
    """Transforme la réponse de l'API en lignes simples (une par mesure)."""
    rows = []
    for r in results:
        period = r.get("period") or {}
        parameter = r.get("parameter") or {}
        rows.append({
            "datetime_utc": (period.get("datetimeFrom") or {}).get("utc"),
            "sensor_id": sensor_id,
            "parameter": parameter.get("name"),
            "value": r.get("value"),
            "unit": parameter.get("units"),
        })
    return rows


def fetch_sensor(sensor_id, date_from, date_to, endpoint="measurements"):
    """Télécharge toutes les mesures d'un capteur sur la période, page par page."""
    rows, page = [], 1
    while True:
        data = get(f"/sensors/{sensor_id}/{endpoint}", {
            "datetime_from": f"{date_from}T00:00:00Z",
            "datetime_to": f"{date_to}T00:00:00Z",
            "limit": PAGE_LIMIT,
            "page": page,
        })
        results = data.get("results", [])
        rows.extend(parse_measurements(results, sensor_id))
        if len(results) < PAGE_LIMIT:
            return rows
        page += 1
        time.sleep(1)  # reste sous la limite d'environ 60 requêtes par minute


def main():
    parser = argparse.ArgumentParser(description="Historique de la station OpenAQ")
    parser.add_argument("--date-from", required=True, help="AAAA-MM-JJ")
    parser.add_argument("--date-to", required=True, help="AAAA-MM-JJ")
    parser.add_argument("--endpoint", choices=["measurements", "hours"], default="measurements",
                        help="measurements = valeurs brutes, hours = moyennes horaires")
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    location = fetch_location(OPENAQ_LOCATION_ID)
    meta_path = OUT_DIR / f"location_{OPENAQ_LOCATION_ID}.json"
    meta_path.write_text(json.dumps(location, indent=2, ensure_ascii=False), encoding="utf-8")

    coords = location.get("coordinates") or {}
    print(f"Station : {location.get('name')} ({location.get('locality')})")
    print(f"Coordonnées : lat={coords.get('latitude')}, lon={coords.get('longitude')}")

    all_rows = []
    for sensor in location.get("sensors", []):
        parameter = sensor.get("parameter") or {}
        print(f"Capteur {sensor['id']} : {parameter.get('name')} ({parameter.get('units')})…")
        rows = fetch_sensor(sensor["id"], args.date_from, args.date_to, args.endpoint)
        print(f"  {len(rows)} mesures")
        all_rows.extend(rows)

    if not all_rows:
        print("Aucune mesure sur cette période : essaie une autre période ou --endpoint hours.")
        return

    df = pd.DataFrame(all_rows).sort_values(["datetime_utc", "parameter"])
    csv_path = OUT_DIR / f"location_{OPENAQ_LOCATION_ID}.csv"
    df.to_csv(csv_path, index=False)
    print(f"\n{len(df)} mesures enregistrées dans {csv_path}")
    print("Période :", df["datetime_utc"].min(), "->", df["datetime_utc"].max())
    print(df.groupby(["parameter", "unit"]).size().rename("nombre").to_string())


if __name__ == "__main__":
    main()
