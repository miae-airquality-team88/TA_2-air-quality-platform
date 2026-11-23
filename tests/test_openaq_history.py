"""Tests du chargement de l'historique OpenAQ (AQ-37), sans appel réseau."""
import pandas as pd

from ingestion.openaq_history import load_messages, openaq_offset, per_day
from ingestion.openaq_replay import rows_to_messages

LOCATION = {"lat": 30.42, "lon": -9.6}


def fake_openaq_csv(days=2):
    """24 heures par jour, PM2.5 et NO2 (en ppm, à convertir)."""
    rows = []
    for d in range(days):
        for h in range(24):
            ts = f"2026-11-{d + 1:02d}T{h:02d}:00:00Z"
            rows.append({"datetime_utc": ts, "sensor_id": 1, "parameter": "pm25",
                         "value": 10 + h, "unit": "µg/m³"})
            rows.append({"datetime_utc": ts, "sensor_id": 2, "parameter": "no2",
                         "value": 0.02, "unit": "ppm"})
    return pd.DataFrame(rows)


def stored(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*), MIN(source), MAX(no2) FROM measurements")
        return cur.fetchone()


def test_identifiant_fixe():
    assert openaq_offset("2026-11-01T01:00:00Z") - openaq_offset("2026-11-01T00:00:00Z") == 3600


def test_bilan_par_jour():
    messages = rows_to_messages(fake_openaq_csv(), LOCATION, location_id=3847229)
    assert per_day(messages) == {"2026-11-01": 24, "2026-11-02": 24}


def test_chargement_et_idempotence(db_conn):
    messages = rows_to_messages(fake_openaq_csv(), LOCATION, location_id=3847229)
    load_messages(db_conn, messages)
    assert stored(db_conn) == (48, "openaq", 37.6)        # NO2 converti en µg/m³
    load_messages(db_conn, messages)                       # relance
    assert stored(db_conn)[0] == 48                        # aucun doublon
