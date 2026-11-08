"""Tests de la transformation OpenAQ -> messages du contrat (AQ-16), sans Kafka."""
import pandas as pd

from common.contract import validate_message
from ingestion.openaq_replay import rows_to_messages

LOCATION = {"lat": 30.42, "lon": -9.6}


def petit_csv():
    return pd.DataFrame([
        # 2e heure écrite en premier pour vérifier le tri
        {"datetime_utc": "2026-10-01T11:00:00Z", "sensor_id": 1, "parameter": "pm25",
         "value": 20.0, "unit": "µg/m³"},
        {"datetime_utc": "2026-10-01T10:00:00Z", "sensor_id": 1, "parameter": "pm25",
         "value": 12.0, "unit": "µg/m³"},
        {"datetime_utc": "2026-10-01T10:00:00Z", "sensor_id": 2, "parameter": "no2",
         "value": 0.02, "unit": "ppm"},
        {"datetime_utc": "2026-10-01T10:00:00Z", "sensor_id": 3, "parameter": "pm1",
         "value": 5.0, "unit": "µg/m³"},  # paramètre hors contrat : ignoré
    ])


def test_un_message_par_heure_trie():
    messages = rows_to_messages(petit_csv(), LOCATION, location_id=3847229)
    assert [m["timestamp"] for m in messages] == ["2026-10-01T10:00:00Z", "2026-10-01T11:00:00Z"]


def test_messages_conformes_au_contrat():
    for message in rows_to_messages(petit_csv(), LOCATION, location_id=3847229):
        assert validate_message(message) == []
        assert message["sensor_id"] == "OPENAQ_3847229"
        assert message["source"] == "openaq"


def test_mesure_absente_vaut_none():
    premier = rows_to_messages(petit_csv(), LOCATION)[0]
    assert premier["measurements"]["o3"] is None
    assert premier["measurements"]["pm2_5"] == 12.0


def test_conversion_ppm():
    premier = rows_to_messages(petit_csv(), LOCATION)[0]
    assert premier["measurements"]["no2"] == 37.6  # 0.02 ppm x 1880
