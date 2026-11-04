"""Tests de la lecture des réponses OpenAQ (AQ-14), sans appel réseau."""
from ingestion.openaq_fetch import parse_measurements

REPONSE_EXEMPLE = [
    {
        "value": 12.4,
        "parameter": {"id": 2, "name": "pm25", "units": "µg/m³"},
        "period": {"datetimeFrom": {"utc": "2026-10-01T10:00:00Z"},
                   "datetimeTo": {"utc": "2026-10-01T11:00:00Z"}},
    },
    {
        "value": 0.021,
        "parameter": {"id": 7, "name": "no2", "units": "ppm"},
        "period": {"datetimeFrom": {"utc": "2026-10-01T10:00:00Z"}},
    },
]


def test_une_ligne_par_mesure():
    rows = parse_measurements(REPONSE_EXEMPLE, sensor_id=555)
    assert len(rows) == 2
    assert rows[0] == {"datetime_utc": "2026-10-01T10:00:00Z", "sensor_id": 555,
                       "parameter": "pm25", "value": 12.4, "unit": "µg/m³"}
    assert rows[1]["unit"] == "ppm"


def test_reponse_vide():
    assert parse_measurements([], sensor_id=1) == []
