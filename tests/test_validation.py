"""Tests de la validation (AQ-13)."""
import copy
import json

import pytest

from common.config import ROOT_DIR
from ingestion.simulator import stream
from streaming.validation import VALID_RANGES, validate

EXAMPLES = ROOT_DIR / "schemas" / "examples"
VALID = json.loads((EXAMPLES / "valid_message.json").read_text(encoding="utf-8"))
INVALID = json.loads((EXAMPLES / "invalid_message.json").read_text(encoding="utf-8"))


def message(**measurements):
    m = copy.deepcopy(VALID)
    m["measurements"].update(measurements)
    return m


def kinds(result):
    return {(i["field"], i["kind"]) for i in result.issues}


def test_message_correct_accepte_sans_probleme():
    result = validate(VALID)
    assert result.accepted and result.is_clean
    assert result.message["measurements"] == VALID["measurements"]


def test_structure_invalide_rejetee():
    result = validate(INVALID)
    assert not result.accepted
    assert result.message is None
    assert all(i["kind"] == "schema" for i in result.issues)
    assert len(result.issues) >= 5


def test_pas_un_dictionnaire():
    assert not validate(["pas", "un", "objet"]).accepted


def test_valeur_negative_nettoyee():
    result = validate(message(pm10=-12.0))
    assert result.accepted and not result.is_clean
    assert ("pm10", "negative") in kinds(result)
    assert result.message["measurements"]["pm10"] is None
    assert result.message["measurements"]["pm2_5"] == 12.5  # les autres mesures sont gardées


def test_humidite_hors_plage():
    result = validate(message(humidity=150.0))
    assert ("humidity", "out_of_range") in kinds(result)
    assert result.message["measurements"]["humidity"] is None


def test_temperature_negative_autorisee():
    assert validate(message(temperature=-5.0)).is_clean


def test_mesure_absente():
    result = validate(message(no2=None))
    assert ("no2", "missing") in kinds(result)


def test_message_original_non_modifie():
    original = message(pm10=-12.0)
    copie = copy.deepcopy(original)
    validate(original)
    assert original == copie


def test_anomalies_du_simulateur_detectees():
    """Chaque anomalie de contenu injectée par le simulateur produit un problème de qualité."""
    attendu = {"negative": "negative", "out_of_range": "out_of_range", "missing_values": "missing"}
    for raw in stream(interval=0, count=60, anomaly_rate=0.5, seed=7):
        result = validate(raw)
        assert result.accepted
        injected = raw["injected_anomaly"]
        if injected in attendu:
            assert attendu[injected] in {i["kind"] for i in result.issues}, raw





@pytest.mark.parametrize("value", ["12.5", True, False, float("nan"), float("inf")])
def test_invalid_numeric_types_rejected(value):
    result = validate(message(pm10=value))
    assert not result.accepted
    assert ("measurements.pm10", "schema") in kinds(result)


@pytest.mark.parametrize("stamp", [
    "2026-02-30T10:00:00Z", "2026-11-10T24:00:00Z",
    "2026-11-10", "2026-11-10T10:00:00+01:00", 1794304800,
])
def test_invalid_timestamp_rejected(stamp):
    raw = message()
    raw["timestamp"] = stamp
    assert not validate(raw).accepted


@pytest.mark.parametrize("field", list(VALID_RANGES))
def test_required_measure_and_inclusive_boundaries(field):
    raw = message()
    del raw["measurements"][field]
    assert not validate(raw).accepted
    low, high = VALID_RANGES[field]
    for boundary in (low, high):
        assert validate(message(**{field: boundary})).is_clean
    for outside in (low - 0.1, high + 0.1):
        result = validate(message(**{field: outside}))
        assert result.accepted and not result.is_clean
        assert result.message["measurements"][field] is None


@pytest.mark.parametrize("container", [None, "location", "measurements"])
def test_unknown_fields_rejected(container):
    raw = message()
    target = raw if container is None else raw[container]
    target["unexpected"] = 1
    assert not validate(raw).accepted


@pytest.mark.parametrize("lat,lon", [(91, 0), (0, -181), (True, 0), ("30.4", 0)])
def test_invalid_coordinates_rejected(lat, lon):
    raw = message()
    raw["location"] = {"lat": lat, "lon": lon}
    assert not validate(raw).accepted


def test_openaq_and_leap_day_accepted():
    raw = message()
    raw.update(sensor_id="OPENAQ_123", source="openaq", timestamp="2024-02-29T00:00:00Z")
    raw.pop("injected_anomaly", None)
    assert validate(raw).is_clean
