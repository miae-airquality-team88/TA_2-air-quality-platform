"""Tests du contrat de données (AQ-15)."""
import json

from jsonschema import Draft202012Validator

from common.config import ROOT_DIR, SCHEMA_PATH
from common.contract import is_valid, validate_message

EXAMPLES = ROOT_DIR / "schemas" / "examples"


def load(path):
    return json.loads(path.read_text(encoding="utf-8"))


def test_le_schema_est_un_json_schema_valide():
    Draft202012Validator.check_schema(load(SCHEMA_PATH))


def test_exemple_valide_accepte():
    assert validate_message(load(EXAMPLES / "valid_message.json")) == []


def test_exemple_invalide_refuse():
    erreurs = validate_message(load(EXAMPLES / "invalid_message.json"))
    assert len(erreurs) >= 5  # sensor_id, timestamp, lat/lon, pm2_5, humidity, status...


def test_mesure_absente_acceptee():
    message = load(EXAMPLES / "valid_message.json")
    message["measurements"]["no2"] = None
    assert is_valid(message)


def test_source_inconnue_refusee():
    message = load(EXAMPLES / "valid_message.json")
    message["source"] = "excel"
    assert not is_valid(message)


def test_champ_en_trop_refuse():
    message = load(EXAMPLES / "valid_message.json")
    message["commentaire"] = "test"
    assert not is_valid(message)
