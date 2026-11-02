"""Validation d'un message par rapport au contrat de données (AQ-15).

Utilisation :
    from common.contract import validate_message, is_valid

    erreurs = validate_message(message)   # liste de textes, vide si le message est conforme
    if is_valid(message):
        ...
"""
import json
from functools import lru_cache

from jsonschema import Draft202012Validator

from common.config import SCHEMA_PATH


@lru_cache(maxsize=1)
def get_validator():
    """Charge le schéma une seule fois (mis en cache) et renvoie un validateur."""
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)  # vérifie que le schéma lui-même est correct
    return Draft202012Validator(schema)


def validate_message(message):
    """Renvoie la liste des erreurs du message (liste vide = conforme)."""
    errors = []
    for error in get_validator().iter_errors(message):
        chemin = "/".join(str(p) for p in error.path) or "(racine)"
        errors.append(f"{chemin} : {error.message}")
    return errors


def is_valid(message):
    """True si le message respecte le contrat."""
    return not validate_message(message)
