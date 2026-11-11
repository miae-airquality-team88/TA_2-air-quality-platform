"""Validation et nettoyage des messages capteurs (AQ-13).

Deux niveaux de contrôle, appliqués à chaque message lu dans raw_data :

1. STRUCTURE (modèle Pydantic SensorMessage) : champs présents, bons types,
   valeurs autorisées. Un message mal formé est REJETÉ (accepted = False) :
   on ne peut rien en faire.

2. CONTENU (plages physiques, voir schemas/README.md) : une mesure absente,
   négative ou hors plage est signalée comme « problème de qualité » (issue)
   puis remplacée par None dans le message nettoyé. Le message est GARDÉ
   (accepted = True) : ses autres mesures restent exploitables.

Utilisation :
    from streaming.validation import validate
    result = validate(raw)          # raw = dict lu dans Kafka
    result.accepted                 # False si la structure est invalide
    result.message                  # message nettoyé (dict) ou None
    result.issues                   # liste de problèmes (dicts)
    result.is_clean                 # True si accepté et sans aucun problème
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

# Plages physiques valides (schemas/README.md, contrat v1.0)
VALID_RANGES = {
    "pm2_5": (0.0, 1000.0),
    "pm10": (0.0, 2000.0),
    "no2": (0.0, 2000.0),
    "o3": (0.0, 1000.0),
    "temperature": (-40.0, 60.0),
    "humidity": (0.0, 100.0),
}

TIMESTAMP_PATTERN = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"


# ----------------------------------------------------------------------
# Modèles Pydantic : la STRUCTURE attendue (miroir du contrat JSON Schema)
# ----------------------------------------------------------------------
class Location(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)


class Measurements(BaseModel):
    # Chaque mesure est obligatoire mais peut valoir None (« float | None » sans valeur par défaut)
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)
    pm2_5: Optional[float]
    pm10: Optional[float]
    no2: Optional[float]
    o3: Optional[float]
    temperature: Optional[float]
    humidity: Optional[float]


class SensorMessage(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)
    sensor_id: str = Field(pattern=r"^(CAP_[0-9]{3}|OPENAQ_[0-9]+)$")
    timestamp: str = Field(pattern=TIMESTAMP_PATTERN)
    location: Location
    measurements: Measurements
    status: Literal["active", "inactive", "maintenance"]
    source: Literal["simulator", "openaq"]
    injected_anomaly: Optional[
        Literal["spike", "negative", "out_of_range", "stuck", "missing_values"]
    ] = None


    @field_validator("timestamp")
    @classmethod
    def valid_calendar_date(cls, value):
        """Reject impossible dates after checking the UTC wire format."""
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
        return value


# ----------------------------------------------------------------------
# Résultat de la validation
# ----------------------------------------------------------------------
@dataclass
class ValidationResult:
    accepted: bool
    message: Optional[dict]
    issues: list = field(default_factory=list)

    @property
    def is_clean(self):
        return self.accepted and not self.issues


def _issue(field_name, kind, value, detail):
    return {"field": field_name, "kind": kind, "value": value, "detail": detail}


def check_ranges(measurements):
    """Contrôle le CONTENU des mesures.

    Renvoie (mesures_nettoyées, liste_de_problèmes). Types de problème :
      missing      : mesure absente (None)
      negative     : valeur < 0 alors que le minimum physique est 0
      out_of_range : valeur hors de la plage valide
    """
    cleaned, issues = {}, []
    for name, (low, high) in VALID_RANGES.items():
        value = measurements.get(name)
        if value is None:
            issues.append(_issue(name, "missing", None, "mesure absente"))
            cleaned[name] = None
        elif value < 0 <= low:
            issues.append(_issue(name, "negative", value, f"valeur négative (minimum {low})"))
            cleaned[name] = None
        elif not low <= value <= high:
            issues.append(_issue(name, "out_of_range", value, f"hors plage [{low} ; {high}]"))
            cleaned[name] = None
        else:
            cleaned[name] = value
    return cleaned, issues


def validate(raw):
    """Valide un message brut (dict) : structure puis contenu."""
    if not isinstance(raw, dict):
        issue = _issue(None, "schema", None, "le message n'est pas un objet JSON")
        return ValidationResult(False, None, [issue])
    try:
        model = SensorMessage.model_validate(raw)
    except ValidationError as exc:
        issues = [
            _issue(".".join(str(p) for p in err["loc"]) or None, "schema", None, err["msg"])
            for err in exc.errors()
        ]
        return ValidationResult(False, None, issues)

    message = model.model_dump()
    message["measurements"], issues = check_ranges(message["measurements"])
    return ValidationResult(True, message, issues)
