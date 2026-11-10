# Contrat de données — messages capteurs (v1.0)

Fichier de référence : [`sensor_message.json`](sensor_message.json) (JSON Schema, draft 2020-12).
Il décrit le format de **tous** les messages publiés dans le topic Kafka `raw_data`,
qu'ils viennent du simulateur (AQ-9) ou de la station OpenAQ (AQ-16).

Toute modification du contrat passe par une Pull Request et augmente la version.

## Champs

| Champ | Type | Obligatoire | Description |
|---|---|---|---|
| `sensor_id` | texte | oui | `CAP_001`… (simulé) ou `OPENAQ_<id>` (station réelle) |
| `timestamp` | texte | oui | Date et heure UTC, format `2026-11-02T14:30:00Z` |
| `location.lat` / `location.lon` | nombre | oui | Coordonnées GPS en degrés |
| `measurements` | objet | oui | Les 6 mesures ci-dessous (une mesure absente vaut `null`) |
| `status` | texte | oui | `active`, `inactive` ou `maintenance` |
| `source` | texte | oui | `simulator` ou `openaq` (lignée des données) |
| `injected_anomaly` | texte ou `null` | non | Type d'anomalie injectée par le simulateur, `null` sinon |

## Mesures : unités et plages valides

| Mesure | Unité | Plage valide | Seuil OMS (24 h) |
|---|---|---|---|
| `pm2_5` | µg/m³ | 0 – 1000 | 15 |
| `pm10` | µg/m³ | 0 – 2000 | 45 |
| `no2` | µg/m³ | 0 – 2000 | 25 |
| `o3` | µg/m³ | 0 – 1000 | 100 (sur 8 h) |
| `temperature` | °C | -40 – 60 | — |
| `humidity` | % | 0 – 100 | — |

**Rôle du schéma :** il vérifie la **forme** du message (champs présents, bons types).
Il accepte volontairement `null` et les valeurs hors plage : ce sont des anomalies de **contenu**,
détectées au Sprint 2 par la validation (AQ-13) à partir des plages ci-dessus.

## Types d'anomalies injectées (`injected_anomaly`)

| Valeur | Signification |
|---|---|
| `spike` | pic : une mesure de pollution multipliée par 5 à 10 |
| `negative` | une mesure négative (physiquement impossible) |
| `out_of_range` | humidité > 100 % ou température > 60 °C |
| `stuck` | capteur bloqué : mêmes valeurs que le message précédent |
| `missing_values` | une à trois mesures absentes (`null`) |

## Exemple
Voir [`examples/valid_message.json`](examples/valid_message.json).

## Vérifier un message en Python
```python
from common.contract import validate_message
erreurs = validate_message(message)   # liste vide = message conforme
```

## Validation et nettoyage en streaming (AQ-13)

`streaming.validation.validate(raw)` applique le contrat avec Pydantic et renvoie
`accepted`, `message`, `issues` et `is_clean`. Les champs inconnus, nombres sous
forme de texte, booléens utilisés comme nombres, valeurs non finies et dates UTC
impossibles sont rejetés. Les entiers sont acceptés comme nombres.

Une structure invalide donne `accepted=False` et `message=None`. Une structure
valide reste acceptée même si une mesure est absente ou hors plage : cette mesure
est remplacée par `None`, avec un problème `missing`, `negative` ou `out_of_range`.
Les autres mesures et le message original sont conservés. Les bornes du tableau
ci-dessus sont inclusives. Les pics et capteurs bloqués nécessitent les règles
ou fenêtres des autres tickets, ils ne sont pas détectés par cette seule validation.

Après récupération du code : `pip install -r requirements.txt`.
Tests ciblés : `pytest -q tests/test_validation.py`.
