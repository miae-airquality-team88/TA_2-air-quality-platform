"""Contrôles qualité des données d'une journée (AQ-29).

Tâche « Validate Quality » du DAG quotidien. Trois contrôles :

| Contrôle   | Question posée                                        | Seuil par défaut |
|------------|-------------------------------------------------------|------------------|
| volume     | Assez de mesures pour la journée ?                    | >= 100 mesures   |
| completude | Quelle part des mesures est valide (sans problème) ?  | >= 80 %          |
| fraicheur  | La donnée la plus récente date-t-elle de moins de X h ? | <= 26 h        |

La fraîcheur n'a de sens que pour une journée récente : pour une journée de
plus de 2 jours (rejeu, backfill), elle est marquée « non applicable ».

Les résultats sont TOUJOURS enregistrés dans la table quality_checks (même en
cas d'échec), puis la tâche échoue si un contrôle n'est pas respecté : le DAG
s'arrête et on n'entraîne pas de modèle sur des données de mauvaise qualité.

Les seuils peuvent être changés sans toucher au code, avec des variables
d'environnement : QUALITY_MIN_ROWS, QUALITY_MIN_COMPLETENESS, QUALITY_MAX_AGE_HOURS.
"""
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from math import isfinite

from common.config import quality_thresholds


@dataclass
class CheckResult:
    name: str
    value: float | None
    threshold: float
    passed: bool
    detail: str


class QualityCheckError(Exception):
    """Levée quand au moins un contrôle qualité échoue."""


def default_thresholds():
    return quality_thresholds()


DAY_STATS_SQL = """
SELECT COUNT(*), COUNT(*) FILTER (WHERE is_valid)
FROM measurements
WHERE measured_at >= %(day)s::timestamp AT TIME ZONE 'UTC'
  AND measured_at < (%(day)s::date + 1)::timestamp AT TIME ZONE 'UTC'
"""


def compute_checks(rows, valid_rows, latest, day, now, thresholds):
    """Applique les 3 contrôles à des chiffres déjà calculés (fonction pure, facile à tester)."""
    if isinstance(day, str):
        day = date.fromisoformat(day)
    if now.tzinfo is None or (latest is not None and latest.tzinfo is None):
        raise ValueError("Les dates doivent inclure un fuseau horaire")
    now = now.astimezone(timezone.utc)
    if not 0 <= valid_rows <= rows:
        raise ValueError("Les nombres de mesures sont incohérents")
    if (not all(isfinite(v) for v in thresholds.values())
            or thresholds["min_rows"] < 1
            or not 0 <= thresholds["min_completeness"] <= 1
            or thresholds["max_age_hours"] < 0):
        raise ValueError("Seuils qualité invalides")
    results = []

    min_rows = thresholds["min_rows"]
    results.append(CheckResult("volume", rows, min_rows, rows >= min_rows,
                               f"{rows} mesures (minimum {min_rows})"))

    min_comp = thresholds["min_completeness"]
    completeness = valid_rows / rows if rows else 0.0
    results.append(CheckResult("completude", completeness, min_comp, completeness >= min_comp,
                               f"{valid_rows}/{rows} mesures valides ({completeness:.1%})"))

    max_age = thresholds["max_age_hours"]
    if day < now.date() - timedelta(days=2):
        results.append(CheckResult("fraicheur", None, max_age, True,
                                   "non applicable : journée ancienne (rejeu ou backfill)"))
    elif latest is None:
        results.append(CheckResult("fraicheur", None, max_age, False, "aucune mesure en base"))
    else:
        age = (now - latest).total_seconds() / 3600
        results.append(CheckResult("fraicheur", age, max_age, 0 <= age <= max_age,
                                   f"dernière mesure il y a {age:.2f} h (maximum {max_age} h)"))
    return results


def run_quality_checks(conn, day, now=None, thresholds=None):
    """Calcule les contrôles d'une journée à partir de la table measurements."""
    now = now or datetime.now(timezone.utc)
    thresholds = default_thresholds() if thresholds is None else thresholds
    with conn.cursor() as cur:
        cur.execute(DAY_STATS_SQL, {"day": day})
        rows, valid_rows = cur.fetchone()
        cur.execute("SELECT MAX(measured_at) FROM measurements")
        latest = cur.fetchone()[0]
    return compute_checks(rows, valid_rows, latest, day, now, thresholds)


def save_checks(conn, day, results, run_id=None):
    """Enregistre les résultats dans quality_checks."""
    with conn.cursor() as cur:
        for r in results:
            cur.execute(
                "INSERT INTO quality_checks (day, run_id, check_name, value, threshold, passed, "
                "detail) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (day, run_id, r.name, r.value, r.threshold, r.passed, r.detail),
            )
    conn.commit()


def validate_day(conn, day, run_id=None, now=None, thresholds=None):
    """Point d'entrée du DAG : contrôle, enregistre, puis échoue si besoin.

    Renvoie la liste des résultats (dicts) si tout est bon.
    """
    if isinstance(day, str):
        day = date.fromisoformat(day)
    results = run_quality_checks(conn, day, now=now, thresholds=thresholds)
    save_checks(conn, day, results, run_id)
    for r in results:
        print(f"[{'OK' if r.passed else 'ÉCHEC'}] {r.name} : {r.detail}")
    failed = [r.name for r in results if not r.passed]
    if failed:
        raise QualityCheckError(f"Contrôles qualité en échec pour le {day} : {', '.join(failed)}")
    return [asdict(r) for r in results]
