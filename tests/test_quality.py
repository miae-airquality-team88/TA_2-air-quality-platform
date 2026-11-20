"""Tests des contrôles qualité (AQ-29)."""
from datetime import date, datetime, timedelta, timezone

import pytest

from orchestration.quality import (
    QualityCheckError,
    compute_checks,
    default_thresholds,
    validate_day,
)

NOW = datetime(2026, 11, 17, 8, 0, tzinfo=timezone.utc)
DAY = date(2026, 11, 16)
SEUILS = {"min_rows": 100, "min_completeness": 0.8, "max_age_hours": 26}


def by_name(results):
    return {r.name: r for r in results}


def test_journee_correcte():
    """Exemple chiffré : 1000 mesures, 950 valides, dernière mesure il y a 8 h."""
    r = by_name(compute_checks(1000, 950, NOW - timedelta(hours=8), DAY, NOW, SEUILS))
    assert r["volume"].passed
    assert r["completude"].value == 0.95 and r["completude"].passed
    assert r["fraicheur"].value == 8.0 and r["fraicheur"].passed


def test_volume_et_completude_insuffisants():
    r = by_name(compute_checks(50, 30, NOW, DAY, NOW, SEUILS))
    assert not r["volume"].passed
    assert r["completude"].value == 0.6 and not r["completude"].passed


def test_donnees_trop_anciennes():
    r = by_name(compute_checks(1000, 1000, NOW - timedelta(hours=40), DAY, NOW, SEUILS))
    assert not r["fraicheur"].passed


def test_fraicheur_non_applicable_pour_une_vieille_journee():
    vieille = date(2026, 10, 1)
    r = by_name(compute_checks(1000, 1000, NOW - timedelta(days=30), vieille, NOW, SEUILS))
    assert r["fraicheur"].passed and r["fraicheur"].value is None


def test_aucune_mesure():
    r = by_name(compute_checks(0, 0, None, DAY, NOW, SEUILS))
    assert not r["volume"].passed and r["completude"].value == 0.0
    assert not r["fraicheur"].passed


def test_seuils_sans_arrondi_avant_comparaison():
    r = by_name(compute_checks(
        100000, 79999, NOW - timedelta(hours=26, seconds=1), DAY, NOW, SEUILS))
    assert not r["completude"].passed
    assert not r["fraicheur"].passed


def test_seuils_exacts_acceptes():
    r = compute_checks(100, 80, NOW - timedelta(hours=26), DAY, NOW, SEUILS)
    assert all(check.passed for check in r)


def test_date_future_rejetee():
    r = by_name(compute_checks(100, 100, NOW + timedelta(minutes=1), DAY, NOW, SEUILS))
    assert not r["fraicheur"].passed


@pytest.mark.parametrize("key,value", [
    ("min_rows", 0), ("min_completeness", 1.1),
    ("max_age_hours", -1), ("max_age_hours", float("nan")),
])
def test_seuil_invalide_rejete(key, value):
    with pytest.raises(ValueError, match="Seuils"):
        compute_checks(100, 100, NOW, DAY, NOW, {**SEUILS, key: value})


def test_configuration_environnement(monkeypatch):
    monkeypatch.setenv("QUALITY_MIN_ROWS", "200")
    monkeypatch.setenv("QUALITY_MIN_COMPLETENESS", "0.9")
    monkeypatch.setenv("QUALITY_MAX_AGE_HOURS", "48")
    assert default_thresholds() == {
        "min_rows": 200, "min_completeness": 0.9, "max_age_hours": 48}


def test_fraicheur_requise_jusqua_deux_jours():
    day = NOW.date() - timedelta(days=2)
    r = by_name(compute_checks(100, 100, NOW - timedelta(hours=49), day, NOW, SEUILS))
    assert not r["fraicheur"].passed


def test_jour_backfill_selon_date_utc():
    local_now = NOW.astimezone(timezone(timedelta(hours=-10)))
    r = by_name(compute_checks(100, 100, None, date(2026, 11, 14), local_now, SEUILS))
    assert r["fraicheur"].passed


def insert_measurements(conn, n, n_valid, measured_at):
    with conn.cursor() as cur:
        for i in range(n):
            cur.execute(
                "INSERT INTO measurements (sensor_id, source, measured_at, is_valid, "
                "kafka_partition, kafka_offset) VALUES ('CAP_001', 'simulator', %s, %s, 0, %s)",
                (measured_at, i < n_valid, i),
            )
    conn.commit()


def count_checks(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*), COUNT(*) FILTER (WHERE passed) FROM quality_checks")
        return cur.fetchone()


def test_validate_day_reussi_et_historise(db_conn):
    insert_measurements(db_conn, 120, 110, datetime(2026, 11, 16, 20, 0, tzinfo=timezone.utc))
    results = validate_day(db_conn, "2026-11-16", run_id="test", now=NOW, thresholds=SEUILS)
    assert [r["name"] for r in results] == ["volume", "completude", "fraicheur"]
    assert count_checks(db_conn) == (3, 3)


def test_validate_day_echoue_mais_historise(db_conn):
    insert_measurements(db_conn, 20, 20, datetime(2026, 11, 16, 20, 0, tzinfo=timezone.utc))
    with pytest.raises(QualityCheckError, match="volume"):
        validate_day(db_conn, "2026-11-16", now=NOW, thresholds=SEUILS)
    assert count_checks(db_conn) == (3, 2)   # enregistré malgré l'échec


def test_journee_vide_historise_tous_les_echecs(db_conn):
    with pytest.raises(QualityCheckError, match="volume, completude, fraicheur"):
        validate_day(db_conn, DAY, run_id="empty", now=NOW, thresholds=SEUILS)
    assert count_checks(db_conn) == (3, 0)
    with db_conn.cursor() as cur:
        cur.execute("SELECT DISTINCT day, run_id FROM quality_checks")
        assert cur.fetchall() == [(DAY, "empty")]


def test_nouvelle_tentative_conserve_historique(db_conn):
    insert_measurements(db_conn, 120, 110, NOW - timedelta(hours=12))
    validate_day(db_conn, DAY, run_id="run-1", now=NOW, thresholds=SEUILS)
    validate_day(db_conn, DAY, run_id="run-1", now=NOW, thresholds=SEUILS)
    assert count_checks(db_conn) == (6, 6)


def test_journee_utc_independante_du_fuseau_sql(db_conn):
    with db_conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'America/New_York'")
        for offset, instant in enumerate([
            datetime(2026, 11, 15, 23, 59, tzinfo=timezone.utc),
            datetime(2026, 11, 16, 0, 0, tzinfo=timezone.utc),
            datetime(2026, 11, 16, 23, 59, tzinfo=timezone.utc),
            datetime(2026, 11, 17, 0, 0, tzinfo=timezone.utc),
        ]):
            cur.execute(
                "INSERT INTO measurements (sensor_id, source, measured_at, is_valid, "
                "kafka_partition, kafka_offset) VALUES ('UTC', 'simulator', %s, true, 0, %s)",
                (instant, offset),
            )
    db_conn.commit()
    try:
        result = validate_day(db_conn, DAY, now=NOW, thresholds={**SEUILS, "min_rows": 2})
        assert result[0]["value"] == 2
        assert result[2]["value"] == 8   # fraîcheur globale, y compris le jour suivant
    finally:
        with db_conn.cursor() as cur:
            cur.execute("SET TIME ZONE 'UTC'")
        db_conn.commit()
