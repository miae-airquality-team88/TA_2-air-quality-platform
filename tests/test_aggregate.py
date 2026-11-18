"""Tests de la tâche Aggregate (AQ-31). Ignorés si PostgreSQL n'est pas démarré."""
from datetime import datetime, timezone

import pytest

from orchestration import aggregate
from orchestration.aggregate import AggregationError, run_daily_aggregate


def insert(conn, sensor, hour, offset, day=16):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO measurements (sensor_id, source, measured_at, pm2_5, is_valid, "
            "kafka_partition, kafka_offset) VALUES (%s, 'simulator', %s, 10, true, 0, %s)",
            (sensor, datetime(2026, 11, day, hour, tzinfo=timezone.utc), offset),
        )
    conn.commit()


def test_agregation_et_reconciliation(db_conn):
    insert(db_conn, "CAP_001", 8, 1)
    insert(db_conn, "CAP_001", 9, 2)
    insert(db_conn, "CAP_002", 10, 3)
    insert(db_conn, "CAP_002", 10, 4, day=17)   # autre journée, non comptée
    result = run_daily_aggregate(db_conn, "2026-11-16")
    assert result == {"day": "2026-11-16", "sensors": 2, "measurements": 3}


def test_relancer_ne_change_rien(db_conn):
    insert(db_conn, "CAP_001", 8, 1)
    premier = run_daily_aggregate(db_conn, "2026-11-16")
    assert run_daily_aggregate(db_conn, "2026-11-16") == premier


def test_journee_vide(db_conn):
    assert run_daily_aggregate(db_conn, "2026-11-16") == {
        "day": "2026-11-16", "sensors": 0, "measurements": 0}


def test_reconciliation_detecte_une_perte(db_conn, monkeypatch):
    insert(db_conn, "CAP_001", 8, 1)
    original = aggregate.aggregate_day

    def corrupted(conn, day):
        count = original(conn, day)
        with conn.cursor() as cur:
            cur.execute("UPDATE daily_aggregates SET measurement_count = 0 WHERE day = %s",
                        (day,))
        conn.commit()
        return count

    monkeypatch.setattr(aggregate, "aggregate_day", corrupted)
    with pytest.raises(AggregationError, match="1 mesures en base, 0"):
        run_daily_aggregate(db_conn, "2026-11-16")


def test_anomalies_sans_mesures(db_conn):
    with db_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO anomalies (sensor_id, source, measured_at, category, "
            "anomaly_type, severity) VALUES "
            "('CAP_001', 'simulator', '2026-11-16T08:00:00Z', 'qualite_donnees', "
            "'json_invalide', 'warning')")
    db_conn.commit()
    assert run_daily_aggregate(db_conn, "2026-11-16") == {
        "day": "2026-11-16", "sensors": 1, "measurements": 0}
    with db_conn.cursor() as cur:
        cur.execute("SELECT anomaly_count, completeness FROM daily_aggregates")
        assert cur.fetchone() == (1, None)


def test_frontieres_utc(db_conn):
    insert(db_conn, "CAP_001", 23, 1, day=15)
    insert(db_conn, "CAP_001", 0, 2)
    insert(db_conn, "CAP_001", 23, 3)
    insert(db_conn, "CAP_001", 0, 4, day=17)
    with db_conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'America/New_York'")
    assert run_daily_aggregate(db_conn, "2026-11-16")["measurements"] == 2


def test_nouvelle_mesure_recalcule_les_agregats(db_conn):
    insert(db_conn, "CAP_001", 8, 1)
    run_daily_aggregate(db_conn, "2026-11-16")
    insert(db_conn, "CAP_001", 9, 2)
    assert run_daily_aggregate(db_conn, "2026-11-16")["measurements"] == 2
    with db_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM daily_aggregates")
        assert cur.fetchone()[0] == 1


def test_date_invalide_avant_acces_base():
    with pytest.raises(ValueError):
        run_daily_aggregate(None, "2026-02-30")
