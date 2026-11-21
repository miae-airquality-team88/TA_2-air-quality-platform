"""Tests de la tâche Extract (AQ-27). Ignorés si PostgreSQL n'est pas démarré."""
from datetime import datetime, timezone

import pandas as pd

from orchestration.extract import extract_day


def insert(conn, sensor, day, hour, offset, valid=True):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO measurements (sensor_id, source, measured_at, pm2_5, is_valid, "
            "kafka_partition, kafka_offset) VALUES (%s, 'simulator', %s, 12.5, %s, 0, %s)",
            (sensor, datetime(2026, 11, day, hour, tzinfo=timezone.utc), valid, offset),
        )
    conn.commit()


def test_extraction_d_une_journee(db_conn, tmp_path):
    insert(db_conn, "CAP_001", 16, 0, 1)
    insert(db_conn, "CAP_002", 16, 23, 2, valid=False)
    insert(db_conn, "CAP_001", 17, 0, 3)          # minuit le lendemain : exclu
    summary = extract_day(db_conn, "2026-11-16", tmp_path)
    assert summary["rows"] == 2 and summary["valid_rows"] == 1 and summary["sensors"] == 2
    df = pd.read_csv(summary["path"])
    assert list(df["sensor_id"]) == ["CAP_001", "CAP_002"]
    assert "pm2_5_mean_15min" in df.columns


def test_journee_vide(db_conn, tmp_path):
    summary = extract_day(db_conn, "2026-11-16", tmp_path)
    assert summary["rows"] == 0
    assert pd.read_csv(summary["path"]).empty
