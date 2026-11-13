"""Tests du stockage (AQ-22).

Les deux premiers tests n'ont pas besoin de PostgreSQL. Les suivants utilisent
la fixture db_conn (base de test) et sont ignorés si PostgreSQL n'est pas démarré.
"""
from datetime import date

from streaming.storage import (
    ANOMALY_COLUMNS,
    MEASUREMENT_COLUMNS,
    Storage,
    aggregate_day,
    anomaly_to_row,
    processed_to_row,
)


def processed(sensor="CAP_001", ts="2026-11-09T10:00:00Z", pm25=12.0, is_valid=True):
    return {
        "sensor_id": sensor, "timestamp": ts, "source": "simulator",
        "location": {"lat": 30.42, "lon": -9.6},
        "measurements": {"pm2_5": pm25, "pm10": 30.0, "no2": 35.0, "o3": 70.0,
                         "temperature": 22.0, "humidity": 60.0},
        "status": "active", "injected_anomaly": None,
        "quality": {"is_valid": is_valid, "issues": [] if is_valid else [{"kind": "missing"}]},
        "windows": {"pm2_5": {"mean_5min": 11.0, "mean_15min": 10.5},
                    "pm10": {"mean_5min": 29.0, "mean_15min": 28.0}},
        "anomaly_count": 0,
        "kafka_timestamp": "2026-11-09T10:00:00.100Z",
        "processed_at": "2026-11-09T10:00:00.150Z",
    }


ANOMALY = {"sensor_id": "CAP_001", "timestamp": "2026-11-09T10:00:00Z", "source": "simulator",
           "category": "pollution", "anomaly_type": "seuil_oms", "parameter": "pm2_5",
           "value": 100.0, "threshold": 75.0, "severity": "alerte", "detail": "test",
           "injected_anomaly": "spike"}


def count(conn, table):
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0]


def test_conversion_en_lignes():
    row = processed_to_row(processed(), partition=1, offset=7)
    assert len(row) == len(MEASUREMENT_COLUMNS)
    assert dict(zip(MEASUREMENT_COLUMNS, row))["pm2_5_mean_15min"] == 10.5
    assert len(anomaly_to_row(ANOMALY)) == len(ANOMALY_COLUMNS)


def test_lot_pas_ecrit_avant_d_etre_plein():
    storage = Storage(conn=None, batch_size=10, max_wait_s=999)
    storage.add(processed(), [ANOMALY], 0, 1)
    assert storage.pending == 2
    assert storage.flush_if_due() is False   # ni plein ni trop ancien : rien n'est écrit


def test_ecriture_par_lot(db_conn):
    storage = Storage(db_conn, batch_size=2, max_wait_s=999)
    storage.add(processed(), [ANOMALY], 0, 1)
    assert storage.flush_if_due() is False
    storage.add(processed(ts="2026-11-09T10:00:01Z"), [], 0, 2)
    assert storage.flush_if_due() is True
    assert count(db_conn, "measurements") == 2
    assert count(db_conn, "anomalies") == 1


def test_pas_de_doublon_si_message_relu(db_conn):
    storage = Storage(db_conn)
    for _ in range(2):   # le même message Kafka (partition 0, offset 5) traité deux fois
        storage.add(processed(), [ANOMALY], 0, 5)
        storage.flush()
    assert count(db_conn, "measurements") == 1
    assert count(db_conn, "anomalies") == 1


def test_agregats_quotidiens(db_conn):
    storage = Storage(db_conn)
    storage.add(processed(pm25=10.0), [ANOMALY], 0, 1)
    storage.add(processed(ts="2026-11-09T12:00:00Z", pm25=30.0, is_valid=False), [], 0, 2)
    storage.add(processed(ts="2026-11-10T01:00:00Z", pm25=99.0), [], 0, 3)   # autre jour
    storage.flush()

    assert aggregate_day(db_conn, date(2026, 11, 9)) == 1
    assert aggregate_day(db_conn, date(2026, 11, 9)) == 1   # relancer ne crée pas de doublon
    with db_conn.cursor() as cur:
        cur.execute("SELECT measurement_count, valid_count, completeness, pm2_5_avg, "
                    "pm2_5_max, anomaly_count FROM daily_aggregates")
        rows = cur.fetchall()
    assert rows == [(2, 1, 0.5, 20.0, 30.0, 1)]
