"""Tests du pipeline streaming complet (AQ-18), sans Kafka.

On fabrique des messages avec le simulateur, on les passe dans le pipeline
comme s'ils venaient de Kafka (bytes), et on vérifie le résultat.
"""
import json

from ingestion.simulator import stream
from streaming.consumer import StreamPipeline
from streaming.storage import Storage


def as_bytes(message):
    return json.dumps(message).encode("utf-8")


def simulate(count=40, anomaly_rate=0.2, seed=11):
    return list(stream(count=count, interval=0, anomaly_rate=anomaly_rate, seed=seed))


def test_message_normal():
    pipeline = StreamPipeline()
    processed, anomalies = pipeline.process(as_bytes(simulate(1, anomaly_rate=0)[0]),
                                            kafka_ts_ms=1_762_682_400_000)
    assert anomalies == []
    assert processed["quality"]["is_valid"] is True
    assert processed["windows"]["pm2_5"]["count_15min"] == 1
    assert processed["kafka_timestamp"] == "2025-11-09T10:00:00.000Z"


def test_json_invalide_rejete():
    processed, anomalies = StreamPipeline().process(b"{pas du json")
    assert processed is None
    assert anomalies[0]["anomaly_type"] == "json_invalide"
    assert anomalies[0]["sensor_id"] == "INCONNU"


def test_structure_invalide_rejetee():
    processed, anomalies = StreamPipeline().process(as_bytes({"sensor_id": "CAP_001"}))
    assert processed is None
    assert anomalies[0]["anomaly_type"] == "structure_invalide"
    assert anomalies[0]["sensor_id"] == "CAP_001"


def test_flux_simule_et_taux_de_detection():
    pipeline = StreamPipeline()
    for message in simulate(count=60, anomaly_rate=0.2):
        processed, _ = pipeline.process(as_bytes(message))
        assert processed is not None
    stats = pipeline.stats
    assert stats["traites"] > 0 and stats["rejetes"] == 0
    assert stats["injectees"] > 0
    # Les règles doivent retrouver la grande majorité des anomalies injectées
    assert stats["injectees_detectees"] / stats["injectees"] >= 0.8, pipeline.summary()


def test_de_bout_en_bout_jusqu_a_postgresql(db_conn):
    pipeline = StreamPipeline()
    storage = Storage(db_conn, batch_size=1000)
    for offset, message in enumerate(simulate(count=10, anomaly_rate=0.3)):
        processed, anomalies = pipeline.process(as_bytes(message), kafka_ts_ms=1_762_682_400_000)
        storage.add(processed, anomalies, partition=0, offset=offset)
    storage.flush()
    with db_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM measurements")
        assert cur.fetchone()[0] == 50
        cur.execute("SELECT COUNT(*) FROM anomalies")
        assert cur.fetchone()[0] == pipeline.stats["anomalies"]
