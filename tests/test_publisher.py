"""Tests de la publication (AQ-19), sans Kafka."""
import json

from streaming.publisher import build_processed, kafka_ts_to_iso, publish

MESSAGE = {"sensor_id": "CAP_001", "timestamp": "2026-11-09T10:00:00Z",
           "measurements": {"pm2_5": 12.0}, "source": "simulator"}
ANOMALY = {"sensor_id": "CAP_001", "anomaly_type": "seuil_oms", "parameter": "pm2_5"}


class FakeProducer:
    def __init__(self):
        self.calls = []

    def produce(self, topic, key=None, value=None, callback=None):
        self.calls.append((topic, key, json.loads(value)))

    def poll(self, timeout):
        return 0


def test_kafka_timestamp_converti():
    assert kafka_ts_to_iso(1_762_682_400_123) == "2025-11-09T10:00:00.123Z"
    assert kafka_ts_to_iso(None) is None
    assert kafka_ts_to_iso(-1) is None


def test_message_enrichi():
    issues = [{"field": "no2", "kind": "missing", "value": None, "detail": "mesure absente"}]
    processed = build_processed(MESSAGE, issues, {"pm2_5": {"mean_5min": 12.0}}, [ANOMALY],
                                kafka_ts_ms=1_762_682_400_123)
    assert processed["quality"] == {"is_valid": False, "issues": issues}
    assert processed["windows"]["pm2_5"]["mean_5min"] == 12.0
    assert processed["anomaly_count"] == 1
    assert processed["processed_at"].endswith("Z")
    assert "quality" not in MESSAGE  # le message d'origine n'est pas modifié


def test_publication_dans_les_bons_topics():
    fake = FakeProducer()
    processed = build_processed(MESSAGE, [], {}, [ANOMALY, ANOMALY])
    publish(fake, processed, [ANOMALY, ANOMALY])
    assert [topic for topic, _, _ in fake.calls] == ["processed_data", "anomalies", "anomalies"]
    assert all(key == b"CAP_001" for _, key, _ in fake.calls)


def test_message_rejete_seulement_anomalie():
    fake = FakeProducer()
    publish(fake, None, [ANOMALY])
    assert [topic for topic, _, _ in fake.calls] == ["anomalies"]
