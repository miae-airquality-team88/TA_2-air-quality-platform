"""Tests du signal d'activité du consumer (AQ-34)."""
from datetime import datetime, timezone

from confluent_kafka import OFFSET_INVALID, TopicPartition

from streaming.heartbeat import compute_lag, send_heartbeat


class FakeConsumer:
    """Imite un consumer Kafka : 3 partitions, avec fin de topic et position connues."""

    def __init__(self, positions, highs, assigned=True):
        self.positions, self.highs, self.assigned = positions, highs, assigned

    def assignment(self):
        return [TopicPartition("raw_data", p) for p in self.positions] if self.assigned else []

    def position(self, partitions):
        return [TopicPartition("raw_data", tp.partition, self.positions[tp.partition])
                for tp in partitions]

    def get_watermark_offsets(self, tp, timeout=None, cached=False):
        return 0, self.highs[tp.partition]


def test_lag_exemple_chiffre():
    # partition 0 : à jour ; partition 1 : 50 de retard ; partition 2 : rien lu sur 30 messages
    consumer = FakeConsumer({0: 100, 1: 1150, 2: OFFSET_INVALID}, {0: 100, 1: 1200, 2: 30})
    assert compute_lag(consumer) == 80


def test_lag_inconnu_sans_partition():
    assert compute_lag(FakeConsumer({}, {}, assigned=False)) is None


def test_heartbeat_mis_a_jour(db_conn):
    measured = datetime(2026, 11, 16, 12, tzinfo=timezone.utc)
    send_heartbeat(db_conn, "running", 100, 20, 5, measured)
    send_heartbeat(db_conn, "stopped", 250, 0, 0)        # 2e signal : remplace le 1er
    with db_conn.cursor() as cur:
        cur.execute("SELECT status, messages_read, consumer_lag, pending_rows, last_measured_at "
                    "FROM pipeline_heartbeats")
        rows = cur.fetchall()
    assert rows == [("stopped", 250, 0, 0, measured)]   # last_measured_at conservé
