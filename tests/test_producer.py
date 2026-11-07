
"""Tests du producer (AQ-11), sans Kafka : on utilise un faux producer."""

import json

from common.contract import is_valid
from ingestion.producer import send_message
from ingestion.simulator import make_sensors, stream


class FakeProducer:
    """Imite un Producer Kafka et enregistre les messages envoyes."""

    def __init__(self):
        self.calls = []

    def produce(self, topic, key=None, value=None, callback=None):
        self.calls.append((topic, key, value))

    def poll(self, timeout):
        return 0


def test_cle_topic_et_contenu():
    fake = FakeProducer()

    message = next(
        stream(
            sensors=make_sensors(1),
            interval=0,
            count=1,
            seed=1,
        )
    )

    send_message(fake, message)

    topic, key, value = fake.calls[0]

    assert topic == "raw_data"
    assert key == b"CAP_001"
    assert json.loads(value) == message


def test_messages_envoyes_conformes_au_contrat():
    fake = FakeProducer()

    for message in stream(
        sensors=make_sensors(3),
        interval=0,
        count=10,
        anomaly_rate=0.2,
        seed=3,
    ):
        send_message(fake, message)

    assert len(fake.calls) > 0
    assert all(
        is_valid(json.loads(value))
        for _, _, value in fake.calls
    )
