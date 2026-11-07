"""Vérification réelle, activée par AQ10_KAFKA_INTEGRATION=1 sur un Kafka de test."""
import os

import pytest
from confluent_kafka.admin import AdminClient, ConfigResource, ResourceType

from common.config import KAFKA_BOOTSTRAP_SERVERS
from scripts.create_topics import build_topics, main


@pytest.mark.skipif(
    os.getenv("AQ10_KAFKA_INTEGRATION") != "1",
    reason="Nécessite un broker Kafka de test et AQ10_KAFKA_INTEGRATION=1",
)
def test_creation_relance_et_configuration_reelles():
    main()
    main()
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    metadata = admin.list_topics(timeout=10)
    for expected in build_topics():
        actual = metadata.topics[expected.topic]
        assert actual.error is None
        assert len(actual.partitions) == expected.num_partitions
        assert all(len(p.replicas) == 1 for p in actual.partitions.values())
        resource = ConfigResource(ResourceType.TOPIC, expected.topic)
        configs = admin.describe_configs([resource], request_timeout=10)
        values = configs[resource].result(timeout=15)
        assert values["retention.ms"].value == expected.config["retention.ms"]
