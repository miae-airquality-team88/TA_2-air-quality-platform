"""Tests du script de création des topics (AQ-10), sans Kafka."""
from concurrent.futures import Future
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from confluent_kafka import KafkaError, KafkaException

from scripts import create_topics
from scripts.create_topics import build_topics


def test_trois_topics_definis():
    noms = [t.topic for t in build_topics()]
    assert noms == ["raw_data", "processed_data", "anomalies"]


def test_partitions():
    partitions = {t.topic: t.num_partitions for t in build_topics()}
    assert partitions == {"raw_data": 3, "processed_data": 3, "anomalies": 1}


def test_retention_et_replication():
    topics = build_topics()
    assert [t.config["retention.ms"] for t in topics] == [
        "604800000", "604800000", "2592000000",
    ]
    assert all(t.replication_factor == 1 for t in topics)


def admin_simule(monkeypatch, existing=(), error=None):
    admin = Mock()
    admin.list_topics.return_value = SimpleNamespace(topics=dict.fromkeys(existing))

    def create(topics, **kwargs):
        futures = {}
        for topic in topics:
            future = Future()
            if error and topic.topic == "raw_data":
                future.set_exception(error)
            else:
                future.set_result(None)
            futures[topic.topic] = future
        return futures

    admin.create_topics.side_effect = create
    monkeypatch.setattr(create_topics, "AdminClient", Mock(return_value=admin))
    return admin


def test_creation_puis_relance(monkeypatch):
    admin = admin_simule(monkeypatch)
    create_topics.main()
    topics = admin.create_topics.call_args.args[0]
    assert [t.topic for t in topics] == ["raw_data", "processed_data", "anomalies"]
    admin.list_topics.return_value = SimpleNamespace(topics={t.topic: None for t in topics})
    create_topics.main()
    assert admin.create_topics.call_count == 1


def test_creation_des_topics_manquants_seulement(monkeypatch):
    admin = admin_simule(monkeypatch, existing=["raw_data", "unrelated"])
    create_topics.main()
    assert [t.topic for t in admin.create_topics.call_args.args[0]] == [
        "processed_data", "anomalies",
    ]


def test_creation_concurrente_acceptee(monkeypatch):
    admin_simule(monkeypatch, error=KafkaException(KafkaError(KafkaError.TOPIC_ALREADY_EXISTS)))
    create_topics.main()


@pytest.mark.parametrize("error", [
    KafkaException(KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED)),
    TimeoutError("Kafka ne répond pas"),
])
def test_echec_signale_sans_masquer_les_autres_resultats(monkeypatch, capsys, error):
    admin_simule(monkeypatch, error=error)
    with pytest.raises(SystemExit, match="1 topic"):
        create_topics.main()
    assert "processed_data : créé" in capsys.readouterr().out


def test_broker_indisponible(monkeypatch):
    admin = admin_simule(monkeypatch)
    admin.list_topics.side_effect = KafkaException(KafkaError(KafkaError._TRANSPORT))
    with pytest.raises(KafkaException):
        create_topics.main()
    admin.create_topics.assert_not_called()
