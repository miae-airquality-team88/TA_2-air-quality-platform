"""Création des topics Kafka du projet (AQ-10).

Le docker-compose désactive la création automatique des topics : ils sont créés
ici, par un script versionné et rejouable (« idempotent ») : le relancer ne
provoque pas d'erreur, les topics existants sont simplement ignorés.

Usage (depuis la racine du dépôt, Kafka démarré) :
    python -m scripts.create_topics
"""
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from common.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_ANOMALIES, TOPIC_PROCESSED, TOPIC_RAW

DAY_MS = 24 * 60 * 60 * 1000

# (nom, nombre de partitions, rétention en jours)
TOPICS = [
    (TOPIC_RAW, 3, 7),         # messages bruts : simulateur + OpenAQ
    (TOPIC_PROCESSED, 3, 7),   # messages nettoyés (Sprint 2)
    (TOPIC_ANOMALIES, 1, 30),  # anomalies détectées (Sprint 2), gardées plus longtemps
]


def build_topics():
    """Transforme la liste TOPICS en objets NewTopic compris par Kafka."""
    return [
        NewTopic(
            name,
            num_partitions=partitions,
            replication_factor=1,  # un seul broker en local
            config={"retention.ms": str(days * DAY_MS)},
        )
        for name, partitions, days in TOPICS
    ]


def main():
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    print(f"Connexion à Kafka : {KAFKA_BOOTSTRAP_SERVERS}")
    existing = set(admin.list_topics(timeout=10).topics)

    to_create = []
    for topic in build_topics():
        if topic.topic in existing:
            print(f"  - {topic.topic} : déjà présent, ignoré")
        else:
            to_create.append(topic)

    if not to_create:
        print("Tous les topics existent déjà.")
        return

    futures = admin.create_topics(to_create, operation_timeout=10, request_timeout=15)
    errors = 0
    for name, future in futures.items():
        try:
            future.result(timeout=20)
            print(f"  + {name} : créé")
        except KafkaException as exc:
            # Un autre processus peut créer le topic après la lecture des métadonnées.
            if exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print(f"  - {name} : déjà présent, ignoré")
            else:
                errors += 1
                print(f"  ! {name} : ÉCHEC ({exc})")
        except Exception as exc:  # on affiche l'erreur et on continue avec les autres topics
            errors += 1
            print(f"  ! {name} : ÉCHEC ({exc})")

    if errors:
        raise SystemExit(f"{errors} topic(s) n'ont pas pu être créés.")
    print("Terminé.")


if __name__ == "__main__":
    main()
