"""Producer Kafka : publie les messages capteurs dans le topic raw_data (AQ-11).

Ce module fournit deux fonctions réutilisables par toutes les sources de données
(simulateur ici, replay OpenAQ dans AQ-16) :
    build_producer()                 -> crée la connexion à Kafka
    send_message(producer, message)  -> envoie un message (dict) au format JSON

Usage (depuis la racine du dépôt, Kafka démarré et topics créés) :
    python -m ingestion.producer --count 10
    python -m ingestion.producer --sensors 5 --interval 1 --anomaly-rate 0.05
"""
import argparse
import json
import sys

from confluent_kafka import Producer

from common.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_RAW
from ingestion.simulator import make_sensors, stream

# Compteurs mis à jour par delivery_report
stats = {"sent": 0, "failed": 0}


def build_producer(client_id="aq-producer"):
    """Crée un producer Kafka configuré pour le projet."""
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": client_id,
        "acks": "all",       # Kafka confirme chaque écriture : pas de perte silencieuse
        "linger.ms": 50,     # regroupe les messages par petits lots (plus efficace)
    })


def delivery_report(err, msg):
    """Appelée par Kafka pour chaque message : confirmation ou erreur."""
    if err is not None:
        stats["failed"] += 1
        print(f"Échec d'envoi : {err}", file=sys.stderr)
    else:
        stats["sent"] += 1


def send_message(producer, message, topic=TOPIC_RAW):
    """Envoie un message (dict) en JSON, avec sensor_id comme clé.

    La clé garantit que tous les messages d'un même capteur vont dans la même
    partition du topic, donc restent dans l'ordre chronologique.
    """
    key = message["sensor_id"].encode("utf-8")
    value = json.dumps(message, ensure_ascii=False).encode("utf-8")
    try:
        producer.produce(topic, key=key, value=value, callback=delivery_report)
    except BufferError:
        # File d'attente locale pleine : on laisse Kafka vider la file puis on réessaie
        producer.poll(1)
        producer.produce(topic, key=key, value=value, callback=delivery_report)
    producer.poll(0)  # déclenche les callbacks des messages déjà confirmés


def main():
    parser = argparse.ArgumentParser(description="Simulateur -> Kafka (topic raw_data)")
    parser.add_argument("--sensors", type=int, default=5)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--anomaly-rate", type=float, default=0.05)
    parser.add_argument("--count", type=int, default=None, help="nombre de tours (infini sinon)")
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    producer = build_producer()
    print(f"Envoi vers {KAFKA_BOOTSTRAP_SERVERS}, topic {TOPIC_RAW} (Ctrl+C pour arrêter)")
    total = 0
    try:
        for message in stream(
    count=args.count,
    interval=args.interval,
    anomaly_rate=args.anomaly_rate,
    seed=args.seed,
    sensors=make_sensors(args.sensors),
):
            send_message(producer, message)
            total += 1
            if total % 50 == 0:
                print(f"{total} messages produits | confirmés : {stats['sent']} | "
                      f"échecs : {stats['failed']}")
    except KeyboardInterrupt:
        print("Arrêt demandé…")
    finally:
        producer.flush(10)  # attend que les derniers messages soient confirmés
        print(f"Terminé : {total} messages produits, {stats['sent']} confirmés, "
              f"{stats['failed']} échecs.")


if __name__ == "__main__":
    main()
