"""Consumer streaming : le cœur de la Phase 1 (AQ-18).

Lit chaque message du topic raw_data et enchaîne toutes les étapes du
traitement, écrites par l'équipe :

    raw_data ──► 1. décodage JSON
             ──► 2. validation + nettoyage           (AQ-13, streaming/validation.py)
             ──► 3. fenêtres glissantes 5/15 min      (AQ-20, streaming/windows.py)
             ──► 4. règles métier -> anomalies        (AQ-21, streaming/rules.py)
             ──► 5. publication processed_data + anomalies (AQ-19, streaming/publisher.py)
             ──► 6. écriture PostgreSQL par lots      (AQ-22, streaming/storage.py)
             ──► 7. validation des offsets Kafka (commit) APRÈS l'écriture en base

Garantie « au moins une fois » : les offsets ne sont validés qu'une fois les
données en base. Après un plantage, Kafka renvoie les messages non validés,
et la base ignore ceux déjà enregistrés (idempotence) : rien n'est perdu,
rien n'est en double.

Usage (depuis la racine du dépôt ; Kafka et PostgreSQL démarrés, tables créées) :
    python -m streaming.consumer
Arrêt propre : Ctrl+C.
"""
import argparse
import json
import time
from collections import Counter

from common.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_RAW
from streaming.publisher import build_processed
from streaming.rules import AnomalyDetector, make_anomaly
from streaming.validation import validate
from streaming.windows import SlidingWindows

GROUP_ID = "aq-streaming"

# Anomalie injectée par le simulateur -> type(s) d'anomalie qui la détectent
DETECTION_MAP = {
    "spike": {"seuil_oms", "pic"},
    "negative": {"negative"},
    "out_of_range": {"out_of_range"},
    "stuck": {"capteur_bloque"},
    "missing_values": {"missing"},
}


class StreamPipeline:
    """Traitement d'un message, SANS Kafka ni base de données (donc facile à tester)."""

    def __init__(self):
        self.windows = SlidingWindows()
        self.detector = AnomalyDetector()
        self.stats = Counter()

    def process(self, value, kafka_ts_ms=None):
        """Traite un message brut (bytes). Renvoie (message_enrichi ou None, anomalies)."""
        self.stats["lus"] += 1

        # 1. Décodage JSON
        try:
            raw = json.loads(value)
        except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
            self.stats["rejetes"] += 1
            return None, [self._rejected({}, "json_invalide", "message illisible (JSON invalide)")]

        # 2. Validation de la structure et du contenu
        result = validate(raw)
        if not result.accepted:
            self.stats["rejetes"] += 1
            detail = "; ".join(f"{i['field']}: {i['detail']}" for i in result.issues[:5])
            return None, [self._rejected(raw if isinstance(raw, dict) else {},
                                         "structure_invalide", detail)]
        message = result.message
        sensor_id = message["sensor_id"]

        # 3. Fenêtres : état AVANT ce message (pour les règles), puis mise à jour
        before = self.windows.stats(sensor_id)
        after = self.windows.update(sensor_id, message["timestamp"], message["measurements"])

        # 4. Règles métier
        anomalies = self.detector.detect(message, result.issues, before)

        # 5. Message enrichi pour processed_data
        processed = build_processed(message, result.issues, after, anomalies, kafka_ts_ms)

        self._count(message, result, anomalies)
        return processed, anomalies

    @staticmethod
    def _rejected(raw, anomaly_type, detail):
        message = {"sensor_id": str(raw.get("sensor_id") or "INCONNU"),
                   "timestamp": raw.get("timestamp") if isinstance(raw.get("timestamp"), str)
                   else None,
                   "source": raw.get("source") if raw.get("source") in ("simulator", "openaq")
                   else None,
                   "injected_anomaly": None}
        return make_anomaly(message, "message", anomaly_type, "alerte", detail)

    def _count(self, message, result, anomalies):
        """Statistiques, dont le taux de détection des anomalies injectées (vérité terrain)."""
        self.stats["traites"] += 1
        self.stats["valides" if result.is_clean else "avec_problemes"] += 1
        self.stats["anomalies"] += len(anomalies)
        injected = message.get("injected_anomaly")
        if injected:
            self.stats["injectees"] += 1
            if DETECTION_MAP.get(injected, set()) & {a["anomaly_type"] for a in anomalies}:
                self.stats["injectees_detectees"] += 1

    def summary(self):
        s = self.stats
        rate = (f"{100 * s['injectees_detectees'] / s['injectees']:.1f} %"
                if s["injectees"] else "-")
        return (f"lus={s['lus']} traités={s['traites']} rejetés={s['rejetes']} "
                f"valides={s['valides']} anomalies={s['anomalies']} "
                f"| anomalies injectées détectées : {s['injectees_detectees']}/{s['injectees']} "
                f"({rate})")


def build_consumer():
    from confluent_kafka import Consumer
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",   # nouveau groupe : lire depuis le début du topic
        "enable.auto.commit": False,       # on valide nous-mêmes, après l'écriture en base
    })


def run(batch_size=50, max_wait_s=2.0, report_every=100):
    from common.db import get_connection
    from ingestion.producer import build_producer
    from streaming.publisher import publish
    from streaming.storage import Storage

    consumer = build_consumer()
    producer = build_producer(client_id="aq-streaming")
    conn = get_connection()
    storage = Storage(conn, batch_size=batch_size, max_wait_s=max_wait_s)
    pipeline = StreamPipeline()

    consumer.subscribe([TOPIC_RAW])
    print(f"Consumer '{GROUP_ID}' à l'écoute de {TOPIC_RAW} ({KAFKA_BOOTSTRAP_SERVERS}). "
          "Ctrl+C pour arrêter.")
    last_report, last_count = time.monotonic(), 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                pass
            elif msg.error():
                print(f"Erreur Kafka : {msg.error()}")
            else:
                processed, anomalies = pipeline.process(msg.value(), msg.timestamp()[1])
                publish(producer, processed, anomalies)
                storage.add(processed, anomalies, msg.partition(), msg.offset())

            if storage.flush_if_due():
                producer.flush(5)
                consumer.commit(asynchronous=False)

            read = pipeline.stats["lus"]
            if read != last_count and (read - last_count >= report_every
                                       or time.monotonic() - last_report > 30):
                print(pipeline.summary())
                last_report, last_count = time.monotonic(), read
    except KeyboardInterrupt:
        print("Arrêt demandé…")
    finally:
        storage.flush()
        producer.flush(10)
        try:
            consumer.commit(asynchronous=False)
        except Exception:   # rien à valider (aucun message lu)
            pass
        consumer.close()
        conn.close()
        print("Bilan :", pipeline.summary())
        print(f"Écrit en base : {storage.total['measurements']} mesures, "
              f"{storage.total['anomalies']} anomalies.")


def main():
    parser = argparse.ArgumentParser(description="Consumer streaming raw_data -> PostgreSQL")
    parser.add_argument("--batch-size", type=int, default=50, help="taille des lots en base")
    parser.add_argument("--max-wait", type=float, default=2.0, help="secondes max avant écriture")
    args = parser.parse_args()
    run(batch_size=args.batch_size, max_wait_s=args.max_wait)


if __name__ == "__main__":
    main()
