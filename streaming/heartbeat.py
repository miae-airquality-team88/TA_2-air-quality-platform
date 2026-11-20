"""Signal d'activité (heartbeat) du consumer streaming (AQ-34).

Toutes les 30 secondes, le consumer écrit dans la table pipeline_heartbeats :
  - qu'il est vivant (status, last_seen) ;
  - combien de messages il a lus ;
  - son RETARD (lag) : nombre de messages encore à lire dans Kafka ;
  - le nombre de lignes encore en attente d'écriture en base ;
  - l'horodatage de la dernière mesure traitée.

Airflow lit ce signal (orchestration.sensors.is_streaming_done) pour savoir
si le traitement en streaming d'une journée est terminé avant de lancer le DAG.
"""
from confluent_kafka import OFFSET_INVALID

COMPONENT = "aq-streaming"

UPSERT_SQL = """
INSERT INTO pipeline_heartbeats
    (component, status, last_seen, messages_read, consumer_lag, pending_rows, last_measured_at)
VALUES (%(component)s, %(status)s, clock_timestamp(), %(messages_read)s, %(consumer_lag)s,
        %(pending_rows)s, %(last_measured_at)s)
ON CONFLICT (component) DO UPDATE SET
    status = EXCLUDED.status,
    last_seen = EXCLUDED.last_seen,
    messages_read = EXCLUDED.messages_read,
    consumer_lag = EXCLUDED.consumer_lag,
    pending_rows = EXCLUDED.pending_rows,
    last_measured_at = COALESCE(EXCLUDED.last_measured_at, pipeline_heartbeats.last_measured_at)
"""


def compute_lag(consumer, timeout=5):
    """Retard du consumer : somme, sur ses partitions, de (dernier offset - position).

    Exemple : partition 1 -> fin du topic à 1 200, position du consumer à 1 150
    -> 50 messages de retard. Renvoie None si aucune partition n'est encore assignée.
    """
    partitions = consumer.assignment()
    if not partitions:
        return None
    lag = 0
    for tp in consumer.position(partitions):
        low, high = consumer.get_watermark_offsets(tp, timeout=timeout, cached=False)
        position = low if tp.offset == OFFSET_INVALID or tp.offset < 0 else tp.offset
        lag += max(high - position, 0)
    return lag


def send_heartbeat(conn, status, messages_read, consumer_lag, pending_rows,
                   last_measured_at=None, component=COMPONENT):
    """Écrit (ou met à jour) le signal d'activité du composant."""
    with conn.cursor() as cur:
        cur.execute(UPSERT_SQL, {
            "component": component, "status": status, "messages_read": messages_read,
            "consumer_lag": consumer_lag, "pending_rows": pending_rows,
            "last_measured_at": last_measured_at,
        })
    conn.commit()
