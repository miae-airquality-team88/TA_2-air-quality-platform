"""Publication des résultats du streaming dans Kafka (AQ-19).

- processed_data : chaque message nettoyé, enrichi (qualité, fenêtres glissantes,
  horodatages de traitement) ;
- anomalies      : chaque anomalie détectée, un message par anomalie.

L'envoi réutilise send_message() du producer (AQ-11) : même clé (sensor_id),
même garantie de livraison (acks=all).

Format d'un message processed_data :
    { ...champs du contrat (mesures nettoyées)...,
      "quality": {"is_valid": bool, "issues": [...]},
      "windows": {"pm2_5": {"mean_5min": ..., "mean_15min": ..., ...}, "pm10": {...}},
      "anomaly_count": 2,
      "kafka_timestamp": "2026-11-09T10:00:00.123Z",   # écriture dans raw_data
      "processed_at": "2026-11-09T10:00:00.180Z" }      # fin du traitement
"""
from datetime import datetime, timezone

from common.config import TOPIC_ANOMALIES, TOPIC_PROCESSED
from ingestion.producer import send_message


def iso_ms(dt):
    """datetime -> '2026-11-09T10:00:00.123Z' (UTC, précision milliseconde)."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def kafka_ts_to_iso(kafka_ts_ms):
    """Horodatage Kafka (millisecondes depuis 1970) -> texte ISO, ou None."""
    if not kafka_ts_ms or kafka_ts_ms <= 0:
        return None
    return iso_ms(datetime.fromtimestamp(kafka_ts_ms / 1000, tz=timezone.utc))


def build_processed(message, issues, window_stats, anomalies, kafka_ts_ms=None):
    """Assemble le message enrichi publié dans processed_data."""
    processed = dict(message)
    processed["quality"] = {"is_valid": not issues, "issues": issues}
    processed["windows"] = window_stats
    processed["anomaly_count"] = len(anomalies)
    processed["kafka_timestamp"] = kafka_ts_to_iso(kafka_ts_ms)
    processed["processed_at"] = iso_ms(datetime.now(timezone.utc))
    return processed


def publish(producer, processed, anomalies):
    """Envoie le message enrichi (s'il existe) et chaque anomalie."""
    if processed is not None:
        send_message(producer, processed, topic=TOPIC_PROCESSED)
    for anomaly in anomalies:
        send_message(producer, anomaly, topic=TOPIC_ANOMALIES)
