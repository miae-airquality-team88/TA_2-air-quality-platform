"""Stockage du flux dans PostgreSQL (AQ-22).

Deux usages :

1. TEMPS RÉEL (appelé par le consumer, AQ-18) : chaque message traité et ses
   anomalies sont mis en attente puis écrits PAR LOTS (moins d'allers-retours
   avec la base). Un lot part dès qu'il contient `batch_size` messages ou que
   `max_wait_s` secondes se sont écoulées.

2. BATCH QUOTIDIEN : aggregate_day() calcule les agrégats d'une journée
   (moyennes, maximums, complétude, nombre d'anomalies) dans daily_aggregates.
   Relancer le calcul remplace les valeurs (idempotent) : Airflow pourra
   l'appeler chaque jour et pour le backfill (Phase 2).

Idempotence : la paire (kafka_partition, kafka_offset) identifie un message
Kafka de façon unique. Si un message est relu après un incident, l'insertion
est ignorée (ON CONFLICT DO NOTHING) : pas de doublon.

Usage en ligne de commande (batch quotidien) :
    python -m streaming.storage --aggregate-day 2026-11-09
    python -m streaming.storage --aggregate-day today
"""
import argparse
import time
from datetime import date, datetime, timezone

from psycopg2.extras import execute_values

from common.db import get_connection

MEASUREMENT_COLUMNS = [
    "sensor_id", "source", "measured_at", "lat", "lon",
    "pm2_5", "pm10", "no2", "o3", "temperature", "humidity",
    "pm2_5_mean_5min", "pm2_5_mean_15min", "pm10_mean_5min", "pm10_mean_15min",
    "is_valid", "issue_count", "anomaly_count", "injected_anomaly",
    "kafka_partition", "kafka_offset", "kafka_timestamp", "processed_at",
]
ANOMALY_COLUMNS = [
    "sensor_id", "source", "measured_at", "category", "anomaly_type", "parameter",
    "value", "threshold", "severity", "detail", "injected_anomaly",
    "kafka_partition", "kafka_offset",
]


def processed_to_row(processed, partition=None, offset=None):
    """Message de processed_data (dict) -> tuple dans l'ordre de MEASUREMENT_COLUMNS."""
    m = processed["measurements"]
    windows = processed.get("windows") or {}
    pm25 = windows.get("pm2_5") or {}
    pm10 = windows.get("pm10") or {}
    quality = processed.get("quality") or {}
    return (
        processed["sensor_id"], processed["source"], processed["timestamp"],
        processed["location"]["lat"], processed["location"]["lon"],
        m.get("pm2_5"), m.get("pm10"), m.get("no2"), m.get("o3"),
        m.get("temperature"), m.get("humidity"),
        pm25.get("mean_5min"), pm25.get("mean_15min"),
        pm10.get("mean_5min"), pm10.get("mean_15min"),
        quality.get("is_valid", True), len(quality.get("issues", [])),
        processed.get("anomaly_count", 0), processed.get("injected_anomaly"),
        partition, offset, processed.get("kafka_timestamp"), processed.get("processed_at"),
    )


def anomaly_to_row(anomaly, partition=None, offset=None):
    """Anomalie (dict) -> tuple dans l'ordre de ANOMALY_COLUMNS."""
    return (
        anomaly.get("sensor_id") or "INCONNU", anomaly.get("source"), anomaly.get("timestamp"),
        anomaly["category"], anomaly["anomaly_type"], anomaly.get("parameter"),
        anomaly.get("value"), anomaly.get("threshold"), anomaly["severity"],
        anomaly.get("detail"), anomaly.get("injected_anomaly"),
        partition, offset,
    )


class Storage:
    """Écriture par lots des mesures et des anomalies."""

    def __init__(self, conn, batch_size=50, max_wait_s=2.0):
        self.conn = conn
        self.batch_size = batch_size
        self.max_wait_s = max_wait_s
        self._measurements = []
        self._anomalies = []
        self._last_flush = time.monotonic()
        self.total = {"measurements": 0, "anomalies": 0}

    @property
    def pending(self):
        return len(self._measurements) + len(self._anomalies)

    def add(self, processed, anomalies, partition=None, offset=None):
        """Met en attente un message traité (peut être None s'il a été rejeté) et ses anomalies."""
        if processed is not None:
            self._measurements.append(processed_to_row(processed, partition, offset))
        self._anomalies.extend(anomaly_to_row(a, partition, offset) for a in anomalies)

    def flush_if_due(self):
        """Écrit le lot s'il est plein ou trop ancien. Renvoie True si une écriture a eu lieu."""
        too_old = time.monotonic() - self._last_flush >= self.max_wait_s
        if self.pending and (len(self._measurements) >= self.batch_size or too_old):
            self.flush()
            return True
        return False

    def flush(self):
        """Écrit tout ce qui est en attente, dans une seule transaction."""
        if self.pending:
            try:
                with self.conn.cursor() as cur:
                    if self._measurements:
                        execute_values(
                            cur,
                            f"INSERT INTO measurements ({', '.join(MEASUREMENT_COLUMNS)}) "
                            "VALUES %s ON CONFLICT DO NOTHING",
                            self._measurements,
                        )
                    if self._anomalies:
                        execute_values(
                            cur,
                            f"INSERT INTO anomalies ({', '.join(ANOMALY_COLUMNS)}) "
                            "VALUES %s ON CONFLICT DO NOTHING",
                            self._anomalies,
                        )
                self.conn.commit()
            except Exception:
                self.conn.rollback()   # rien n'est écrit : le lot sera relu depuis Kafka
                raise
            self.total["measurements"] += len(self._measurements)
            self.total["anomalies"] += len(self._anomalies)
            self._measurements.clear()
            self._anomalies.clear()
        self._last_flush = time.monotonic()


AGGREGATE_SQL = """
INSERT INTO daily_aggregates (
    day, sensor_id, source, measurement_count, valid_count, completeness,
    pm2_5_avg, pm2_5_max, pm10_avg, pm10_max, no2_avg, no2_max, o3_avg, o3_max,
    temperature_avg, humidity_avg, anomaly_count, computed_at
)
SELECT
    %(day)s::date, m.sensor_id, MIN(m.source),
    COUNT(*), COUNT(*) FILTER (WHERE m.is_valid),
    ROUND((COUNT(*) FILTER (WHERE m.is_valid))::numeric / COUNT(*), 4),
    AVG(m.pm2_5), MAX(m.pm2_5), AVG(m.pm10), MAX(m.pm10),
    AVG(m.no2), MAX(m.no2), AVG(m.o3), MAX(m.o3),
    AVG(m.temperature), AVG(m.humidity),
    COALESCE(MAX(a.nb), 0),
    clock_timestamp()
FROM measurements m
LEFT JOIN (
    SELECT sensor_id, COUNT(*) AS nb
    FROM anomalies
    WHERE measured_at >= %(day)s::date AND measured_at < %(day)s::date + 1
    GROUP BY sensor_id
) a ON a.sensor_id = m.sensor_id
WHERE m.measured_at >= %(day)s::date AND m.measured_at < %(day)s::date + 1
GROUP BY m.sensor_id
ON CONFLICT (day, sensor_id) DO UPDATE SET
    source = EXCLUDED.source,
    measurement_count = EXCLUDED.measurement_count,
    valid_count = EXCLUDED.valid_count,
    completeness = EXCLUDED.completeness,
    pm2_5_avg = EXCLUDED.pm2_5_avg, pm2_5_max = EXCLUDED.pm2_5_max,
    pm10_avg = EXCLUDED.pm10_avg, pm10_max = EXCLUDED.pm10_max,
    no2_avg = EXCLUDED.no2_avg, no2_max = EXCLUDED.no2_max,
    o3_avg = EXCLUDED.o3_avg, o3_max = EXCLUDED.o3_max,
    temperature_avg = EXCLUDED.temperature_avg, humidity_avg = EXCLUDED.humidity_avg,
    anomaly_count = EXCLUDED.anomaly_count,
    computed_at = EXCLUDED.computed_at
"""


def aggregate_day(conn, day):
    """Calcule (ou recalcule) les agrégats d'une journée UTC. Renvoie le nombre de capteurs."""
    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")   # les journées sont découpées en UTC
        cur.execute(AGGREGATE_SQL, {"day": day})
        count = cur.rowcount
    conn.commit()
    return count


def parse_day(value):
    if value == "today":
        return datetime.now(timezone.utc).date()
    return date.fromisoformat(value)


def main():
    parser = argparse.ArgumentParser(description="Batch quotidien : agrégats par capteur")
    parser.add_argument("--aggregate-day", required=True, help="AAAA-MM-JJ ou 'today'")
    args = parser.parse_args()
    day = parse_day(args.aggregate_day)
    conn = get_connection()
    try:
        count = aggregate_day(conn, day)
        print(f"Agrégats du {day} : {count} capteur(s) calculé(s) dans daily_aggregates.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
