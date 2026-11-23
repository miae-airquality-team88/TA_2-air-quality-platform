"""Charge des mesures simulées historiques pour le backfill (AQ-36)."""
import argparse
import json
import random
from contextlib import closing, contextmanager
from datetime import date, datetime, time, timedelta, timezone

from common.config import BACKFILL_END_DATE, BACKFILL_START_DATE
from common.db import get_connection
from ingestion.simulator import make_sensors, simulate_measurement
from streaming.consumer import StreamPipeline
from streaming.storage import Storage

HISTORY_PARTITION = -1
MAX_DAYS = 30
MINUTES_PER_DAY = 24 * 60


def history_days(start, end, today=None):
    """Renvoie les journées UTC inclusives, limitées à la veille."""
    today = today or datetime.now(timezone.utc).date()
    if end < start:
        raise ValueError("La date de fin précède la date de début.")
    count = (min(end, today - timedelta(days=1)) - start).days + 1
    if count > MAX_DAYS:
        raise ValueError(f"La période ne peut pas dépasser {MAX_DAYS} jours.")
    return [start + timedelta(days=i) for i in range(count)]


def measures_per_day(n_sensors, step_minutes, anomaly_rate):
    """Valide les options et calcule le volume d'une journée."""
    if not 1 <= n_sensors <= 99:
        raise ValueError("Le nombre de capteurs doit être compris entre 1 et 99.")
    if not 1 <= step_minutes <= MINUTES_PER_DAY:
        raise ValueError("Le pas doit être compris entre 1 et 1 440 minutes.")
    if not 0 <= anomaly_rate <= 1:
        raise ValueError("Le taux d'anomalies doit être compris entre 0 et 1.")
    return n_sensors * len(range(0, MINUTES_PER_DAY, step_minutes))


def history_offset(timestamp, sensor_index):
    # Les offsets stables partagent l'espace de stockage existant, dans la partition -1.
    return int(timestamp.timestamp()) * 100 + sensor_index


def generate_messages(day, n_sensors=5, step_minutes=5, anomaly_rate=0.05, seed=42):
    """Génère les messages d'une journée. La graine et la date fixent son contenu."""
    measures_per_day(n_sensors, step_minutes, anomaly_rate)
    rng = random.Random(f"{seed}:{day.isoformat()}")
    sensors = make_sensors(n_sensors, random.Random(seed))
    midnight = datetime.combine(day, time.min, tzinfo=timezone.utc)
    for minute in range(0, MINUTES_PER_DAY, step_minutes):
        timestamp = midnight + timedelta(minutes=minute)
        for index, sensor in enumerate(sensors, start=1):
            message = simulate_measurement(sensor, timestamp, anomaly_rate, rng)
            yield message, history_offset(timestamp, index)


@contextmanager
def history_lock(conn):
    """Sérialise les chargements historiques, même entre les commits des lots."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_lock(36, 0)")
    try:
        yield
    finally:
        if not conn.closed:
            conn.rollback()
            with conn.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_unlock(36, 0)")
            conn.commit()


def existing_offsets(conn, day):
    midnight = datetime.combine(day, time.min, tzinfo=timezone.utc)
    tomorrow = midnight + timedelta(days=1)
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT kafka_offset FROM measurements "
            "WHERE kafka_partition = %s AND kafka_offset >= %s AND kafka_offset < %s",
            (HISTORY_PARTITION, history_offset(midnight, 0), history_offset(tomorrow, 0)),
        )
        return {row[0] for row in cursor}


def load_history(
    conn, days, n_sensors=5, step_minutes=5, anomaly_rate=0.05, seed=42, batch_size=2000,
):
    """Applique le pipeline par jour et valide les lots sur une connexion dédiée."""
    measures_per_day(n_sensors, step_minutes, anomaly_rate)
    if batch_size < 1:
        raise ValueError("La taille de lot doit être positive.")
    storage = Storage(conn, batch_size=batch_size, max_wait_s=3600)
    with history_lock(conn):
        for day in sorted(set(days)):
            # Chaque journée rejouée seule doit produire les mêmes mesures et fenêtres.
            pipeline = StreamPipeline()
            stored = existing_offsets(conn, day)
            for message, offset in generate_messages(
                day, n_sensors, step_minutes, anomaly_rate, seed,
            ):
                processed, anomalies = pipeline.process(json.dumps(message).encode("utf-8"))
                if processed is None:
                    raise ValueError(f"Le pipeline a rejeté le message simulé {offset}.")
                if offset in stored:
                    continue
                storage.add(processed, anomalies, HISTORY_PARTITION, offset)
                storage.flush_if_due()
            storage.flush()
    return storage.total


def main():
    parser = argparse.ArgumentParser(description="Charge des mesures simulées historiques")
    parser.add_argument("--days", type=int, help="Nombre de jours UTC terminés, au maximum 30")
    parser.add_argument("--start", type=date.fromisoformat, help="Premier jour inclus (AAAA-MM-JJ)")
    parser.add_argument("--end", type=date.fromisoformat, help="Dernier jour inclus (AAAA-MM-JJ)")
    parser.add_argument("--sensors", type=int, default=5)
    parser.add_argument("--step-minutes", type=int, default=5)
    parser.add_argument("--anomaly-rate", type=float, default=0.05)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    try:
        today = datetime.now(timezone.utc).date()
        if args.days is not None:
            if args.start or args.end or not 1 <= args.days <= MAX_DAYS:
                raise ValueError("--days attend 1 à 30 et exclut --start et --end.")
            start, end = today - timedelta(days=args.days), today - timedelta(days=1)
        else:
            start = args.start or date.fromisoformat(BACKFILL_START_DATE)
            end = args.end or date.fromisoformat(BACKFILL_END_DATE)
        days = history_days(start, end, today)
        if not days:
            raise ValueError("Aucune journée UTC terminée dans cette période.")
        quantity = measures_per_day(args.sensors, args.step_minutes, args.anomaly_rate)
    except ValueError as exc:
        parser.error(str(exc))

    print(f"{len(days)} jour(s) du {days[0]} au {days[-1]}, {quantity} mesures par jour.")
    with closing(get_connection()) as conn:
        totals = load_history(
            conn, days, args.sensors, args.step_minutes, args.anomaly_rate, args.seed,
        )
    print(f"Nouvelles lignes : {totals['measurements']} mesures, {totals['anomalies']} anomalies.")


if __name__ == "__main__":
    main()
