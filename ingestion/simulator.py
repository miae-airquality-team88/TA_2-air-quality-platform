
"""Simulateur IoT AQ-9 : generation de mesures et injection d'anomalies."""

import argparse
import json
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from common.config import DATA_DIR
from common.contract import validate_message

DEFAULT_LAT = 30.4202
DEFAULT_LON = -9.5982

NORMAL_PROFILE = {
    "pm2_5": (15.0, 5.0),
    "pm10": (25.0, 8.0),
    "no2": (20.0, 7.0),
    "o3": (40.0, 12.0),
    "temperature": (25.0, 5.0),
    "humidity": (55.0, 12.0),
}

ANOMALY_TYPES = (
    "spike",
    "negative",
    "out_of_range",
    "stuck",
    "missing_values",
)

MEASUREMENT_KEYS = tuple(NORMAL_PROFILE)


def base_coordinates():
    """Renvoie les coordonnees de reference."""
    return DEFAULT_LAT, DEFAULT_LON


def make_sensors(n=5, rng=None):
    """Cree des capteurs repartis autour de la station."""
    if n < 1:
        raise ValueError("n doit etre superieur ou egal a 1")

    rng = rng or random.Random()
    base_lat, base_lon = base_coordinates()
    sensors = []

    for index in range(1, n + 1):
        sensors.append({
            "sensor_id": f"CAP_{index:03d}",
            "location": {
                "lat": round(base_lat + rng.uniform(-0.018, 0.018), 6),
                "lon": round(base_lon + rng.uniform(-0.020, 0.020), 6),
            },
            "last": None,
            "mute_left": 0,
        })

    return sensors


def normal_measurements(rng):
    """Genere des mesures normales avec une distribution gaussienne."""
    measurements = {}

    for key, (mean, stddev) in NORMAL_PROFILE.items():
        value = rng.gauss(mean, stddev)

        if key == "humidity":
            value = max(0.0, min(100.0, value))
        else:
            value = max(0.0, value)

        measurements[key] = round(value, 1)

    return measurements


def inject_anomaly(measurements, anomaly, previous, rng):
    """Injecte une anomalie dans une copie des mesures."""
    result = measurements.copy()

    if anomaly == "spike":
        key = rng.choice(("pm2_5", "pm10", "no2", "o3"))
        result[key] = round(result[key] * rng.uniform(5, 10), 1)

    elif anomaly == "negative":
        key = rng.choice(("pm2_5", "pm10", "no2", "o3"))
        result[key] = -round(abs(result[key]), 1)

    elif anomaly == "out_of_range":
        if rng.choice(("humidity", "temperature")) == "humidity":
            result["humidity"] = round(rng.uniform(101, 120), 1)
        else:
            result["temperature"] = round(rng.uniform(61, 80), 1)

    elif anomaly == "stuck":
        if previous is None:
            raise ValueError("stuck necessite des mesures precedentes")
        result = previous.copy()

    elif anomaly == "missing_values":
        number_missing = rng.randint(1, 3)
        for key in rng.sample(list(MEASUREMENT_KEYS), number_missing):
            result[key] = None

    else:
        raise ValueError(f"Anomalie inconnue : {anomaly}")

    return result


def build_message(sensor, timestamp, measurements, anomaly=None):
    """Construit et valide un message selon le contrat AQ-15."""
    message = {
        "sensor_id": sensor["sensor_id"],
        "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "location": sensor["location"].copy(),
        "measurements": measurements.copy(),
        "status": "active",
        "source": "simulator",
        "injected_anomaly": anomaly,
    }

    errors = validate_message(message)
    if errors:
        raise ValueError(f"Message invalide : {errors}")

    return message


def stream(
    count=None,
    interval=1.0,
    anomaly_rate=0.05,
    seed=None,
    sensors=None,
    start_time=None,
):
    """
    Genere les messages un par un.

    count correspond au nombre de cycles.
    Chaque cycle peut produire un message par capteur.
    Sans count, la generation continue indefiniment.
    """
    if count is not None and count < 1:
        raise ValueError("count doit etre superieur ou egal a 1")
    if interval < 0:
        raise ValueError("interval ne peut pas etre negatif")
    if not 0 <= anomaly_rate <= 1:
        raise ValueError("anomaly_rate doit etre entre 0 et 1")

    rng = random.Random(seed)
    sensors = sensors if sensors is not None else make_sensors(5, rng)

    if start_time is not None:
        current_time = start_time
    elif seed is not None:
        current_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    else:
        current_time = datetime.now(timezone.utc)

    current_time = current_time.replace(microsecond=0)
    cycle = 0

    while count is None or cycle < count:
        timestamp = current_time + timedelta(seconds=cycle)

        for sensor in sensors:
            if sensor["mute_left"] > 0:
                sensor["mute_left"] -= 1
                continue

            # Silence aleatoire du capteur pendant 10 a 30 cycles.
            if rng.random() < 0.001:
                sensor["mute_left"] = rng.randint(10, 30)
                continue

            measurements = normal_measurements(rng)
            anomaly = None

            if rng.random() < anomaly_rate:
                available = list(ANOMALY_TYPES)

                if sensor["last"] is None:
                    available.remove("stuck")

                anomaly = rng.choice(available)
                measurements = inject_anomaly(
                    measurements,
                    anomaly,
                    sensor["last"],
                    rng,
                )

            message = build_message(
                sensor,
                timestamp,
                measurements,
                anomaly,
            )

            sensor["last"] = measurements.copy()
            yield message

        cycle += 1

        if interval > 0 and (count is None or cycle < count):
            time.sleep(interval)


def run_simulator(count, interval, anomaly_rate, seed, output):
    """Execute le simulateur et enregistre les messages JSONL."""
    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    total = 0
    anomalies = 0

    with output_path.open("w", encoding="utf-8") as file:
        for message in stream(
            count=count,
            interval=interval,
            anomaly_rate=anomaly_rate,
            seed=seed,
        ):
            file.write(json.dumps(message, ensure_ascii=False) + "\n")
            total += 1

            if message["injected_anomaly"] is not None:
                anomalies += 1

    print("=== SIMULATEUR AQ-9 ===")
    print(f"Messages generes : {total}")
    print(f"Messages conformes : {total}")
    print(f"Messages avec anomalies : {anomalies}")
    print(f"Fichier cree : {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Simulateur IoT AQ-9")

    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--anomaly-rate", type=float, default=0.05)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--output",
        default=str(DATA_DIR / "simulator_output.jsonl"),
    )

    args = parser.parse_args()

    if args.count < 1:
        parser.error("--count doit etre superieur ou egal a 1")
    if args.interval < 0:
        parser.error("--interval ne peut pas etre negatif")
    if not 0 <= args.anomaly_rate <= 1:
        parser.error("--anomaly-rate doit etre entre 0 et 1")

    run_simulator(
        args.count,
        args.interval,
        args.anomaly_rate,
        args.seed,
        args.output,
    )


if __name__ == "__main__":
    main()
