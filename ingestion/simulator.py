
"""Simulateur IoT AQ-9 : génération de messages et injection d'anomalies."""

import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from common.config import DATA_DIR
from common.contract import validate_message


SENSOR_IDS = ["CAP_001", "CAP_002", "CAP_003"]

ANOMALIES = [
    "spike",
    "negative",
    "out_of_range",
    "stuck",
    "missing_values",
]

MEASUREMENT_KEYS = [
    "pm2_5",
    "pm10",
    "no2",
    "o3",
    "temperature",
    "humidity",
]


def generate_measurements(rng):
    """Génère des mesures normales et réalistes."""

    return {
        "pm2_5": round(rng.uniform(5, 80), 1),
        "pm10": round(rng.uniform(10, 120), 1),
        "no2": round(rng.uniform(5, 100), 1),
        "o3": round(rng.uniform(10, 150), 1),
        "temperature": round(rng.uniform(10, 35), 1),
        "humidity": round(rng.uniform(20, 85), 1),
    }


def inject_anomaly(measurements, anomaly, previous):
    """Injecte une anomalie dans les mesures."""

    if anomaly == "spike":
        measurements["pm2_5"] = round(
            measurements["pm2_5"] * 7, 1
        )

    elif anomaly == "negative":
        measurements["no2"] = -abs(measurements["no2"])

    elif anomaly == "out_of_range":
        measurements["humidity"] = 105.0

    elif anomaly == "stuck":
        measurements = previous.copy()

    elif anomaly == "missing_values":
        for key in random.sample(MEASUREMENT_KEYS, 3):
            measurements[key] = None

    return measurements


def generate_message(sensor_id, timestamp, rng, previous):
    """Construit un message conforme au contrat AQ-15."""

    measurements = generate_measurements(rng)

    anomaly = None

    if rng.random() < 0.25:
        available = ANOMALIES.copy()

        if sensor_id not in previous:
            available.remove("stuck")

        anomaly = rng.choice(available)
        measurements = inject_anomaly(
            measurements,
            anomaly,
            previous.get(sensor_id, {}),
        )

    message = {
        "sensor_id": sensor_id,
        "timestamp": timestamp.strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
        "location": {
            "lat": round(30.4202 + rng.uniform(-0.01, 0.01), 6),
            "lon": round(-9.5982 + rng.uniform(-0.01, 0.01), 6),
        },
        "measurements": measurements,
        "status": "active",
        "source": "simulator",
        "injected_anomaly": anomaly,
    }

    errors = validate_message(message)

    if errors:
        raise ValueError(
            f"Message invalide : {errors}"
        )

    previous[sensor_id] = measurements.copy()

    return message


def run_simulator(count, seed, output):
    """Génère, valide et enregistre les messages."""

    rng = random.Random(seed)
    previous = {}

    messages = []
    start = datetime.now(timezone.utc).replace(microsecond=0)

    for index in range(count):
        sensor_id = SENSOR_IDS[index % len(SENSOR_IDS)]
        timestamp = start + timedelta(seconds=index)

        message = generate_message(
            sensor_id,
            timestamp,
            rng,
            previous,
        )

        messages.append(message)

    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as file:
        for message in messages:
            file.write(
                json.dumps(message, ensure_ascii=False) + "\n"
            )

    anomalies = sum(
        message["injected_anomaly"] is not None
        for message in messages
    )

    print("=== SIMULATEUR AQ-9 ===")
    print(f"Messages générés : {len(messages)}")
    print(f"Messages conformes : {len(messages)}")
    print(f"Messages avec anomalies : {anomalies}")
    print(f"Fichier créé : {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Simulateur IoT AQ-9"
    )

    parser.add_argument(
        "--count",
        type=int,
        default=20,
        help="Nombre de messages à générer",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Graine aléatoire pour reproduire les résultats",
    )

    parser.add_argument(
        "--output",
        default=str(DATA_DIR / "simulator_output.jsonl"),
        help="Chemin du fichier de sortie",
    )

    args = parser.parse_args()

    if args.count < 1:
        parser.error("--count doit être supérieur ou égal à 1")

    run_simulator(
        args.count,
        args.seed,
        args.output,
    )


if __name__ == "__main__":
    main()
