
import random
from datetime import datetime, timezone

from common.contract import validate_message
from ingestion.simulator import (
    ANOMALY_TYPES,
    inject_anomaly,
    make_sensors,
    normal_measurements,
    stream,
)


def test_200_messages_respect_contract():
    messages = list(stream(count=40, interval=0, seed=42))
    assert len(messages) == 200
    assert all(not validate_message(message) for message in messages)


def test_normal_measurements_are_physically_possible():
    rng = random.Random(42)

    for _ in range(200):
        measurements = normal_measurements(rng)
        assert all(
            value >= 0
            for value in measurements.values()
        )
        assert 0 <= measurements["humidity"] <= 100


def test_anomaly_rate_one_labels_every_message():
    messages = list(
        stream(count=10, interval=0, anomaly_rate=1, seed=42)
    )
    assert len(messages) == 50
    assert all(
        message["injected_anomaly"] in ANOMALY_TYPES
        for message in messages
    )


def test_negative_anomaly_creates_negative_value():
    measurements = normal_measurements(random.Random(1))
    result = inject_anomaly(
        measurements, "negative", None, random.Random(2)
    )
    assert any(
        value < 0
        for value in result.values()
        if value is not None
    )


def test_stuck_anomaly_repeats_previous_measurements():
    previous = {
        "pm2_5": 10.0,
        "pm10": 20.0,
        "no2": 15.0,
        "o3": 30.0,
        "temperature": 25.0,
        "humidity": 50.0,
    }

    result = inject_anomaly(
        normal_measurements(random.Random(1)),
        "stuck",
        previous,
        random.Random(2),
    )
    assert result == previous


def test_silent_sensor_does_not_emit_message():
    sensors = make_sensors(1, random.Random(42))
    sensors[0]["mute_left"] = 1

    messages = list(
        stream(
            count=1,
            interval=0,
            seed=42,
            sensors=sensors,
        )
    )
    assert messages == []


def test_same_seed_reproduces_same_messages():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)

    first = list(
        stream(count=5, interval=0, seed=123, start_time=start)
    )
    second = list(
        stream(count=5, interval=0, seed=123, start_time=start)
    )

    assert first == second
