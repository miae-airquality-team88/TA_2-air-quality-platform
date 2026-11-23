from datetime import date

import pytest

from ingestion.history import generate_messages, history_days, load_history, measures_per_day

DAY = date(2026, 11, 21)
TODAY = date(2026, 11, 23)


def test_history_uses_only_completed_utc_days():
    assert history_days(DAY, TODAY, today=TODAY) == [DAY, date(2026, 11, 22)]
    assert history_days(TODAY, TODAY, today=TODAY) == []
    with pytest.raises(ValueError, match="30 jours"):
        history_days(date(2026, 10, 22), TODAY, today=TODAY)


def test_default_messages_have_daily_volume_unique_offsets_and_reproducible_labels():
    entries = list(generate_messages(DAY))
    assert len(entries) == 1440
    assert len({offset for _, offset in entries}) == 1440
    assert entries[0][0]["timestamp"] == "2026-11-21T00:00:00Z"
    assert entries[-1][0]["timestamp"] == "2026-11-21T23:55:00Z"
    assert any(message["injected_anomaly"] for message, _ in entries)
    assert entries == list(generate_messages(DAY))


@pytest.mark.parametrize("sensors,step,rate", [(0, 5, 0), (100, 5, 0), (1, 0, 0), (1, 5, 1.1)])
def test_invalid_options(sensors, step, rate):
    with pytest.raises(ValueError):
        measures_per_day(sensors, step, rate)


def test_load_writes_pipeline_outputs_once(db_conn):
    days = history_days(date(2026, 10, 24), TODAY, today=TODAY)
    created = load_history(db_conn, days, batch_size=500)
    assert created["measurements"] == 43200
    assert created["anomalies"] > 0
    assert load_history(db_conn, days[-1:]) == {"measurements": 0, "anomalies": 0}
    assert load_history(db_conn, days[:1], seed=7) == {"measurements": 0, "anomalies": 0}

    with db_conn.cursor() as cursor:
        cursor.execute(
            """SELECT (measured_at AT TIME ZONE 'UTC')::date AS day,
                      COUNT(*), COUNT(*) FILTER (WHERE is_valid),
                      COUNT(pm2_5_mean_5min), COUNT(injected_anomaly)
               FROM measurements
               WHERE kafka_partition = -1 AND source = 'simulator'
               GROUP BY day ORDER BY day"""
        )
        daily = cursor.fetchall()
        cursor.execute("SELECT COUNT(*) FROM anomalies WHERE kafka_partition = -1")
        assert cursor.fetchone() == (created["anomalies"],)

    assert [row[0] for row in daily] == days
    for _, count, valid, windows, injected in daily:
        assert count == 1440
        assert valid / count > 0.8
        assert windows > 0
        assert injected > 0
