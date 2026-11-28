"""Tests du rapport AQ-43, de la vue PostgreSQL à l'export CSV."""
import csv
from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import pytest

from scripts import quality_report


def test_period_report_uses_utc_and_keeps_missing_days_and_latest_checks(db_conn):
    with db_conn.cursor() as cur:
        cur.execute("SET LOCAL TIME ZONE 'America/New_York'")
        cur.executemany(
            "INSERT INTO measurements (sensor_id, source, measured_at, is_valid) "
            "VALUES (%s, %s, %s, %s)",
            [
                ("CAP_001", "simulator", datetime(2026, 11, 1, 0, 0, tzinfo=timezone.utc), True),
                ("CAP_001", "openaq", datetime(2026, 11, 1, 10, 0, tzinfo=timezone.utc), False),
                ("CAP_002", "simulator", datetime(2026, 11, 4, 10, 0, tzinfo=timezone.utc), True),
            ],
        )
        cur.execute(
            "INSERT INTO anomalies (sensor_id, measured_at, category, anomaly_type, severity) "
            "VALUES ('CAP_001', '2026-11-02T00:00:00Z', 'qualite_donnees', 'missing', 'alerte'), "
            "       ('CAP_001', NULL, 'message', 'json_invalide', 'alerte')"
        )
        cur.execute(
            "INSERT INTO quality_checks (day, check_name, passed, checked_at) VALUES "
            "('2026-11-01', 'volume', FALSE, '2026-11-05T10:00:00Z'), "
            "('2026-11-01', 'volume', TRUE,  '2026-11-05T11:00:00Z'), "
            "('2026-11-01', 'volume', FALSE, '2026-11-05T09:00:00Z'), "
            "('2026-11-02', 'volume', FALSE, '2026-11-05T11:00:00Z')"
        )

    rows = quality_report.period_report(db_conn, date(2026, 11, 1), date(2026, 11, 4))
    assert [row["status"] for row in rows] == [
        "OK", "AUCUNE DONNÉE", "AUCUNE DONNÉE", "non contrôlée",
    ]
    assert rows[0]["measurements"] == 2
    assert rows[0]["completeness"] == 0.5
    assert rows[0]["sources"] == "openaq, simulator"
    assert rows[1]["anomalies"] == 1
    assert rows[1]["checks_passed"] is False
    assert rows[1]["failed_checks"] == "volume"
    assert rows[2]["checks_passed"] is None
    assert rows[3]["day"] == date(2026, 11, 4)

    stats = quality_report.summary(rows)
    assert (stats["days"], stats["days_missing"]) == (4, 2)
    assert (stats["days_ok"], stats["days_failed"]) == (1, 1)
    assert stats["measurements"] == 3
    assert stats["completeness"] == pytest.approx(2 / 3)
    assert stats["anomalies"] == 1


def test_cli_exports_csv_without_an_explicit_output_path(monkeypatch, tmp_path, capsys):
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = [(date(2026, 11, 5), 0, 0, None, 0, None, 1, False, "volume")]
    monkeypatch.setattr(quality_report, "get_connection", lambda: connection)
    monkeypatch.setattr(quality_report, "ROOT_DIR", tmp_path)

    quality_report.main(["--from", "2026-11-05", "--to", "2026-11-06"])

    path = tmp_path / "docs" / "metrics" / "qualite-2026-11-05-2026-11-06.csv"
    with path.open(newline="", encoding="utf-8") as file:
        rows = list(csv.DictReader(file))
    assert [(row["day"], row["status"]) for row in rows] == [
        ("2026-11-05", "AUCUNE DONNÉE"), ("2026-11-06", "AUCUNE DONNÉE"),
    ]
    assert rows[0]["checks_passed"] == "False"
    assert rows[0]["failed_checks"] == "volume"
    assert rows[1]["completeness"] == ""
    assert "sans données : 2" in capsys.readouterr().out
    connection.close.assert_called_once()


def test_cli_rejects_invalid_period_before_connecting(monkeypatch, capsys):
    connection = MagicMock()
    monkeypatch.setattr(quality_report, "get_connection", connection)

    with pytest.raises(SystemExit) as error:
        quality_report.main(["--from", "2026-11-06", "--to", "2026-11-05"])

    assert error.value.code == 2
    assert "--from doit précéder ou égaler --to" in capsys.readouterr().err
    connection.assert_not_called()
