"""Tests de la surveillance du consumer (AQ-39)."""
from datetime import datetime, timedelta, timezone

import orchestration.alerts as alerts
from orchestration.monitor import check_streaming, decide_alert, evaluate

NOW = datetime(2026, 11, 24, 10, 0, tzinfo=timezone.utc)
LIMITS = {"max_silence_min": 5, "max_lag": 1000}


def test_regles_de_decision():
    assert evaluate(None, NOW, LIMITS)[0] == "absent"
    assert evaluate((NOW - timedelta(minutes=20), 0), NOW, LIMITS)[:2] == ("arrete", "critique")
    assert evaluate((NOW - timedelta(minutes=1), 5000), NOW, LIMITS)[:2] == ("en_retard", "alerte")
    assert evaluate((NOW - timedelta(minutes=1), 12), NOW, LIMITS)[0] is None


def test_anti_spam():
    assert decide_alert(None, "arrete", "critique", "x")["title"] == "Consumer streaming arrêté"
    assert decide_alert("arrete", "arrete", "critique", "x") is None        # déjà signalé
    assert decide_alert("arrete", None, None, "ok")["severity"] == "info"   # rétabli
    assert decide_alert(None, None, None, "ok") is None


def heartbeat(conn, last_seen, lag=0):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pipeline_heartbeats (component, status, last_seen, consumer_lag) "
            "VALUES ('aq-streaming', 'running', %s, %s) ON CONFLICT (component) DO UPDATE "
            "SET last_seen = EXCLUDED.last_seen, consumer_lag = EXCLUDED.consumer_lag",
            (last_seen, lag))
    conn.commit()


def test_scenario_complet(db_conn, monkeypatch):
    monkeypatch.setenv("ALERT_CHANNEL", "aucun")
    sent = []
    real_notify = alerts.notify
    monkeypatch.setattr(alerts, "notify",
                        lambda alert, conn=None: sent.append(alert["title"])
                        or real_notify(alert, conn=conn))

    heartbeat(db_conn, NOW - timedelta(minutes=1))
    assert check_streaming(db_conn, NOW, LIMITS)["alerted"] is False      # tout va bien

    later = NOW + timedelta(minutes=30)                                    # consumer arrêté
    assert check_streaming(db_conn, later, LIMITS)["problem"] == "arrete"
    assert check_streaming(db_conn, later + timedelta(minutes=15), LIMITS)["alerted"] is False

    heartbeat(db_conn, later + timedelta(minutes=29))                      # relancé
    assert check_streaming(db_conn, later + timedelta(minutes=30), LIMITS)["alerted"] is True

    assert sent == ["Consumer streaming arrêté", "Consumer streaming rétabli"]
    with db_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM pipeline_alerts WHERE source = 'streaming_monitor'")
        assert cur.fetchone()[0] == 2
