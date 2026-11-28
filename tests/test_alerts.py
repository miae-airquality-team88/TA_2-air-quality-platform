"""Tests des alertes (AQ-38), sans vrai Slack ni email."""
from datetime import datetime, timezone
from types import SimpleNamespace

import orchestration.alerts as alerts

CONTEXT = {
    "task_instance": SimpleNamespace(task_id="validate_quality", try_number=2,
                                     log_url="http://localhost:8080/log?x=1"),
    "dag": SimpleNamespace(dag_id="air_quality_daily"),
    "params": {"day": None},
    "data_interval_start": datetime(2026, 11, 20, tzinfo=timezone.utc),
    "run_id": "scheduled__2026-11-20",
    "exception": ValueError("Contrôles qualité en échec : volume"),
}


def rows(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT source, severity, task_id, day, try_number, channel, delivered, "
                    "delivery_error FROM pipeline_alerts ORDER BY id")
        return cur.fetchall()


def test_alerte_construite_depuis_le_contexte():
    alert = alerts.alert_from_context(CONTEXT)
    assert alert["title"] == "Échec de la tâche validate_quality (air_quality_daily)"
    assert alert["day"] == "2026-11-20" and alert["try_number"] == 2
    assert "volume" in alert["message"]
    texte = alerts.format_text(alert)
    assert "Journée : 2026-11-20" in texte and "Logs : http://localhost:8080" in texte


def test_envoi_slack_reussi(monkeypatch):
    envoyes = []
    monkeypatch.setitem(alerts.SENDERS, "slack", lambda alert: envoyes.append(alert))
    assert alerts.deliver({"title": "t"}, channel="slack") == ("slack", True, None)
    assert len(envoyes) == 1


def test_envoi_en_echec_ne_leve_pas(monkeypatch):
    def panne(alert):
        raise ConnectionError("Slack injoignable")
    monkeypatch.setitem(alerts.SENDERS, "slack", panne)
    channel, delivered, error = alerts.deliver({"title": "t"}, channel="slack")
    assert (channel, delivered) == ("slack", False) and "injoignable" in error


def test_slack_sans_webhook(monkeypatch):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    assert alerts.deliver({"title": "t", "severity": "info", "message": "m"},
                          channel="slack")[1] is False


def test_callback_enregistre_meme_si_envoi_echoue(db_conn, monkeypatch):
    monkeypatch.setenv("ALERT_CHANNEL", "slack")
    monkeypatch.setitem(alerts.SENDERS, "slack",
                        lambda alert: (_ for _ in ()).throw(ConnectionError("hors ligne")))
    alerts.notify(alerts.alert_from_context(CONTEXT), conn=db_conn)
    (row,) = rows(db_conn)
    assert row[:7] == ("dag_failure", "critique", "validate_quality", "2026-11-20", 2,
                       "slack", False)
    assert "hors ligne" in row[7]


def test_sans_canal_enregistre_quand_meme(db_conn, monkeypatch):
    monkeypatch.setenv("ALERT_CHANNEL", "aucun")
    alerts.notify(alerts.alert_from_context(CONTEXT), conn=db_conn)
    assert rows(db_conn)[0][5:7] == ("aucun", False)


def test_callback_ne_plante_jamais(monkeypatch):
    def panne(alert, conn=None):
        raise RuntimeError("base indisponible")
    monkeypatch.setattr(alerts, "notify", panne)
    alerts.task_failure_callback(CONTEXT)   # aucune exception ne remonte à Airflow
