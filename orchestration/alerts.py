"""Alertes du pipeline (AQ-38).

Quand une tâche d'un DAG échoue (après ses nouvelles tentatives), Airflow appelle
task_failure_callback(). L'alerte est :
  1. ENVOYÉE sur le canal choisi par l'équipe (variable ALERT_CHANNEL du .env) :
       slack -> message dans un canal Slack via un « webhook entrant » (SLACK_WEBHOOK_URL)
       email -> email via un serveur SMTP (SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD,
                ALERT_EMAIL_TO)
       aucun -> pas d'envoi (développement)
  2. ENREGISTRÉE dans la table pipeline_alerts, MÊME si l'envoi a échoué : on garde
     toujours la trace d'un incident.

Règle d'or : une fonction d'alerte ne doit JAMAIS lever d'erreur elle-même (sinon
elle masquerait l'erreur d'origine). Tout est protégé par des try/except.

notify() est aussi utilisée par le DAG de surveillance du consumer (AQ-39).
"""
import os
import smtplib
from email.message import EmailMessage

import requests

SEVERITY_ICONS = {"info": "ℹ️", "alerte": "⚠️", "critique": "🚨"}


def alert_channel():
    return os.getenv("ALERT_CHANNEL", "aucun").strip().lower() or "aucun"


def format_text(alert):
    """Texte lisible de l'alerte (Slack et email)."""
    lines = [f"{SEVERITY_ICONS.get(alert['severity'], '')} {alert['title']}", alert["message"]]
    for label, key in [("DAG", "dag_id"), ("Tâche", "task_id"), ("Journée", "day"),
                       ("Tentative", "try_number"), ("Logs", "log_url")]:
        if alert.get(key) not in (None, ""):
            lines.append(f"{label} : {alert[key]}")
    return "\n".join(lines)


def send_slack(alert, webhook_url=None, timeout=10):
    """Poste l'alerte dans Slack via un webhook entrant."""
    url = webhook_url or os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        raise RuntimeError("SLACK_WEBHOOK_URL manquant dans le .env")
    response = requests.post(url, json={"text": format_text(alert)}, timeout=timeout)
    response.raise_for_status()


def send_email(alert):
    """Envoie l'alerte par email (SMTP avec chiffrement STARTTLS)."""
    host = os.getenv("SMTP_HOST")
    to = os.getenv("ALERT_EMAIL_TO")
    if not host or not to:
        raise RuntimeError("SMTP_HOST ou ALERT_EMAIL_TO manquant dans le .env")
    msg = EmailMessage()
    msg["Subject"] = f"[Air Quality] {alert['title']}"
    msg["From"] = os.getenv("SMTP_USER", "airflow@localhost")
    msg["To"] = to
    msg.set_content(format_text(alert))
    with smtplib.SMTP(host, int(os.getenv("SMTP_PORT", "587")), timeout=15) as smtp:
        smtp.starttls()
        if os.getenv("SMTP_USER"):
            smtp.login(os.getenv("SMTP_USER"), os.getenv("SMTP_PASSWORD", ""))
        smtp.send_message(msg)


SENDERS = {"slack": send_slack, "email": send_email}


def deliver(alert, channel=None):
    """Envoie l'alerte. Renvoie (canal, envoyé ?, erreur éventuelle). Ne lève jamais."""
    channel = channel or alert_channel()
    sender = SENDERS.get(channel)
    if sender is None:
        return "aucun", False, None
    try:
        sender(alert)
        return channel, True, None
    except Exception as exc:
        return channel, False, f"{exc.__class__.__name__}: {exc}"[:500]


INSERT_SQL = """
INSERT INTO pipeline_alerts (source, severity, title, message, dag_id, task_id, run_id, day,
                             try_number, log_url, channel, delivered, delivery_error)
VALUES (%(source)s, %(severity)s, %(title)s, %(message)s, %(dag_id)s, %(task_id)s, %(run_id)s,
        %(day)s, %(try_number)s, %(log_url)s, %(channel)s, %(delivered)s, %(delivery_error)s)
"""


def record_alert(conn, alert, channel, delivered, error):
    """Enregistre l'alerte dans pipeline_alerts."""
    row = {key: alert.get(key) for key in
           ["source", "severity", "title", "message", "dag_id", "task_id", "run_id", "day",
            "try_number", "log_url"]}
    row.update(channel=channel, delivered=delivered, delivery_error=error)
    with conn.cursor() as cur:
        cur.execute(INSERT_SQL, row)
    conn.commit()


def notify(alert, conn=None):
    """Envoie puis enregistre une alerte. Ne lève jamais d'erreur. Renvoie (canal, envoyé)."""
    channel, delivered, error = deliver(alert)
    print(f"Alerte « {alert['title']} » : canal={channel}, envoyée={delivered}"
          + (f", erreur={error}" if error else ""))
    own_conn = conn is None
    try:
        if own_conn:
            from common.db import get_connection
            conn = get_connection()
        record_alert(conn, alert, channel, delivered, error)
    except Exception as exc:
        print(f"Impossible d'enregistrer l'alerte en base : {exc}")
    finally:
        if own_conn and conn is not None:
            conn.close()
    return channel, delivered


def alert_from_context(context):
    """Construit l'alerte à partir du « contexte » qu'Airflow passe au callback."""
    ti = context.get("task_instance")
    dag = context.get("dag")
    params = context.get("params") or {}
    interval_start = context.get("data_interval_start")
    day = params.get("day") or (interval_start.strftime("%Y-%m-%d") if interval_start else None)
    exception = context.get("exception")
    return {
        "source": "dag_failure",
        "severity": "critique",
        "title": f"Échec de la tâche {getattr(ti, 'task_id', '?')} "
                 f"({getattr(dag, 'dag_id', '?')})",
        "message": f"Erreur : {exception}" if exception else "La tâche a échoué.",
        "dag_id": getattr(dag, "dag_id", None),
        "task_id": getattr(ti, "task_id", None),
        "run_id": context.get("run_id"),
        "day": day,
        "try_number": getattr(ti, "try_number", None),
        "log_url": getattr(ti, "log_url", None),
    }


def task_failure_callback(context):
    """À mettre dans default_args["on_failure_callback"] de chaque DAG."""
    try:
        notify(alert_from_context(context))
    except Exception as exc:   # sécurité ultime : le callback ne doit jamais planter
        print(f"Erreur dans le callback d'alerte : {exc}")
