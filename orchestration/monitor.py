"""Surveillance du consumer streaming (AQ-39).

Toutes les 15 minutes, le DAG streaming_monitor lit le signal d'activité du
consumer (table pipeline_heartbeats, AQ-34) et décide :

| Situation                                        | Problème       | Gravité   |
|--------------------------------------------------|----------------|-----------|
| aucun signal                                     | absent         | critique  |
| dernier signal il y a plus de 5 minutes          | arrete         | critique  |
| retard Kafka > 1 000 messages                    | en_retard      | alerte    |
| sinon                                            | (aucun)        | -         |

Anti-spam : l'état est mémorisé dans monitor_state. Une alerte n'est envoyée que
lorsque la situation CHANGE : apparition d'un problème, changement de problème,
ou retour à la normale (message « rétabli »).
"""
import os
from datetime import datetime, timezone

COMPONENT = "aq-streaming"


def thresholds():
    return {
        "max_silence_min": float(os.getenv("MONITOR_MAX_SILENCE_MIN", "5")),
        "max_lag": int(os.getenv("MONITOR_MAX_LAG", "1000")),
    }


def evaluate(heartbeat, now, limits=None):
    """Décide s'il y a un problème. heartbeat = (last_seen, consumer_lag) ou None.

    Renvoie (problème ou None, gravité, explication).
    """
    limits = limits or thresholds()
    if heartbeat is None:
        return "absent", "critique", "aucun signal d'activité : le consumer n'a jamais tourné"
    last_seen, lag = heartbeat
    silence = (now - last_seen).total_seconds() / 60
    if silence > limits["max_silence_min"]:
        return ("arrete", "critique",
                f"dernier signal il y a {silence:.0f} min (maximum {limits['max_silence_min']:g})")
    if lag is not None and lag > limits["max_lag"]:
        return ("en_retard", "alerte",
                f"retard Kafka de {lag} messages (maximum {limits['max_lag']})")
    return None, None, f"consumer actif (signal il y a {silence:.0f} min, retard {lag})"


def decide_alert(previous_problem, problem, severity, reason):
    """Faut-il alerter ? Renvoie l'alerte (dict) ou None, en évitant les répétitions."""
    if problem == previous_problem:
        return None                               # rien de nouveau : pas de spam
    if problem is None:
        return {"severity": "info", "title": "Consumer streaming rétabli",
                "message": f"Retour à la normale après « {previous_problem} » : {reason}"}
    titles = {"absent": "Consumer streaming absent", "arrete": "Consumer streaming arrêté",
              "en_retard": "Consumer streaming en retard"}
    return {"severity": severity, "title": titles.get(problem, problem), "message": reason}


def check_streaming(conn, now=None, limits=None, component=COMPONENT):
    """Point d'entrée du DAG : évalue, alerte si besoin, mémorise l'état. Renvoie un résumé."""
    from orchestration.alerts import notify

    now = now or datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute("SELECT last_seen, consumer_lag FROM pipeline_heartbeats WHERE component = %s",
                    (component,))
        heartbeat = cur.fetchone()
        cur.execute("SELECT problem FROM monitor_state WHERE component = %s", (component,))
        row = cur.fetchone()
    previous = row[0] if row else None

    problem, severity, reason = evaluate(heartbeat, now, limits)
    print(f"Surveillance : {reason}")
    alert = decide_alert(previous, problem, severity, reason)
    if alert:
        alert.update(source="streaming_monitor", dag_id="streaming_monitor",
                     task_id="check_streaming")
        notify(alert, conn=conn)

    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO monitor_state (component, problem, since, updated_at)
               VALUES (%(c)s, %(p)s, %(now)s, clock_timestamp())
               ON CONFLICT (component) DO UPDATE SET
                   since = CASE WHEN monitor_state.problem IS NOT DISTINCT FROM EXCLUDED.problem
                                THEN monitor_state.since ELSE EXCLUDED.since END,
                   problem = EXCLUDED.problem,
                   updated_at = clock_timestamp()""",
            {"c": component, "p": problem, "now": now},
        )
    conn.commit()
    return {"problem": problem, "alerted": alert is not None, "reason": reason}
