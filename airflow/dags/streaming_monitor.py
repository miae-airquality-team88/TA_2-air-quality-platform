"""Surveillance du consumer streaming (AQ-39).

Toutes les 15 minutes : le consumer est-il vivant et à jour ? Sinon, alerte
(Slack/email + table pipeline_alerts), une seule fois par problème.

Astuce : quand vous arrêtez volontairement le streaming (fin de séance),
mettez ce DAG en pause (interrupteur) pour ne pas recevoir l'alerte « arrêté ».
"""
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from orchestration.alerts import task_failure_callback


def _check_streaming():
    from common.db import get_connection
    from orchestration.monitor import check_streaming

    conn = get_connection()
    try:
        return check_streaming(conn)
    finally:
        conn.close()


with DAG(
    dag_id="streaming_monitor",
    description="Surveillance du consumer streaming (signal d'activité et retard Kafka)",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "equipe-aq",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=1),
        "on_failure_callback": task_failure_callback,
    },
    tags=["phase2", "dataops", "surveillance"],
    doc_md=__doc__,
) as dag:
    PythonOperator(task_id="check_streaming", python_callable=_check_streaming)
