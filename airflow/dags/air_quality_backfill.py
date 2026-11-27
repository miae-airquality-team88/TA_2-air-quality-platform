"""DAG de backfill : réexécution historique du pipeline (Phase 2, AQ-41).

Le sujet demande « un DAG pour le backfill (réexécution historique) ». Ce DAG
rejoue, pour CHAQUE journée de la période historique, les étapes du pipeline
quotidien :

    extract (AQ-27) ─► validate_quality (AQ-29) ─► aggregate (AQ-31)

Fonctionnement : catchup=True + start_date + end_date. Dès que le DAG est
activé, Airflow crée automatiquement UNE exécution par journée de la période
(30 exécutions pour 30 jours), au plus 2 en parallèle.

Pas d'entraînement ici : le DAG quotidien réentraîne chaque jour le modèle sur
les 7 derniers jours d'extractions, que ce backfill vient justement de produire.

La période est réglable dans le .env : BACKFILL_START_DATE / BACKFILL_END_DATE
(les mêmes dates que le générateur d'historique AQ-36).

Relancer une journée (bouton « Clear » dans Airflow) ne crée aucun doublon :
toutes les étapes sont idempotentes.
"""
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from common.config import BACKFILL_END_DATE, BACKFILL_START_DATE
from orchestration.alerts import task_failure_callback

DAY = "{{ data_interval_start | ds }}"    # la journée de CETTE exécution


def _extract(day):
    from orchestration.tasks import run_extract

    return run_extract(day)


def _validate_quality(day, run_id):
    from orchestration.tasks import run_validate_quality

    return run_validate_quality(day, run_id)


def _aggregate(day):
    from orchestration.tasks import run_aggregate

    return run_aggregate(day)


with DAG(
    dag_id="air_quality_backfill",
    description="Réexécution historique : extract, qualité et agrégats pour chaque journée passée",
    schedule="@daily",
    start_date=pendulum.parse(BACKFILL_START_DATE, tz="UTC"),
    end_date=pendulum.parse(BACKFILL_END_DATE, tz="UTC"),
    catchup=True,                # une exécution par journée de la période
    max_active_runs=2,           # au plus 2 journées traitées en même temps
    default_args={
        "owner": "equipe-aq",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=2),
        "on_failure_callback": task_failure_callback,
    },
    tags=["phase2", "dataops", "backfill"],
    doc_md=__doc__,
) as dag:

    extract = PythonOperator(task_id="extract", python_callable=_extract,
                             op_kwargs={"day": DAY})

    validate_quality = PythonOperator(task_id="validate_quality",
                                      python_callable=_validate_quality,
                                      op_kwargs={"day": DAY, "run_id": "{{ run_id }}"})

    aggregate = PythonOperator(task_id="aggregate", python_callable=_aggregate,
                               op_kwargs={"day": DAY})

    extract >> validate_quality >> aggregate
