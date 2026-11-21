"""DAG quotidien de la plateforme qualité de l'air (Phase 2, AQ-27).

Chaque jour, pour la journée précédente (ou celle choisie au déclenchement) :

  wait_new_data (SqlSensor, AQ-33) ──────┐
                                        ├─► extract (AQ-27) ─► validate_quality (AQ-29)
  wait_streaming_done (PythonSensor, AQ-34)┘                          │
            ┌─────────────────────────────────────────────────────────┤
            ▼                                                         ▼
  aggregate (AQ-31)              train ─► evaluate (AQ-32) ─► deploy_if_better (AQ-28)
            │                                                   │            │
            │                                          deploy_model    skip_deploy
            └──────────────────────────► end ◄──────────────┴────────────┘

Les fonctions métier sont dans le package orchestration/ (testées sans Airflow).
Ce fichier ne fait que les ENCHAÎNER : c'est la bonne pratique pour garder des
DAGs simples, rapides à charger et faciles à tester.

Déclencher pour une journée précise : bouton ▶ « Trigger DAG w/ config »,
puis saisir la date dans le champ « day » (ex. 2026-11-14).
"""
import pendulum
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.sensors.python import PythonSensor

from orchestration.sensors import new_data_sql, streaming_done_for_sensor

# Journée traitée : le paramètre « day » s'il est rempli, sinon le début de
# l'intervalle de données (= la veille pour une exécution planifiée).
DAY = "{{ params.day or data_interval_start | ds }}"


# --------------------------------------------------------------------------
# Fonctions appelées par les tâches. Les imports sont faits À L'INTÉRIEUR des
# fonctions : Airflow relit ce fichier toutes les 30 s, il doit rester léger.
# --------------------------------------------------------------------------
def _with_connection(func, *args, **kwargs):
    from common.db import get_connection

    conn = get_connection()
    try:
        return func(conn, *args, **kwargs)
    finally:
        conn.close()


def _extract(day):
    from common.config import EXTRACTS_DIR
    from orchestration.extract import extract_day

    return _with_connection(extract_day, day, EXTRACTS_DIR)


def _validate_quality(day, run_id):
    from orchestration.quality import validate_day

    return _with_connection(validate_day, day, run_id=run_id)


def _aggregate(day):
    from orchestration.aggregate import run_daily_aggregate

    return _with_connection(run_daily_aggregate, day)


def _train(day):
    from common.config import EXTRACTS_DIR, MODELS_DIR
    from orchestration.training import NotEnoughDataError, train

    try:
        return train(EXTRACTS_DIR, MODELS_DIR, day)
    except NotEnoughDataError as exc:
        # Pas assez de données : on saute l'entraînement au lieu d'échouer
        raise AirflowSkipException(f"Entraînement sauté : {exc}") from exc


def _evaluate(ti):
    from orchestration.training import evaluate

    info = ti.xcom_pull(task_ids="train")
    return evaluate(info["model_path"], info["test_path"])


def _choose_branch(ti):
    from common.config import MODELS_DIR
    from orchestration.deploy import choose_branch

    return choose_branch(ti.xcom_pull(task_ids="evaluate"), MODELS_DIR)


def _deploy(ti):
    from common.config import MODELS_DIR
    from orchestration.deploy import promote

    return promote(ti.xcom_pull(task_ids="evaluate"), MODELS_DIR)


# --------------------------------------------------------------------------
# Définition du DAG
# --------------------------------------------------------------------------
with DAG(
    dag_id="air_quality_daily",
    description="Pipeline quotidien : extraction, qualité, agrégats, entraînement, déploiement",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,               # le rattrapage de l'historique sera un DAG dédié (Sprint 4)
    max_active_runs=1,
    default_args={
        "owner": "equipe-aq",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=2),
    },
    params={
        "day": Param(None, type=["null", "string"],
                     description="Journée à traiter (AAAA-MM-JJ). Vide = la veille."),
    },
    tags=["phase2", "dataops", "mlops"],
    doc_md=__doc__,
) as dag:

    wait_new_data = SqlSensor(
        task_id="wait_new_data",
        conn_id="airquality_db",
        sql=new_data_sql(DAY),
        mode="reschedule",        # libère la place entre deux vérifications
        poke_interval=60,
        timeout=2 * 60 * 60,
    )

    wait_streaming_done = PythonSensor(
        task_id="wait_streaming_done",
        python_callable=streaming_done_for_sensor,
        op_kwargs={"day": DAY},
        mode="reschedule",
        poke_interval=60,
        timeout=2 * 60 * 60,
    )

    extract = PythonOperator(task_id="extract", python_callable=_extract,
                             op_kwargs={"day": DAY})

    validate_quality = PythonOperator(task_id="validate_quality",
                                      python_callable=_validate_quality,
                                      op_kwargs={"day": DAY, "run_id": "{{ run_id }}"})

    aggregate = PythonOperator(task_id="aggregate", python_callable=_aggregate,
                               op_kwargs={"day": DAY})

    train = PythonOperator(task_id="train", python_callable=_train, op_kwargs={"day": DAY})

    evaluate = PythonOperator(task_id="evaluate", python_callable=_evaluate)

    deploy_if_better = BranchPythonOperator(task_id="deploy_if_better",
                                            python_callable=_choose_branch)

    deploy_model = PythonOperator(task_id="deploy_model", python_callable=_deploy)

    skip_deploy = EmptyOperator(task_id="skip_deploy")

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    [wait_new_data, wait_streaming_done] >> extract >> validate_quality >> [aggregate, train]
    train >> evaluate >> deploy_if_better >> [deploy_model, skip_deploy]
    [aggregate, deploy_model, skip_deploy] >> end
