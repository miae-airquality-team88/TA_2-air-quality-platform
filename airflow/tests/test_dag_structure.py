"""Tests de structure des DAGs (AQ-42).

Exécutés dans l'image Airflow par la CI (contrôle « DAGs Airflow »). Ils vérifient
que l'ENCHAÎNEMENT des tâches et les réglages de chaque DAG sont ceux prévus :
une modification qui casse le pipeline (tâche supprimée, dépendance oubliée,
rattrapage activé par erreur…) fait échouer la CI avant d'arriver sur main.
"""
import pytest
from airflow.models import DagBag


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(include_examples=False)


def upstream(dag, task_id):
    return set(dag.get_task(task_id).upstream_task_ids)


def downstream(dag, task_id):
    return set(dag.get_task(task_id).downstream_task_ids)


def test_aucune_erreur_d_import(dagbag):
    assert dagbag.import_errors == {}


def test_les_trois_dags_existent(dagbag):
    assert {"air_quality_daily", "air_quality_backfill", "streaming_monitor"} <= set(dagbag.dags)


@pytest.mark.parametrize("dag_id", ["air_quality_daily", "air_quality_backfill",
                                    "streaming_monitor"])
def test_reglages_communs(dagbag, dag_id):
    dag = dagbag.get_dag(dag_id)
    for task in dag.tasks:
        assert task.retries >= 1, f"{task.task_id} : aucune nouvelle tentative"
        assert task.on_failure_callback, f"{task.task_id} : pas d'alerte en cas d'échec"
    assert dag.tags, "chaque DAG doit avoir des étiquettes"


def test_dag_quotidien(dagbag):
    dag = dagbag.get_dag("air_quality_daily")
    assert dag.schedule_interval == "@daily"
    assert dag.catchup is False and dag.max_active_runs == 1
    assert {t.task_id for t in dag.tasks} == {
        "wait_new_data", "wait_streaming_done", "extract", "validate_quality", "aggregate",
        "train", "evaluate", "deploy_if_better", "deploy_model", "skip_deploy", "end"}
    assert upstream(dag, "extract") == {"wait_new_data", "wait_streaming_done"}
    assert upstream(dag, "validate_quality") == {"extract"}
    assert downstream(dag, "validate_quality") == {"aggregate", "train"}
    assert upstream(dag, "deploy_if_better") == {"evaluate"}
    assert downstream(dag, "deploy_if_better") == {"deploy_model", "skip_deploy"}
    assert dag.get_task("end").trigger_rule == "none_failed_min_one_success"
    for sensor in ["wait_new_data", "wait_streaming_done"]:
        assert dag.get_task(sensor).mode == "reschedule"


def test_dag_backfill(dagbag):
    dag = dagbag.get_dag("air_quality_backfill")
    assert dag.catchup is True
    assert dag.start_date is not None and dag.end_date is not None
    assert dag.start_date < dag.end_date
    assert dag.max_active_runs == 2
    assert upstream(dag, "validate_quality") == {"extract"}
    assert upstream(dag, "aggregate") == {"validate_quality"}
    assert {t.task_id for t in dag.tasks} == {"extract", "validate_quality", "aggregate"}


def test_dag_surveillance(dagbag):
    dag = dagbag.get_dag("streaming_monitor")
    assert dag.schedule_interval == "*/15 * * * *"
    assert dag.catchup is False
    assert [t.task_id for t in dag.tasks] == ["check_streaming"]
