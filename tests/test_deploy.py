"""Tests du déploiement conditionnel (AQ-28)."""
import csv
import json

from orchestration.deploy import (
    DEPLOY_TASK,
    SKIP_TASK,
    choose_branch,
    is_better,
    load_production_metrics,
    promote,
)


def candidate(tmp_path, name, f1):
    """Crée un faux modèle candidat avec son fichier de métriques."""
    run_dir = tmp_path / "candidates" / name
    run_dir.mkdir(parents=True)
    (run_dir / "model.joblib").write_bytes(b"faux modele " + name.encode())
    metrics = {"f1": f1, "model_path": str(run_dir / "model.joblib")}
    (run_dir / "metrics.json").write_text(json.dumps(metrics))
    return metrics


def test_premier_modele_toujours_deploye():
    assert is_better({"f1": 0.5}, None)[0] is True


def test_comparaison_f1():
    assert is_better({"f1": 0.85}, {"f1": 0.80})[0] is True
    assert is_better({"f1": 0.80}, {"f1": 0.80})[0] is False   # égalité : on garde l'actuel
    assert is_better({"f1": 0.79}, {"f1": 0.80})[0] is False
    assert is_better({"f1": 0.805}, {"f1": 0.80}, min_improvement=0.01)[0] is False


def test_scenario_complet(tmp_path):
    # Jour 1 : aucun modèle en production -> déploiement
    c1 = candidate(tmp_path, "jour1", 0.80)
    assert choose_branch(c1, tmp_path) == DEPLOY_TASK
    promote(c1, tmp_path)
    assert load_production_metrics(tmp_path)["f1"] == 0.80

    # Jour 2 : candidat moins bon -> on garde le modèle du jour 1
    c2 = candidate(tmp_path, "jour2", 0.75)
    assert choose_branch(c2, tmp_path) == SKIP_TASK

    # Jour 3 : candidat meilleur -> déploiement
    c3 = candidate(tmp_path, "jour3", 0.90)
    assert choose_branch(c3, tmp_path) == DEPLOY_TASK
    promote(c3, tmp_path)
    assert (tmp_path / "production" / "model.joblib").read_bytes() == b"faux modele jour3"

    with (tmp_path / "deployments.csv").open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert [(r["f1"], r["previous_f1"]) for r in rows] == [("0.8", ""), ("0.9", "0.8")]
