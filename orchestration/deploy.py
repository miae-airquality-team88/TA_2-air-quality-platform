"""Déploiement conditionnel du modèle : « deploy if better » (AQ-28).

Après l'évaluation, le DAG compare le modèle CANDIDAT (qui vient d'être
entraîné) au modèle EN PRODUCTION :
    - pas encore de modèle en production  -> on déploie le candidat ;
    - candidat meilleur (F1 plus élevé)   -> on déploie ;
    - sinon                               -> on garde le modèle actuel.

« Déployer » = copier le modèle et ses métriques dans models/production/ et
noter la décision dans models/deployments.csv (historique). Au Sprint 5,
MLflow Model Registry remplacera ce dossier (transition Staging -> Production).

Dans le DAG, choose_branch() est appelée par un BranchPythonOperator : elle
renvoie le NOM de la tâche suivante à exécuter.
"""
import csv
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

PRIMARY_METRIC = "f1"      # métrique de décision
MIN_IMPROVEMENT = 0.0      # amélioration minimale exigée (0.01 = +1 point de F1)

DEPLOY_TASK = "deploy_model"
SKIP_TASK = "skip_deploy"


def production_dir(models_dir):
    return Path(models_dir) / "production"


def load_production_metrics(models_dir):
    """Métriques du modèle en production, ou None s'il n'y en a pas encore."""
    path = production_dir(models_dir) / "metrics.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def is_better(candidate, production, metric=PRIMARY_METRIC, min_improvement=MIN_IMPROVEMENT):
    """Compare candidat et production. Renvoie (décision, explication)."""
    if production is None:
        return True, "aucun modèle en production : le candidat est déployé"
    new, old = candidate[metric], production[metric]
    if new > old + min_improvement:
        return True, f"{metric} {new} > {old} (production) : candidat meilleur"
    return False, f"{metric} {new} <= {old} (production) : on garde le modèle actuel"


def choose_branch(candidate_metrics, models_dir):
    """Appelée par le BranchPythonOperator : renvoie la tâche suivante."""
    better, reason = is_better(candidate_metrics, load_production_metrics(models_dir))
    print(f"Décision : {reason}")
    return DEPLOY_TASK if better else SKIP_TASK


def promote(candidate_metrics, models_dir):
    """Met le candidat en production et l'ajoute à l'historique des déploiements."""
    previous = load_production_metrics(models_dir)
    source = Path(candidate_metrics["model_path"]).parent
    target = production_dir(models_dir)
    target.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source / "model.joblib", target / "model.joblib")
    metrics = dict(candidate_metrics, deployed_at=datetime.now(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"), source_dir=str(source))
    (target / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    history = Path(models_dir) / "deployments.csv"
    new_file = not history.exists()
    with history.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if new_file:
            writer.writerow(["deployed_at", "source_dir", PRIMARY_METRIC,
                             f"previous_{PRIMARY_METRIC}"])
        writer.writerow([metrics["deployed_at"], str(source), metrics[PRIMARY_METRIC],
                         previous[PRIMARY_METRIC] if previous else ""])
    print(f"Modèle déployé : {PRIMARY_METRIC} = {metrics[PRIMARY_METRIC]}")
    return str(target / "model.joblib")
