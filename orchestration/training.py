"""Tâches « Train » et « Evaluate » du DAG quotidien (AQ-32).

Modèle de référence (baseline) pour le problème n°1 du sujet :
CLASSIFICATION BINAIRE « anomalie / normal ».

- Données : les extractions quotidiennes produites par la tâche Extract
  (data/extracts/measurements_AAAA-MM-JJ.csv), sur les 7 derniers jours.
  On garde les mesures du simulateur, les seules à avoir une étiquette.
- Étiquette (y) : 1 si le simulateur avait injecté une anomalie
  (colonne injected_anomaly non vide), 0 sinon : c'est la vérité terrain.
- Variables (X) : les 6 mesures, les moyennes glissantes, le nombre de
  problèmes de qualité, des indicateurs « mesure absente » et le rapport
  PM2.5 / moyenne 15 min.
- Modèle : forêt aléatoire (RandomForestClassifier), classes équilibrées.
- Évaluation : 25 % des données gardées de côté (jamais vues à l'entraînement) ;
  précision, rappel, F1, exactitude, aire sous la courbe ROC.

Au Sprint 5, MLflow remplacera l'enregistrement local des modèles et métriques.
"""
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import joblib
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score
from sklearn.model_selection import train_test_split

MEASURES = ["pm2_5", "pm10", "no2", "o3", "temperature", "humidity"]
WINDOW_FEATURES = ["pm2_5_mean_5min", "pm2_5_mean_15min", "pm10_mean_5min", "pm10_mean_15min"]
WINDOW_DAYS = 7
TEST_SIZE = 0.25
RANDOM_STATE = 42          # graine : même données -> même modèle (reproductibilité)
MIN_ROWS = 50
MIN_POSITIVES = 5


class NotEnoughDataError(Exception):
    """Pas assez de données étiquetées pour entraîner un modèle fiable."""


def extract_files(extracts_dir, day, window_days=WINDOW_DAYS):
    """Fichiers d'extraction disponibles sur les `window_days` jours se terminant à `day`."""
    files = []
    for offset in range(window_days):
        path = Path(extracts_dir) / f"measurements_{day - timedelta(days=offset)}.csv"
        if path.exists():
            files.append(path)
    return sorted(files)


def load_training_data(files):
    """Concatène les extractions et garde les mesures étiquetées (simulateur)."""
    if not files:
        return pd.DataFrame()
    df = pd.concat((pd.read_csv(f) for f in files), ignore_index=True)
    return df[df["source"] == "simulator"].reset_index(drop=True)


def _column(df, name):
    """Colonne numérique du DataFrame (valeurs vides si la colonne n'existe pas)."""
    if name not in df:
        return pd.Series(float("nan"), index=df.index)
    return pd.to_numeric(df[name], errors="coerce")


def build_features(df):
    """Construit X (variables) et y (étiquette) à partir des mesures."""
    X = pd.DataFrame(index=df.index)
    for col in MEASURES + WINDOW_FEATURES:
        X[col] = _column(df, col)
        if col in MEASURES:
            X[f"{col}_absent"] = X[col].isna().astype(int)   # mesure manquante ou invalide
    X["issue_count"] = _column(df, "issue_count").fillna(0)
    mean15 = X["pm2_5_mean_15min"].where(X["pm2_5_mean_15min"] > 0)
    X["pm2_5_ratio_15min"] = X["pm2_5"] / mean15     # 1 = normal, 5 = pic
    X = X.fillna(-1)                                 # valeur conventionnelle pour « absent »
    y = df["injected_anomaly"].notna().astype(int)
    return X, y


def train(extracts_dir, models_dir, day):
    """Entraîne un modèle candidat. Renvoie un dict (chemins + infos) pour la tâche Evaluate."""
    if isinstance(day, str):
        day = date.fromisoformat(day)
    files = extract_files(extracts_dir, day)
    df = load_training_data(files)
    if len(df) < MIN_ROWS:
        raise NotEnoughDataError(f"{len(df)} mesures étiquetées (minimum {MIN_ROWS})")
    X, y = build_features(df)
    if y.sum() < MIN_POSITIVES or (len(y) - y.sum()) < MIN_POSITIVES:
        raise NotEnoughDataError(f"{int(y.sum())} anomalies sur {len(y)} mesures : trop peu "
                                 f"pour apprendre (minimum {MIN_POSITIVES} de chaque classe)")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE, stratify=y)
    model = RandomForestClassifier(n_estimators=100, class_weight="balanced",
                                   random_state=RANDOM_STATE, n_jobs=-1)
    model.fit(X_train, y_train)

    run_dir = Path(models_dir) / "candidates" / f"{day}_{datetime.now(timezone.utc):%H%M%S}"
    run_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, run_dir / "model.joblib")
    X_test.assign(label=y_test.values).to_csv(run_dir / "test_set.csv", index=False)

    info = {
        "day": day.isoformat(),
        "run_dir": str(run_dir),
        "model_path": str(run_dir / "model.joblib"),
        "test_path": str(run_dir / "test_set.csv"),
        "train_rows": len(X_train),
        "test_rows": len(X_test),
        "anomaly_rate": round(float(y.mean()), 4),
        "features": list(X.columns),
        "training_files": [Path(f).name for f in files],   # lignée : sur quelles données ?
    }
    (run_dir / "training_info.json").write_text(json.dumps(info, indent=2), encoding="utf-8")
    print(f"Modèle entraîné sur {len(X_train)} mesures ({len(files)} fichier(s)), "
          f"taux d'anomalies {info['anomaly_rate']:.1%}")
    return info


def evaluate(model_path, test_path):
    """Évalue un modèle sur son jeu de test. Renvoie et enregistre les métriques."""
    model = joblib.load(model_path)
    test = pd.read_csv(test_path)
    y_true = test.pop("label")
    y_pred = model.predict(test)
    y_score = model.predict_proba(test)[:, 1]
    metrics = {
        "f1": round(float(f1_score(y_true, y_pred)), 4),
        "precision": round(float(precision_score(y_true, y_pred, zero_division=0)), 4),
        "recall": round(float(recall_score(y_true, y_pred)), 4),
        "accuracy": round(float(accuracy_score(y_true, y_pred)), 4),
        "roc_auc": round(float(roc_auc_score(y_true, y_score)), 4),
        "test_rows": int(len(y_true)),
        "model_path": str(model_path),
        "evaluated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    metrics_path = Path(model_path).parent / "metrics.json"
    metrics_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    print("Métriques :", {k: metrics[k] for k in ["f1", "precision", "recall", "roc_auc"]})
    return metrics
