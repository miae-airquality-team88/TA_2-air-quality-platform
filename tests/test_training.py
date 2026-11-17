"""Tests de l'entraînement et de l'évaluation (AQ-32), sur des données simulées."""
import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from ingestion.simulator import make_sensors, stream
from orchestration.training import (
    NotEnoughDataError,
    build_features,
    evaluate,
    extract_files,
    train,
)
from streaming.consumer import StreamPipeline
from streaming.storage import MEASUREMENT_COLUMNS, processed_to_row

DAY = date(2026, 11, 16)


def fake_extract(tmp_path, day=DAY, count=80, anomaly_rate=0.15, seed=1):
    """Fabrique une extraction comme la tâche Extract (simulateur -> pipeline -> CSV)."""
    pipeline, rows = StreamPipeline(), []
    for message in stream(
    sensors=make_sensors(n=5),
    interval=0,
    count=count,
    anomaly_rate=anomaly_rate,
    seed=seed,):
        processed, _ = pipeline.process(json.dumps(message).encode())
        rows.append(processed_to_row(processed))
    path = Path(tmp_path) / "extracts" / f"measurements_{day}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=MEASUREMENT_COLUMNS).to_csv(path, index=False)
    return path


def test_fichiers_de_la_fenetre(tmp_path):
    fake_extract(tmp_path, day=DAY)
    fake_extract(tmp_path, day=date(2026, 11, 1))   # hors fenêtre de 7 jours
    files = extract_files(Path(tmp_path) / "extracts", DAY)
    assert [f.name for f in files] == ["measurements_2026-11-16.csv"]


def test_variables_et_etiquette():
    df = pd.DataFrame({"pm2_5": [15.0, None], "pm2_5_mean_15min": [15.0, 15.0],
                       "issue_count": [0, 1], "injected_anomaly": [None, "missing_values"],
                       "source": ["simulator", "simulator"]})
    X, y = build_features(df)
    assert list(y) == [0, 1]
    assert list(X["pm2_5_absent"]) == [0, 1]
    assert X.loc[0, "pm2_5_ratio_15min"] == 1.0
    assert X.isna().sum().sum() == 0


def test_entrainement_puis_evaluation(tmp_path):
    fake_extract(tmp_path)
    info = train(Path(tmp_path) / "extracts", Path(tmp_path) / "models", DAY)
    assert Path(info["model_path"]).exists() and info["train_rows"] == 300
    metrics = evaluate(info["model_path"], info["test_path"])
    assert metrics["test_rows"] == 100
    assert metrics["f1"] >= 0.7, metrics          # le modèle apprend réellement
    assert (Path(info["run_dir"]) / "metrics.json").exists()


def test_meme_donnees_meme_resultat(tmp_path):
    fake_extract(tmp_path)
    a = train(Path(tmp_path) / "extracts", Path(tmp_path) / "m1", DAY)
    b = train(Path(tmp_path) / "extracts", Path(tmp_path) / "m2", DAY)
    assert evaluate(a["model_path"], a["test_path"])["f1"] == \
        evaluate(b["model_path"], b["test_path"])["f1"]


def test_pas_assez_de_donnees(tmp_path):
    fake_extract(tmp_path, count=4)
    with pytest.raises(NotEnoughDataError):
        train(Path(tmp_path) / "extracts", Path(tmp_path) / "models", DAY)


def test_aucune_extraction(tmp_path):
    with pytest.raises(NotEnoughDataError):
        train(Path(tmp_path) / "extracts", Path(tmp_path) / "models", DAY)
