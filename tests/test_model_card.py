"""Tests de la génération de la fiche du modèle (AQ-40)."""
import json

import pytest

from scripts.model_card import build_card


def fake_production(tmp_path):
    run_dir = tmp_path / "candidates" / "2026-11-20_101500"
    run_dir.mkdir(parents=True)
    (run_dir / "training_info.json").write_text(json.dumps({
        "train_rows": 1173, "test_rows": 392, "anomaly_rate": 0.07,
        "features": ["pm2_5", "pm10"],
        "training_files": ["measurements_2026-11-19.csv", "measurements_2026-11-20.csv"],
    }))
    prod = tmp_path / "production"
    prod.mkdir()
    (prod / "metrics.json").write_text(json.dumps({
        "f1": 0.7727, "precision": 1.0, "recall": 0.6296, "accuracy": 0.97, "roc_auc": 0.8513,
        "deployed_at": "2026-11-20T10:15:00Z", "source_dir": str(run_dir),
    }))


def test_fiche_contient_les_vrais_chiffres(tmp_path):
    fake_production(tmp_path)
    card = build_card(tmp_path)
    assert "**0.7727**" in card and "| Rappel | 0.6296 |" in card
    assert "du 2026-11-19 au 2026-11-20 (2 jour(s))" in card
    assert "1173 / 392" in card and "7.0%" in card
    assert "Capteur bloqué non détecté" in card


def test_sans_modele_en_production(tmp_path):
    with pytest.raises(SystemExit):
        build_card(tmp_path)
