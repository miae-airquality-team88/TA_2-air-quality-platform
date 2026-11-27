"""Génère la fiche descriptive du modèle en production : docs/model_card.md (AQ-40).

Une « model card » documente un modèle de ML : à quoi il sert, sur quelles
données il a été entraîné, comment il se comporte, et ses LIMITES. Les chiffres
viennent directement des fichiers produits par le DAG (models/production/
metrics.json et le training_info.json du modèle) : la fiche est donc toujours
fidèle au modèle réellement déployé. On la régénère après chaque déploiement.

Usage (depuis la racine du dépôt, après au moins un déploiement par le DAG) :
    python -m scripts.model_card
"""
import argparse
import json
from pathlib import Path

from common.config import MODELS_DIR, ROOT_DIR

OUTPUT = ROOT_DIR / "docs" / "model_card.md"

TEMPLATE = """# Fiche descriptive du modèle (model card)

> Générée automatiquement par `python -m scripts.model_card` à partir du modèle en production.
> Ne pas modifier les chiffres à la main : relancer le script après chaque déploiement.

## 1. Objectif
Détecter automatiquement les **mesures anormales** des capteurs de qualité de l'air
(problème n°1 du sujet : classification binaire « anomalie / normal », supervisée).
Le modèle complète les règles métier du streaming (Sprint 2) ; il est réentraîné chaque jour
par le DAG `air_quality_daily` et n'est déployé que s'il est meilleur que le modèle actuel.

## 2. Modèle déployé
| Élément | Valeur |
|---|---|
| Algorithme | Forêt aléatoire (`RandomForestClassifier`, scikit-learn) |
| Hyperparamètres | 100 arbres, `class_weight="balanced"`, `random_state=42` |
| Déployé le | {deployed_at} |
| DAG Airflow | `air_quality_daily` |
| Run ID Airflow | `{airflow_run_id}` |
| Dossier du modèle | `{source_dir}` |

## 3. Données d'entraînement
| Élément | Valeur |
|---|---|
| Source | extractions quotidiennes du DAG (`data/extracts/`), mesures du simulateur uniquement |
| Fichiers utilisés (lignée) | {training_files} |
| Période | {period} |
| Mesures d'entraînement / de test | {train_rows} / {test_rows} (découpage 75 % / 25 %, stratifié) |
| Taux d'anomalies | {anomaly_rate} |
| Étiquette | `injected_anomaly` non vide : anomalie injectée par le simulateur (vérité terrain) |

**Variables ({n_features})** : {features}

## 4. Performances (jeu de test, jamais vu à l'entraînement)
| Métrique | Valeur | Lecture |
|---|---|---|
| **F1** (métrique de décision) | **{f1}** | équilibre entre précision et rappel |
| Précision | {precision} | part des alertes du modèle qui sont de vraies anomalies |
| Rappel | {recall} | part des vraies anomalies retrouvées par le modèle |
| Exactitude | {accuracy} | peu informative : les anomalies sont rares |
| ROC AUC | {roc_auc} | 1 = classement parfait, 0,5 = hasard |

## 5. Limites connues
- **Capteur bloqué non détecté** : le modèle examine chaque mesure seule, sans la comparer à la
  précédente ; une mesure « bloquée » ressemble à une mesure normale. La règle métier
  `capteur_bloque` du streaming reste nécessaire.
- **Données simulées** : les étiquettes viennent du simulateur. Les performances sur des
  anomalies réelles (OpenAQ) ne sont pas mesurées, faute d'étiquettes.
- **Valeurs absentes** : remplacées par -1 avec un indicateur ; le modèle s'appuie fortement sur
  ces indicateurs, déjà couverts par la validation.

## 6. Pistes d'amélioration (Phase 3)
- Variables temporelles : écart avec la mesure précédente du capteur, tendance sur 1 h.
- Comparer plusieurs modèles (régression logistique, XGBoost) et suivre les essais dans **MLflow**.
- Remplacer le dossier `models/production/` par le **MLflow Model Registry**
  (transition Staging → Production) et piloter l'entraînement par un opérateur Airflow dédié.
"""


def _period(files):
    days = sorted(f.replace("measurements_", "").replace(".csv", "") for f in files)
    return f"du {days[0]} au {days[-1]} ({len(days)} jour(s))" if days else "-"


def build_card(models_dir=MODELS_DIR, run_id=None):
    """Construit le texte de la fiche à partir des fichiers du modèle en production."""
    production = Path(models_dir) / "production" / "metrics.json"
    if not production.exists():
        raise SystemExit("Aucun modèle en production : lancez d'abord le DAG air_quality_daily.")
    metrics = json.loads(production.read_text(encoding="utf-8"))

    source_dir = str(metrics.get("source_dir") or "")
    info_path = Path(source_dir) / "training_info.json"

    # Si le chemin du conteneur Docker n'existe pas sur l'hote,
    # retrouver le meme candidat dans le repertoire local models_dir.
    if not info_path.is_file() and source_dir:
        parts = source_dir.replace("\\", "/").rstrip("/").split("/")

        if len(parts) >= 2 and parts[-2] == "candidates":
            candidate_name = parts[-1]
            info_path = (
                Path(models_dir)
                / "candidates"
                / candidate_name
                / "training_info.json"
            )

    if not info_path.is_file():
        raise FileNotFoundError(
            f"Metadonnees d'entrainement introuvables : {info_path}"
        )

    info = json.loads(info_path.read_text(encoding="utf-8"))

    files = info.get("training_files", [])
    return TEMPLATE.format(
        airflow_run_id=run_id or "Non renseigné",
        deployed_at=metrics.get("deployed_at", "-"),
        source_dir=metrics.get("source_dir", "-"),
        training_files=", ".join(f"`{f}`" for f in files) or "-",
        period=_period(files),
        train_rows=info.get("train_rows", "-"),
        test_rows=info.get("test_rows", metrics.get("test_rows", "-")),
        anomaly_rate=f"{info['anomaly_rate']:.1%}" if "anomaly_rate" in info else "-",
        n_features=len(info.get("features", [])),
        features=", ".join(f"`{f}`" for f in info.get("features", [])) or "-",
        **{k: metrics.get(k, "-") for k in ["f1", "precision", "recall", "accuracy", "roc_auc"]},
    )



def main():
    parser = argparse.ArgumentParser(
        description="Generer la Model Card du modele en production"
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Identifiant du run Airflow ayant deploye le modele",
    )
    args = parser.parse_args()

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(build_card(run_id=args.run_id), encoding="utf-8")
    print(f"Fiche écrite dans {OUTPUT}")


if __name__ == "__main__":
    main()
