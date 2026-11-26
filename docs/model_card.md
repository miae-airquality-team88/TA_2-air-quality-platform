# Fiche descriptive du modèle (model card)

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
| Déployé le | 2026-11-26T22:44:45Z |
| Dossier du modèle | `D:\projet Data ML OPS\TA_2 Air Quality Platform\models\candidates\2026-11-22_224220` |

## 3. Données d'entraînement
| Élément | Valeur |
|---|---|
| Source | extractions quotidiennes du DAG (`data/extracts/`), mesures du simulateur uniquement |
| Fichiers utilisés (lignée) | `measurements_2026-11-22.csv` |
| Période | du 2026-11-22 au 2026-11-22 (1 jour(s)) |
| Mesures d'entraînement / de test | 1080 / 360 (découpage 75 % / 25 %, stratifié) |
| Taux d'anomalies | 4.9% |
| Étiquette | `injected_anomaly` non vide : anomalie injectée par le simulateur (vérité terrain) |

**Variables (18)** : `pm2_5`, `pm2_5_absent`, `pm10`, `pm10_absent`, `no2`, `no2_absent`, `o3`, `o3_absent`, `temperature`, `temperature_absent`, `humidity`, `humidity_absent`, `pm2_5_mean_5min`, `pm2_5_mean_15min`, `pm10_mean_5min`, `pm10_mean_15min`, `issue_count`, `pm2_5_ratio_15min`

## 4. Performances (jeu de test, jamais vu à l'entraînement)
| Métrique | Valeur | Lecture |
|---|---|---|
| **F1** (métrique de décision) | **0.8** | équilibre entre précision et rappel |
| Précision | 0.9231 | part des alertes du modèle qui sont de vraies anomalies |
| Rappel | 0.7059 | part des vraies anomalies retrouvées par le modèle |
| Exactitude | 0.9833 | peu informative : les anomalies sont rares |
| ROC AUC | 0.9231 | 1 = classement parfait, 0,5 = hasard |

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
