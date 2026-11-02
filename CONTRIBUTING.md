# Guide de contribution

Ce guide décrit comment l'équipe travaille sur **air-quality-platform**. Il s'applique à tous les membres.

## 1. Un ticket Jira = une branche = une Pull Request

Tout travail commence par un ticket Jira (`AQ-XX`) assigné à un membre. Pas de ticket, pas de code.

1. Passer le ticket à **En cours** dans Jira.
2. Partir d'un `main` à jour :
   ```bash
   git checkout main
   git pull
   git checkout -b feature/AQ-XX-description-courte
   ```
3. Coder, tester, committer (voir conventions ci-dessous).
4. Pousser et ouvrir une Pull Request vers `main`.
5. Après approbation et merge : supprimer la branche, passer le ticket à **Terminé**.

## 2. Nommage des branches

| Préfixe | Usage | Exemple |
|---|---|---|
| `feature/` | nouvelle fonctionnalité | `feature/AQ-9-simulateur-iot` |
| `fix/` | correction de bug | `fix/AQ-31-timestamp-utc` |
| `docs/` | documentation seule | `docs/AQ-40-rapport-architecture` |
| `chore/` | configuration, outillage | `chore/AQ-7-ci-github-actions` |

La clé Jira (`AQ-XX`) est **obligatoire** : c'est elle qui relie la branche au ticket dans Jira.

## 3. Messages de commit (Conventional Commits)

Format : `type(périmètre): AQ-XX description à l'infinitif ou au présent`

| Type | Usage |
|---|---|
| `feat` | nouvelle fonctionnalité |
| `fix` | correction |
| `docs` | documentation |
| `test` | ajout ou modification de tests |
| `refactor` | réorganisation sans changement de comportement |
| `chore` | configuration, dépendances, CI |

Périmètres usuels : `infra`, `ingestion`, `streaming`, `storage`, `airflow`, `ml`, `api`, `catalog`, `dashboard`.

Exemples :
```
feat(ingestion): AQ-9 simulateur de capteurs avec injection d'anomalies
test(ingestion): AQ-9 tests de conformité au contrat de données
fix(storage): AQ-12 index manquant sur la colonne timestamp
```

Faire des commits **petits et fréquents** plutôt qu'un gros commit en fin de semaine.

## 4. Pull Requests

- Titre : `AQ-XX Description du ticket`
- Remplir le modèle de PR (description, tests effectués).
- **Au moins 1 approbation** d'un autre membre est obligatoire (`main` est protégée).
- Le relecteur vérifie : le code fonctionne, il respecte le contrat de données, aucun secret n'est commité.
- Les commentaires de relecture doivent être résolus avant le merge.

## 5. Règles de code

- Python 3.11, style vérifié par `ruff` (`ruff check .`).
- Tests avec `pytest` dans le dossier `tests/` (`pytest -q`).
- **Aucune adresse, aucun mot de passe en dur** : tout passe par `common/config.py`, qui lit le fichier `.env`.
- Le fichier `.env` n'est **jamais** commité ; seules les valeurs d'exemple vont dans `.env.example`.
- Les messages Kafka respectent le contrat `schemas/sensor_message.json`.
- Les données locales vont dans `data/` (ignoré par Git).

## 6. Environnement de développement

```bash
cp .env.example .env              # puis adapter les valeurs
docker compose up -d              # Kafka, PostgreSQL, Kafka UI
conda create -n airquality python=3.11 -y
conda activate airquality
pip install -r requirements.txt
```

## 7. Rituels de l'équipe

- Sprints d'une semaine (lundi → dimanche), suivis dans Jira.
- Point d'avancement court en milieu de sprint.
- Revue + planification du sprint suivant en fin de sprint ; compte rendu dans `docs/reunions/`.
