# Contrôles qualité quotidiens — AQ-29

Après récupération de cette branche, exécuter `python -m scripts.init_db`
pour créer `quality_checks` sans supprimer les tables existantes.

Le point d'entrée pour AQ-27 est
`validate_day(conn, day, run_id=...)`, dans `orchestration.quality`.
`day` est une date ISO ou une `date` représentant la journée UTC à traiter
(la veille pour une exécution quotidienne). La connexion appartient à l'appelant.
La fonction renvoie trois dictionnaires sérialisables pour les XCom.

- Volume : au moins `QUALITY_MIN_ROWS` mesures de cette journée (100 par défaut).
- Complétude : proportion de mesures `is_valid`, au moins
  `QUALITY_MIN_COMPLETENESS` (0.8 par défaut). Les seuils sont comparés sans arrondi.
- Fraîcheur : âge de la dernière mesure de toute la base, entre zéro et
  `QUALITY_MAX_AGE_HOURS` (26 par défaut). Ce contrôle décrit l'activité globale
  du flux ; volume et complétude restent propres à la journée demandée.
  Pour une journée strictement antérieure à aujourd'hui UTC moins deux jours,
  la fraîcheur est non applicable (valeur NULL, résultat réussi, détail explicite).

Les variables sont lues depuis l'environnement ou le `.env` par `common.config`.
Le service Airflow les transmet déjà dans `docker-compose.yml`.

Les trois résultats sont insérés et validés en base avant de lever
`QualityCheckError` si un contrôle échoue. Une nouvelle tentative ajoute un
nouveau lot de résultats ; `run_id` et `checked_at` permettent de les retrouver.
Un problème d'accès à PostgreSQL reste une erreur et ne devient pas un succès.
Utiliser une connexion dédiée à cette tâche : l'enregistrement valide sa transaction.

Tests : `pytest -q tests/test_quality.py`. Les tests utilisant `db_conn`
nécessitent PostgreSQL et utilisent uniquement la base de test du projet.
L'enchaînement dans le DAG est fourni séparément par AQ-27.
