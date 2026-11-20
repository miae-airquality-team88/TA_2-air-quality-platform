# Agrégation quotidienne — AQ-31

Point d'entrée pour AQ-27 :
`orchestration.aggregate.run_daily_aggregate(conn, day)`.
Passer la journée UTC terminée à traiter (date ISO ou objet `date`).
La fonction réutilise `streaming.storage.aggregate_day`, puis compare le nombre
de mesures brutes avec la somme de `measurement_count` des agrégats du jour.

Elle renvoie un dictionnaire sérialisable dans un XCom :
`{"day": "2026-11-16", "sensors": 2, "measurements": 3}`.
Un écart lève `AggregationError` pour faire échouer la tâche Airflow.
Les erreurs PostgreSQL sont propagées. La connexion reste à la charge de l'appelant.

Le batch existant valide les agrégats en base avant la réconciliation :
un échec signale donc une incohérence mais n'annule pas les écritures déjà validées.
Lancer cette tâche après le sensor de fin du streaming (AQ-34), afin que des
insertions concurrentes ne changent pas le volume entre les deux opérations.
Relancer sur les mêmes données conserve les mêmes comptes ; l'arrivée de nouvelles
mesures est prise en compte par recalcul, sans ajouter un deuxième agrégat par capteur.
Les capteurs n'ayant que des anomalies sont également conservés.

Tests : `pytest -q tests/test_aggregate.py`. Sept cas utilisent PostgreSQL :
réconciliation, rejeu, journée vide, incohérence détectée, anomalies seules,
frontières UTC et recalcul après arrivée d'une mesure. Un cas rejette une date invalide.
Le raccordement final au DAG quotidien est réalisé dans AQ-27.
