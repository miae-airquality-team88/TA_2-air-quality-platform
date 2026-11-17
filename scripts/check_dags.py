"""Vérifie que tous les DAGs Airflow se chargent sans erreur (AQ-25).

Exécuté par la CI dans l'image Airflow du projet. Échoue (code 1) si un DAG
contient une erreur d'import : un DAG cassé n'arrive jamais sur main.
"""
import sys

from airflow.models import DagBag

bag = DagBag(include_examples=False)   # dossier des DAGs défini par la configuration Airflow

if bag.import_errors:
    for path, error in bag.import_errors.items():
        print(f"ERREUR dans {path} :\n{error}")
    sys.exit(1)

print(f"{len(bag.dags)} DAG(s) chargé(s) sans erreur.")
for dag_id, dag in sorted(bag.dags.items()):
    print(f"  - {dag_id} : {len(dag.tasks)} tâches")
