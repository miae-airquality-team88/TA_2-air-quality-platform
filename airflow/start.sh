#!/usr/bin/env bash
# =============================================================
#  Démarrage d'Airflow en un seul conteneur (AQ-25)
#  Scheduler + interface web dans le même conteneur : moins de mémoire
#  qu'avec un conteneur par composant (adapté à un PC de développement).
# =============================================================
set -euo pipefail

python /opt/aq/create_airflow_db.py        # 1. base des métadonnées
airflow db migrate                         # 2. tables internes d'Airflow
airflow users create \
    --username admin \
    --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" \
    --firstname Admin --lastname AirQuality \
    --role Admin --email admin@example.org || true   # 3. compte admin (ignoré s'il existe)

airflow scheduler &                        # 4. planificateur, en arrière-plan
exec airflow webserver                     # 5. interface web sur le port 8080
