# =============================================================
#  Image Airflow du projet (AQ-25)
#  Image officielle Airflow 2.10 + les bibliothèques dont nos tâches ont besoin.
# =============================================================
FROM apache/airflow:2.10.5-python3.11

# On réinstalle la même version d'Airflow en même temps : pip garde ainsi
# Airflow intact pendant qu'il ajoute nos bibliothèques.
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir "apache-airflow==2.10.5" -r /tmp/requirements-airflow.txt

# Scripts de démarrage
COPY start.sh create_airflow_db.py /opt/aq/
