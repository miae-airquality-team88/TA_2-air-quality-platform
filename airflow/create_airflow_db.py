"""Crée la base « airflow » dans PostgreSQL si elle n'existe pas (AQ-25).

Airflow a besoin de sa propre base pour ses métadonnées (DAGs, exécutions,
historique des tâches). On la place dans le même serveur PostgreSQL que le
projet, mais dans une base séparée : les données du projet restent à part.
"""
import os
import time

import psycopg2

params = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": int(os.environ.get("POSTGRES_PORT", "5432")),
    "user": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
    "dbname": os.environ.get("POSTGRES_DB", "airquality"),
}
AIRFLOW_DB = "airflow"

for attempt in range(30):  # attend que PostgreSQL soit prêt
    try:
        conn = psycopg2.connect(**params)
        break
    except psycopg2.OperationalError:
        print("PostgreSQL pas encore prêt, nouvelle tentative dans 2 s…")
        time.sleep(2)
else:
    raise SystemExit("Impossible de joindre PostgreSQL.")

conn.autocommit = True  # CREATE DATABASE interdit dans une transaction
with conn.cursor() as cur:
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (AIRFLOW_DB,))
    if cur.fetchone() is None:
        cur.execute(f'CREATE DATABASE "{AIRFLOW_DB}"')
        print(f"Base '{AIRFLOW_DB}' créée.")
    else:
        print(f"Base '{AIRFLOW_DB}' déjà présente.")
conn.close()
