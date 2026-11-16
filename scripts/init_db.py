"""Création des tables PostgreSQL du projet (AQ-12, étendu en AQ-25).

Exécute, dans l'ordre alphabétique, TOUS les fichiers sql/*.sql :
    sql/010_quality_checks.sql, sql/020_pipeline_heartbeats.sql, sql/init.sql…
Chaque membre ajoute ainsi ses tables dans son propre fichier, sans modifier
celui des autres (pas de conflit Git). Tous les fichiers sont idempotents
(CREATE ... IF NOT EXISTS) : le script peut être relancé sans risque.

Usage (depuis la racine du dépôt, PostgreSQL démarré) :
    python -m scripts.init_db
"""
from common.config import POSTGRES_DB, POSTGRES_HOST, POSTGRES_PORT, ROOT_DIR
from common.db import get_connection

SQL_DIR = ROOT_DIR / "sql"
CORE_TABLES = ["anomalies", "daily_aggregates", "measurements"]


def sql_files():
    """Fichiers SQL à exécuter, dans l'ordre alphabétique."""
    return sorted(SQL_DIR.glob("*.sql"))


def init_db(conn):
    """Exécute tous les fichiers sql/*.sql sur la connexion donnée."""
    with conn.cursor() as cur:
        for path in sql_files():
            cur.execute(path.read_text(encoding="utf-8"))
    conn.commit()


def existing_tables(conn):
    """Renvoie toutes les tables du schéma public, triées par nom."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_type = 'BASE TABLE' ORDER BY table_name"
        )
        return [row[0] for row in cur.fetchall()]


def main():
    print(f"Connexion à PostgreSQL : {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    conn = get_connection()
    try:
        init_db(conn)
        for path in sql_files():
            print(f"  ✓ {path.name} exécuté")
        for table in existing_tables(conn):
            print(f"  ✓ table {table}")
        print("Base prête.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
