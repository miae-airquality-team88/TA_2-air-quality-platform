"""Création des tables PostgreSQL du projet (AQ-12).

Exécute sql/init.sql. Le script est idempotent : le relancer ne change rien
si les tables existent déjà.

Usage (depuis la racine du dépôt, PostgreSQL démarré) :
    python -m scripts.init_db
"""
from common.config import POSTGRES_DB, POSTGRES_HOST, POSTGRES_PORT, ROOT_DIR
from common.db import get_connection

INIT_SQL = ROOT_DIR / "sql" / "init.sql"
TABLES = ["measurements", "anomalies", "daily_aggregates"]


def init_db(conn):
    """Exécute sql/init.sql sur la connexion donnée."""
    with conn.cursor() as cur:
        cur.execute(INIT_SQL.read_text(encoding="utf-8"))
    conn.commit()


def existing_tables(conn):
    """Renvoie les tables du projet présentes dans la base."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = ANY(%s) ORDER BY table_name",
            (TABLES,),
        )
        return [row[0] for row in cur.fetchall()]


def main():
    print(f"Connexion à PostgreSQL : {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    conn = get_connection()
    try:
        init_db(conn)
        for table in existing_tables(conn):
            print(f"  ✓ table {table}")
        print("Base prête.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
