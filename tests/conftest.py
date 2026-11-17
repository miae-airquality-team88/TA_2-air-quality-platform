"""Outils partagés par les tests (AQ-12).

La fixture db_conn fournit une connexion à une base de TEST séparée
(airquality_test) : les tests ne touchent jamais aux données de la base
de travail. Si PostgreSQL n'est pas joignable, les tests qui utilisent
db_conn sont simplement ignorés (« skipped ») au lieu d'échouer.
"""
import psycopg2
import pytest
from psycopg2 import sql

from common.config import POSTGRES_DB
from common.db import get_connection
from scripts.init_db import existing_tables, init_db

TEST_DB = "airquality_test"


def _ensure_test_database():
    """Crée la base de test si elle n'existe pas encore."""
    conn = get_connection(dbname=POSTGRES_DB, connect_timeout=3)
    conn.autocommit = True  # CREATE DATABASE interdit dans une transaction
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (TEST_DB,))
            if cur.fetchone() is None:
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(TEST_DB)))
    finally:
        conn.close()


@pytest.fixture(scope="session")
def _test_database():
    try:
        _ensure_test_database()
        conn = get_connection(dbname=TEST_DB, connect_timeout=3)
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL non disponible ({exc.__class__.__name__}) : test ignoré")
    init_db(conn)
    yield conn
    conn.close()


@pytest.fixture
def db_conn(_test_database):
    """Connexion à la base de test, TOUTES les tables vidées avant chaque test."""
    conn = _test_database
    tables = existing_tables(conn)
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("TRUNCATE {} RESTART IDENTITY").format(
                sql.SQL(", ").join(sql.Identifier(t) for t in tables)
            )
        )
    conn.commit()
    return conn
