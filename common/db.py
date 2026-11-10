"""Connexion à PostgreSQL (AQ-12).

Tous les modules qui accèdent à la base passent par get_connection() :
les paramètres viennent du fichier .env via common/config.py.

Exemple :
    from common.db import get_connection
    with get_connection() as conn:
        ...
"""
import psycopg2

from common.config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)


def get_connection(dbname=None, connect_timeout=5):
    """Ouvre une connexion PostgreSQL (base du .env par défaut)."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=dbname or POSTGRES_DB,
        connect_timeout=connect_timeout,
    )
