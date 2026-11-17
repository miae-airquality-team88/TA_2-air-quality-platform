"""Tests du schéma PostgreSQL (AQ-12, AQ-25). Ignorés si PostgreSQL n'est pas démarré."""
import psycopg2
import pytest

from scripts.init_db import CORE_TABLES, existing_tables, init_db, sql_files


def test_init_sql_fait_partie_des_fichiers_executes():
    assert "init.sql" in [p.name for p in sql_files()]


def test_les_tables_principales_existent(db_conn):
    assert set(CORE_TABLES) <= set(existing_tables(db_conn))


def test_init_db_est_idempotent(db_conn):
    avant = existing_tables(db_conn)
    init_db(db_conn)  # 2e exécution : aucune erreur, aucune table en plus
    assert existing_tables(db_conn) == avant


def test_meme_message_kafka_pas_insere_deux_fois(db_conn):
    insert = ("INSERT INTO measurements (sensor_id, source, measured_at, is_valid, "
              "kafka_partition, kafka_offset) VALUES ('CAP_001', 'simulator', now(), true, 0, 42)")
    with db_conn.cursor() as cur:
        cur.execute(insert)
    db_conn.commit()
    with pytest.raises(psycopg2.errors.UniqueViolation):
        with db_conn.cursor() as cur:
            cur.execute(insert)
    db_conn.rollback()
