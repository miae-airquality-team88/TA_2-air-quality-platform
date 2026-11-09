"""Tests du schéma PostgreSQL (AQ-12). Ignorés si PostgreSQL n'est pas démarré."""
import psycopg2
import pytest

from scripts.init_db import existing_tables, init_db


def test_les_trois_tables_existent(db_conn):
    assert existing_tables(db_conn) == ["anomalies", "daily_aggregates", "measurements"]


def test_init_db_est_idempotent(db_conn):
    init_db(db_conn)  # 2e exécution : aucune erreur
    assert len(existing_tables(db_conn)) == 3


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


def test_initialisation_conserve_les_donnees(db_conn):
    with db_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO measurements (sensor_id, source, measured_at, is_valid) "
            "VALUES ('CAP_001', 'simulator', now(), true)"
        )
    db_conn.commit()
    init_db(db_conn)
    with db_conn.cursor() as cur:
        cur.execute("SELECT sensor_id FROM measurements")
        assert cur.fetchall() == [("CAP_001",)]


def test_anomalies_distinctes_et_doublon_sans_parametre(db_conn):
    insert = (
        "INSERT INTO anomalies (sensor_id, category, anomaly_type, severity, "
        "kafka_partition, kafka_offset, parameter) "
        "VALUES ('CAP_001', 'message', 'invalid', 'alerte', 0, 42, %s)"
    )
    with db_conn.cursor() as cur:
        cur.execute(insert, (None,))
        cur.execute(insert, ("pm10",))
        cur.execute("SELECT count(*) FROM anomalies")
        assert cur.fetchone()[0] == 2
    db_conn.commit()
    with pytest.raises(psycopg2.errors.UniqueViolation):
        with db_conn.cursor() as cur:
            cur.execute(insert, (None,))
    db_conn.rollback()


def test_agregat_unique_par_jour_et_capteur(db_conn):
    insert = (
        "INSERT INTO daily_aggregates (day, sensor_id, measurement_count, valid_count) "
        "VALUES (%s, %s, 10, 9)"
    )
    with db_conn.cursor() as cur:
        cur.execute(insert, ("2026-11-09", "CAP_001"))
        cur.execute(insert, ("2026-11-10", "CAP_001"))
        cur.execute(insert, ("2026-11-09", "CAP_002"))
    db_conn.commit()
    with pytest.raises(psycopg2.errors.UniqueViolation):
        with db_conn.cursor() as cur:
            cur.execute(insert, ("2026-11-09", "CAP_001"))
    db_conn.rollback()


def test_horodatage_avec_fuseau_et_mesures_nulles(db_conn):
    with db_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO measurements (sensor_id, source, measured_at, is_valid) "
            "VALUES ('CAP_001', 'simulator', '2026-11-09T11:00:00+01:00', false) "
            "RETURNING measured_at AT TIME ZONE 'UTC', pm2_5, inserted_at"
        )
        measured_at, pm2_5, inserted_at = cur.fetchone()
        assert measured_at.hour == 10
        assert pm2_5 is None
        assert inserted_at.tzinfo is not None
