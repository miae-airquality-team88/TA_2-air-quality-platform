"""Tests des conditions des sensors (AQ-33). Ignorés si PostgreSQL n'est pas démarré."""
from datetime import datetime, timedelta, timezone

from orchestration.sensors import is_streaming_done, new_data_sql


def insert(conn, n, day=16):
    with conn.cursor() as cur:
        for i in range(n):
            cur.execute(
                "INSERT INTO measurements (sensor_id, source, measured_at, is_valid, "
                "kafka_partition, kafka_offset) VALUES ('CAP_001', 'simulator', %s, true, 0, %s)",
                (datetime(2026, 11, day, 12, tzinfo=timezone.utc) + timedelta(seconds=i), i),
            )
    conn.commit()


def sensor_result(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()[0]


def test_requete_contient_la_date():
    sql = new_data_sql("2026-11-16", min_rows=10)
    assert "COUNT(*) >= 10" in sql and "'2026-11-16'" in sql


def test_faux_tant_qu_il_manque_des_donnees(db_conn):
    insert(db_conn, 5)
    assert sensor_result(db_conn, new_data_sql("2026-11-16", min_rows=10)) is False


def test_vrai_quand_assez_de_donnees(db_conn):
    insert(db_conn, 12)
    assert sensor_result(db_conn, new_data_sql("2026-11-16", min_rows=10)) is True


def test_les_autres_journees_ne_comptent_pas(db_conn):
    insert(db_conn, 12, day=17)
    assert sensor_result(db_conn, new_data_sql("2026-11-16", min_rows=10)) is False


# --- AQ-34 : fin du traitement streaming ---------------------------------------------------

def heartbeat(conn, last_seen, lag, pending=0, status="running"):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pipeline_heartbeats (component, status, last_seen, consumer_lag, "
            "pending_rows) VALUES ('aq-streaming', %s, %s, %s, %s)",
            (status, last_seen, lag, pending),
        )
    conn.commit()


def test_streaming_aucun_signal(db_conn):
    assert is_streaming_done(db_conn, "2026-11-16")[0] is False


def test_streaming_termine(db_conn):
    heartbeat(db_conn, datetime(2026, 11, 17, 0, 5, tzinfo=timezone.utc), lag=0, status="stopped")
    done, reason = is_streaming_done(db_conn, "2026-11-16")
    assert done is True, reason


def test_streaming_en_retard(db_conn):
    heartbeat(db_conn, datetime(2026, 11, 17, 0, 5, tzinfo=timezone.utc), lag=120)
    assert is_streaming_done(db_conn, "2026-11-16")[0] is False


def test_streaming_signal_trop_ancien(db_conn):
    heartbeat(db_conn, datetime(2026, 11, 16, 23, 50, tzinfo=timezone.utc), lag=0)
    assert is_streaming_done(db_conn, "2026-11-16")[0] is False


def test_streaming_lot_pas_encore_ecrit(db_conn):
    heartbeat(db_conn, datetime(2026, 11, 17, 1, 0, tzinfo=timezone.utc), lag=0, pending=12)
    assert is_streaming_done(db_conn, "2026-11-16")[0] is False
