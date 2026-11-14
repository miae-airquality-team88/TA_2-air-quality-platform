"""Tests du rapport de latence (AQ-23). Ignorés si PostgreSQL n'est pas démarré."""
from scripts.latency_report import latency_stats


def insert(conn, offset, kafka_ms_before, processed_ms_before, source="simulator"):
    """Insère une mesure dont les horodatages sont décalés par rapport à inserted_at."""
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO measurements (sensor_id, source, measured_at, is_valid,
                   kafka_partition, kafka_offset, kafka_timestamp, processed_at, inserted_at)
               VALUES ('CAP_001', %s, now(), true, 0, %s,
                       now() - make_interval(secs => %s / 1000.0),
                       now() - make_interval(secs => %s / 1000.0),
                       now())""",
            (source, offset, kafka_ms_before, processed_ms_before),
        )
    conn.commit()


def test_latences_calculees(db_conn):
    insert(db_conn, 1, kafka_ms_before=100, processed_ms_before=40)
    insert(db_conn, 2, kafka_ms_before=300, processed_ms_before=60)
    rows = {r["etape"]: r for r in latency_stats(db_conn, minutes=5)}
    bout_en_bout = rows["kafka_vers_db"]
    assert bout_en_bout["messages"] == 2
    assert bout_en_bout["moyenne_ms"] == 200.0
    assert bout_en_bout["max_ms"] == 300.0
    assert rows["traitement_vers_db"]["moyenne_ms"] == 50.0
    assert rows["kafka_vers_traitement"]["moyenne_ms"] == 150.0


def test_aucune_donnee(db_conn):
    assert latency_stats(db_conn, minutes=5) == []
