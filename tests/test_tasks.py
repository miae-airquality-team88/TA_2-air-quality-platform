"""Tests des fonctions partagées par les DAGs (AQ-41). Ignorés sans PostgreSQL.

On remplace la connexion de common.db par la base de test, puis on enchaîne
les 3 étapes du backfill sur une journée générée par AQ-36.
"""
from datetime import date

import common.db
import orchestration.tasks as tasks
from ingestion.history import load_history


def test_enchainement_du_backfill(db_conn, monkeypatch, tmp_path):
    load_history(db_conn, [date(2026, 11, 1)], n_sensors=5, step_minutes=15)   # 480 mesures

    class SameConn:            # get_connection() renvoie la base de test, close() ne ferme pas
        def __getattr__(self, name):
            return getattr(db_conn, name)

        def close(self):
            pass

    monkeypatch.setattr(common.db, "get_connection", lambda: SameConn())
    monkeypatch.setattr("common.config.EXTRACTS_DIR", tmp_path)
    monkeypatch.setenv("QUALITY_MAX_AGE_HOURS", "100000")

    extract = tasks.run_extract("2026-11-01")
    assert extract["rows"] == 480
    checks = tasks.run_validate_quality("2026-11-01", run_id="backfill__test")
    assert all(c["passed"] for c in checks)
    assert tasks.run_aggregate("2026-11-01") == {"day": "2026-11-01", "sensors": 5,
                                                 "measurements": 480}
    # relancer la journée ne change rien (idempotence)
    assert tasks.run_aggregate("2026-11-01")["measurements"] == 480
