-- =============================================================
--  Historique des alertes du pipeline (AQ-38)
--  Toute alerte est enregistrée ici, qu'elle ait pu être envoyée ou non.
-- =============================================================
CREATE TABLE IF NOT EXISTS pipeline_alerts (
    id              BIGSERIAL PRIMARY KEY,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    source          TEXT        NOT NULL,   -- dag_failure | streaming_monitor
    severity        TEXT        NOT NULL,   -- info | alerte | critique
    title           TEXT        NOT NULL,
    message         TEXT        NOT NULL,
    dag_id          TEXT,
    task_id         TEXT,
    run_id          TEXT,
    day             TEXT,                   -- journée traitée par le DAG
    try_number      INTEGER,
    log_url         TEXT,
    channel         TEXT        NOT NULL,   -- slack | email | aucun
    delivered       BOOLEAN     NOT NULL,   -- l'envoi a-t-il réussi ?
    delivery_error  TEXT
);

CREATE INDEX IF NOT EXISTS idx_pipeline_alerts_time ON pipeline_alerts (created_at);
