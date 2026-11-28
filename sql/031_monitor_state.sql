-- =============================================================
--  État de la surveillance du consumer (AQ-39)
--  Mémorise le problème en cours pour ne pas renvoyer la même alerte
--  toutes les 15 minutes (anti-spam).
-- =============================================================
CREATE TABLE IF NOT EXISTS monitor_state (
    component   TEXT PRIMARY KEY,           -- ex. 'aq-streaming'
    problem     TEXT,                       -- NULL = tout va bien
    since       TIMESTAMPTZ,                -- début du problème en cours
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
