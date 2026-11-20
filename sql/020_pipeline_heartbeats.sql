-- =============================================================
--  Signal d'activité (heartbeat) des composants du pipeline (AQ-34)
--  Une ligne par composant, mise à jour régulièrement par le composant lui-même.
-- =============================================================
CREATE TABLE IF NOT EXISTS pipeline_heartbeats (
    component         TEXT PRIMARY KEY,          -- ex. 'aq-streaming'
    status            TEXT        NOT NULL,      -- running | stopped
    last_seen         TIMESTAMPTZ NOT NULL,      -- heure du dernier signal
    messages_read     BIGINT      NOT NULL DEFAULT 0,
    consumer_lag      BIGINT,                    -- messages restant à lire dans Kafka (NULL = inconnu)
    pending_rows      INTEGER     NOT NULL DEFAULT 0,  -- lignes en attente d'écriture en base
    last_measured_at  TIMESTAMPTZ               -- horodatage de la dernière mesure traitée
);
