-- =============================================================
--  Historique des contrôles qualité du DAG quotidien (AQ-29)
--  Une ligne par contrôle et par exécution du DAG.
-- =============================================================
CREATE TABLE IF NOT EXISTS quality_checks (
    id          BIGSERIAL PRIMARY KEY,
    day         DATE        NOT NULL,              -- journée contrôlée
    run_id      TEXT,                              -- identifiant de l'exécution Airflow
    check_name  TEXT        NOT NULL,              -- volume | completude | fraicheur
    value       DOUBLE PRECISION,                  -- valeur mesurée
    threshold   DOUBLE PRECISION,                  -- seuil attendu
    passed      BOOLEAN     NOT NULL,
    detail      TEXT,
    checked_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS idx_quality_checks_day ON quality_checks (day, check_name);
