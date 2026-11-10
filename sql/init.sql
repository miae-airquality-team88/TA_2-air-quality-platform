-- =============================================================
--  Schéma PostgreSQL du projet (AQ-12)
--  Idempotent : peut être exécuté plusieurs fois sans erreur
--  (CREATE ... IF NOT EXISTS).
--  Exécution : python -m scripts.init_db
-- =============================================================

-- -------------------------------------------------------------
-- 1. Mesures en temps réel (une ligne par message traité)
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS measurements (
    id                BIGSERIAL PRIMARY KEY,
    sensor_id         TEXT        NOT NULL,
    source            TEXT        NOT NULL,              -- simulator | openaq (lignée)
    measured_at       TIMESTAMPTZ NOT NULL,              -- horodatage de la mesure (capteur)
    lat               DOUBLE PRECISION,
    lon               DOUBLE PRECISION,
    -- mesures nettoyées (NULL si absente ou invalide)
    pm2_5             DOUBLE PRECISION,
    pm10              DOUBLE PRECISION,
    no2               DOUBLE PRECISION,
    o3                DOUBLE PRECISION,
    temperature       DOUBLE PRECISION,
    humidity          DOUBLE PRECISION,
    -- moyennes glissantes calculées par le streaming (AQ-20)
    pm2_5_mean_5min   DOUBLE PRECISION,
    pm2_5_mean_15min  DOUBLE PRECISION,
    pm10_mean_5min    DOUBLE PRECISION,
    pm10_mean_15min   DOUBLE PRECISION,
    -- qualité des données
    is_valid          BOOLEAN     NOT NULL,              -- aucune anomalie de qualité
    issue_count       INTEGER     NOT NULL DEFAULT 0,
    anomaly_count     INTEGER     NOT NULL DEFAULT 0,
    injected_anomaly  TEXT,                              -- vérité terrain (simulateur)
    -- traçabilité Kafka et mesure de latence (AQ-23)
    kafka_partition   INTEGER,
    kafka_offset      BIGINT,
    kafka_timestamp   TIMESTAMPTZ,                       -- écriture du message dans Kafka
    processed_at      TIMESTAMPTZ,                       -- traitement par le consumer
    inserted_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),  -- écriture en base
    -- idempotence : un même message Kafka n'est jamais inséré deux fois
    CONSTRAINT uq_measurements_kafka UNIQUE (kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS idx_measurements_sensor_time ON measurements (sensor_id, measured_at);
CREATE INDEX IF NOT EXISTS idx_measurements_time        ON measurements (measured_at);

-- -------------------------------------------------------------
-- 2. Anomalies détectées (une ligne par anomalie)
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS anomalies (
    id                BIGSERIAL PRIMARY KEY,
    sensor_id         TEXT        NOT NULL,
    source            TEXT,
    measured_at       TIMESTAMPTZ,
    category          TEXT        NOT NULL,  -- qualite_donnees | pollution | capteur | message
    anomaly_type      TEXT        NOT NULL,  -- negative, out_of_range, missing, seuil_oms, pic...
    parameter         TEXT,                  -- mesure concernée (pm2_5…) ou NULL
    value             DOUBLE PRECISION,
    threshold         DOUBLE PRECISION,
    severity          TEXT        NOT NULL,  -- info | alerte | critique
    detail            TEXT,
    injected_anomaly  TEXT,                  -- vérité terrain, pour évaluer la détection
    kafka_partition   INTEGER,
    kafka_offset      BIGINT,
    inserted_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- idempotence : la même anomalie d'un même message Kafka n'est enregistrée qu'une fois
CREATE UNIQUE INDEX IF NOT EXISTS uq_anomalies_kafka
    ON anomalies (kafka_partition, kafka_offset, anomaly_type, COALESCE(parameter, ''));
CREATE INDEX IF NOT EXISTS idx_anomalies_time ON anomalies (measured_at);
CREATE INDEX IF NOT EXISTS idx_anomalies_type ON anomalies (category, anomaly_type);

-- -------------------------------------------------------------
-- 3. Agrégats quotidiens (batch, une ligne par jour et par capteur)
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS daily_aggregates (
    day                DATE    NOT NULL,
    sensor_id          TEXT    NOT NULL,
    source             TEXT,
    measurement_count  INTEGER NOT NULL,
    valid_count        INTEGER NOT NULL,
    completeness       DOUBLE PRECISION,     -- part des mesures valides (0 à 1)
    pm2_5_avg          DOUBLE PRECISION,
    pm2_5_max          DOUBLE PRECISION,
    pm10_avg           DOUBLE PRECISION,
    pm10_max           DOUBLE PRECISION,
    no2_avg            DOUBLE PRECISION,
    no2_max            DOUBLE PRECISION,
    o3_avg             DOUBLE PRECISION,
    o3_max             DOUBLE PRECISION,
    temperature_avg    DOUBLE PRECISION,
    humidity_avg       DOUBLE PRECISION,
    anomaly_count      INTEGER NOT NULL DEFAULT 0,
    computed_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (day, sensor_id)
);
