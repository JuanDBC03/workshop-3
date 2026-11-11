-- ============================================================================
-- Workshop-3: Streaming ETL with Apache Kafka and Machine Learning
-- Part C — Step 7: PostgreSQL Analytical Database Schema (sql/create_tables.sql)
-- ============================================================================
-- Architecture:
--   1. Raw Staging Table : raw_happiness_events (stores unmodified Kafka events
--                          and tracks contract validation / processing status)
--   2. Dimension Tables  : dim_country, dim_date
--   3. Fact Table        : fact_predictions (links every prediction back to the
--                          exact raw event via raw_event_id and to dimensions)
-- ============================================================================

-- 1. RAW STAGING TABLE (Traceability, Auditing & Fault Tolerance)
CREATE TABLE IF NOT EXISTS raw_happiness_events (
    raw_event_id        SERIAL PRIMARY KEY,
    event_id            VARCHAR(64),
    kafka_topic         VARCHAR(128) NOT NULL,
    kafka_partition     INTEGER NOT NULL,
    kafka_offset        BIGINT NOT NULL,
    kafka_key           VARCHAR(128),
    raw_payload         TEXT NOT NULL,
    processing_status   VARCHAR(32) NOT NULL DEFAULT 'RECEIVED'
                        CHECK (processing_status IN (
                            'RECEIVED',
                            'VALID',
                            'INVALID_SCHEMA',
                            'INVALID_VALUES',
                            'PREDICTION_ERROR'
                        )),
    validation_notes    TEXT,
    ingested_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_raw_events_status
    ON raw_happiness_events (processing_status);

CREATE INDEX IF NOT EXISTS idx_raw_events_event_id
    ON raw_happiness_events (event_id);


-- 2. DIMENSION TABLE: COUNTRY (dim_country)
CREATE TABLE IF NOT EXISTS dim_country (
    country_id          SERIAL PRIMARY KEY,
    country_name        VARCHAR(128) NOT NULL UNIQUE,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- 3. DIMENSION TABLE: DATE / TIME (dim_date)
-- Captures the historical observation year and the streaming event date
CREATE TABLE IF NOT EXISTS dim_date (
    date_id             SERIAL PRIMARY KEY,
    observation_year    INTEGER NOT NULL,
    event_date          DATE NOT NULL,
    event_year          INTEGER NOT NULL,
    event_month         INTEGER NOT NULL,
    event_day           INTEGER NOT NULL,
    CONSTRAINT uq_dim_date_obs_event UNIQUE (observation_year, event_date)
);


-- 4. FACT TABLE: PREDICTIONS (fact_predictions)
CREATE TABLE IF NOT EXISTS fact_predictions (
    prediction_id           SERIAL PRIMARY KEY,
    raw_event_id            INTEGER NOT NULL UNIQUE
                            REFERENCES raw_happiness_events (raw_event_id)
                            ON DELETE RESTRICT,
    country_id              INTEGER NOT NULL
                            REFERENCES dim_country (country_id)
                            ON DELETE RESTRICT,
    date_id                 INTEGER NOT NULL
                            REFERENCES dim_date (date_id)
                            ON DELETE RESTRICT,
    actual_score            DOUBLE PRECISION NOT NULL,
    predicted_score         DOUBLE PRECISION NOT NULL,
    prediction_error        DOUBLE PRECISION NOT NULL,
    event_time              TIMESTAMP NOT NULL,
    prediction_timestamp    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_predictions_country_id
    ON fact_predictions (country_id);

CREATE INDEX IF NOT EXISTS idx_fact_predictions_date_id
    ON fact_predictions (date_id);

CREATE INDEX IF NOT EXISTS idx_fact_predictions_event_time
    ON fact_predictions (event_time);
