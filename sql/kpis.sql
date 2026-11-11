-- ============================================================================
-- Workshop-3: Streaming ETL with Apache Kafka and Machine Learning
-- Part C — Step 9: PostgreSQL Analytical KPI Queries (sql/kpis.sql)
-- ============================================================================
-- These queries connect directly to the PostgreSQL database ('happiness_db')
-- to power BI dashboards (Power BI / Looker Studio / Tableau) for the 4
-- required KPIs and pipeline auditing.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- KPI 1: Average Prediction Error (Global MAE, RMSE, Min/Max Error & Volume)
-- ----------------------------------------------------------------------------
SELECT
    COUNT(fp.prediction_id)                             AS total_predictions,
    ROUND(AVG(fp.prediction_error)::numeric, 4)         AS avg_prediction_error_mae,
    ROUND(SQRT(AVG(POWER(fp.actual_score - fp.predicted_score, 2)))::numeric, 4) AS rmse,
    ROUND(MIN(fp.prediction_error)::numeric, 4)         AS min_prediction_error,
    ROUND(MAX(fp.prediction_error)::numeric, 4)         AS max_prediction_error,
    ROUND(AVG(fp.actual_score)::numeric, 4)             AS avg_actual_score,
    ROUND(AVG(fp.predicted_score)::numeric, 4)          AS avg_predicted_score
FROM fact_predictions fp;


-- ----------------------------------------------------------------------------
-- KPI 2: Predictions by Country (Aggregated Scores, Error & Volume per Country)
-- ----------------------------------------------------------------------------
SELECT
    dc.country_name                                     AS country,
    COUNT(fp.prediction_id)                             AS total_predictions,
    ROUND(AVG(fp.actual_score)::numeric, 4)             AS avg_actual_score,
    ROUND(AVG(fp.predicted_score)::numeric, 4)          AS avg_predicted_score,
    ROUND(AVG(fp.prediction_error)::numeric, 4)         AS avg_prediction_error
FROM fact_predictions fp
INNER JOIN dim_country dc
    ON fp.country_id = dc.country_id
GROUP BY dc.country_name
ORDER BY avg_predicted_score DESC;


-- ----------------------------------------------------------------------------
-- KPI 3: Predicted vs. Actual Score (Record-Level Comparison with Traceability)
-- ----------------------------------------------------------------------------
SELECT
    fp.prediction_id,
    rhe.event_id,
    dc.country_name                                     AS country,
    dd.observation_year                                 AS year,
    ROUND(fp.actual_score::numeric, 4)                  AS actual_score,
    ROUND(fp.predicted_score::numeric, 4)               AS predicted_score,
    ROUND((fp.predicted_score - fp.actual_score)::numeric, 4) AS signed_error,
    ROUND(fp.prediction_error::numeric, 4)              AS absolute_error
FROM fact_predictions fp
INNER JOIN dim_country dc
    ON fp.country_id = dc.country_id
INNER JOIN dim_date dd
    ON fp.date_id = dd.date_id
INNER JOIN raw_happiness_events rhe
    ON fp.raw_event_id = rhe.raw_event_id
ORDER BY dd.observation_year ASC, fp.actual_score DESC;


-- ----------------------------------------------------------------------------
-- KPI 4A: Prediction Trends Over Time (By Historical Observation Year 2015-2019)
-- ----------------------------------------------------------------------------
SELECT
    dd.observation_year                                 AS year,
    COUNT(fp.prediction_id)                             AS predictions_count,
    ROUND(AVG(fp.actual_score)::numeric, 4)             AS avg_actual_score,
    ROUND(AVG(fp.predicted_score)::numeric, 4)          AS avg_predicted_score,
    ROUND(AVG(fp.prediction_error)::numeric, 4)         AS avg_prediction_error
FROM fact_predictions fp
INNER JOIN dim_date dd
    ON fp.date_id = dd.date_id
GROUP BY dd.observation_year
ORDER BY dd.observation_year ASC;


-- ----------------------------------------------------------------------------
-- KPI 4B: Streaming Prediction Trends Over Event Time (Real-Time Arrival Window)
-- ----------------------------------------------------------------------------
SELECT
    DATE_TRUNC('second', fp.event_time)                 AS event_second_window,
    COUNT(fp.prediction_id)                             AS events_processed,
    ROUND(AVG(fp.actual_score)::numeric, 4)             AS avg_actual_score,
    ROUND(AVG(fp.predicted_score)::numeric, 4)          AS avg_predicted_score,
    ROUND(AVG(fp.prediction_error)::numeric, 4)         AS avg_prediction_error
FROM fact_predictions fp
GROUP BY DATE_TRUNC('second', fp.event_time)
ORDER BY event_second_window ASC;


-- ----------------------------------------------------------------------------
-- BONUS AUDIT QUERY: Event Contract Validation & Processing Status Breakdown
-- ----------------------------------------------------------------------------
SELECT
    processing_status,
    COUNT(*)                                            AS total_events,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ())::numeric, 2) AS percentage
FROM raw_happiness_events
GROUP BY processing_status
ORDER BY total_events DESC;
