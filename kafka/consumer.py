"""
Workshop-3: Streaming ETL with Apache Kafka and Machine Learning
Part B & C — Step 6 & Step 8: Kafka Consumer & PostgreSQL Loader (kafka/consumer.py)

Consumes streaming events from the 'happiness-events' topic under consumer group
'happiness-ml-team', stores every raw event immediately in PostgreSQL
(raw_happiness_events) BEFORE validation or prediction, validates the event
contract, performs real-time inference using models/model.pkl, and loads valid
prediction results into the PostgreSQL star schema (dim_country, dim_date,
fact_predictions).
"""

import argparse
from datetime import datetime
import json
import logging
import math
import os
from pathlib import Path
from typing import Any, Dict, Tuple

import joblib
from kafka import KafkaConsumer
import pandas as pd
import psycopg2


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("HappinessKafkaConsumer")

TOPIC_NAME = "happiness-events"
CONSUMER_GROUP_ID = "happiness-ml-team"

# Exact feature ordering used during model training in notebooks/model_training.ipynb
MODEL_FEATURES = ["gdp", "family", "health", "freedom", "generosity", "corruption"]

REQUIRED_CONTRACT_FIELDS = [
    "event_id",
    "country",
    "year",
    "gdp",
    "family",
    "health",
    "freedom",
    "generosity",
    "corruption",
    "actual_happiness_score",
    "event_time",
]

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_MODEL_PATH = PROJECT_ROOT / "models" / "model.pkl"
DEFAULT_DDL_PATH = PROJECT_ROOT / "sql" / "create_tables.sql"
ENV_FILE_PATH = PROJECT_ROOT / ".env"


def load_env_file(env_path: Path = ENV_FILE_PATH) -> None:
    """Load key=value pairs from .env into os.environ without external hardcoding."""
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


load_env_file()


def get_postgres_connection() -> psycopg2.extensions.connection:
    """Create a connection to the PostgreSQL database using environment variables from .env."""
    required_vars = ["POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"]
    missing_vars = [v for v in required_vars if not os.getenv(v)]
    if missing_vars:
        raise EnvironmentError(
            f"Missing required PostgreSQL environment variables in .env: {missing_vars}. "
            "Please copy .env.example to .env and configure your credentials."
        )
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def ensure_database_schema(conn: psycopg2.extensions.connection, ddl_path: Path) -> None:
    """Ensure all required PostgreSQL tables exist by executing sql/create_tables.sql."""
    if not ddl_path.exists():
        raise FileNotFoundError(f"DDL script not found at: {ddl_path}")
    sql_script = ddl_path.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql_script)
    conn.commit()
    logger.info("Verified PostgreSQL schema from '%s'.", ddl_path.name)


def insert_raw_event(
    conn: psycopg2.extensions.connection,
    kafka_msg: Any,
    raw_payload_str: str,
    kafka_key_str: str | None,
) -> int:
    """
    Step 6.2 Requirement: Store the original incoming Kafka message in
    raw_happiness_events BEFORE applying any transformation, validation, or prediction.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw_happiness_events (
                kafka_topic,
                kafka_partition,
                kafka_offset,
                kafka_key,
                raw_payload,
                processing_status
            )
            VALUES (%s, %s, %s, %s, %s, 'RECEIVED')
            RETURNING raw_event_id;
            """,
            (
                kafka_msg.topic,
                kafka_msg.partition,
                kafka_msg.offset,
                kafka_key_str,
                raw_payload_str,
            ),
        )
        raw_event_id = int(cur.fetchone()[0])
    conn.commit()
    return raw_event_id


def update_raw_event_status(
    conn: psycopg2.extensions.connection,
    raw_event_id: int,
    status: str,
    event_id: str | None = None,
    notes: str | None = None,
) -> None:
    """Update the processing status and validation notes of a raw event in PostgreSQL."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE raw_happiness_events
               SET processing_status = %s,
                   event_id = COALESCE(%s, event_id),
                   validation_notes = %s
             WHERE raw_event_id = %s;
            """,
            (status, event_id, notes, raw_event_id),
        )
    conn.commit()


def is_valid_finite_number(value: Any) -> bool:
    """Check whether a value is a valid finite int or float (rejecting booleans and NaNs)."""
    if isinstance(value, bool) or value is None:
        return False
    if not isinstance(value, (int, float)):
        return False
    return not (math.isnan(float(value)) or math.isinf(float(value)))


def validate_event_contract(raw_payload_str: str) -> Tuple[str, Dict[str, Any] | None, str]:
    """
    Validate an incoming Kafka message against the defined Event Contract.
    Returns (status, parsed_event_dict, validation_message) where status is one of:
      - 'VALID'
      - 'INVALID_SCHEMA'
      - 'INVALID_VALUES'
    """
    # 1. Parse JSON structure
    try:
        event = json.loads(raw_payload_str)
    except Exception as exc:
        return "INVALID_SCHEMA", None, f"Malformed JSON payload: {exc}"

    if not isinstance(event, dict):
        return "INVALID_SCHEMA", None, "Event payload must be a JSON object."

    # 2. Check required contract fields and model features
    missing_fields = [f for f in REQUIRED_CONTRACT_FIELDS if f not in event]
    if missing_fields:
        missing_model_feats = [f for f in MODEL_FEATURES if f in missing_fields]
        if missing_model_feats:
            return (
                "INVALID_SCHEMA",
                event,
                f"Missing required model features: {missing_model_feats}",
            )
        return (
            "INVALID_SCHEMA",
            event,
            f"Missing required contract fields: {missing_fields}",
        )

    # 3. Validate 'event_id' and 'country' (non-empty strings)
    for str_field in ("event_id", "country"):
        val = event.get(str_field)
        if not isinstance(val, str) or not val.strip():
            return (
                "INVALID_VALUES",
                event,
                f"Field '{str_field}' must be a non-empty string.",
            )

    # 4. Validate 'year' (integer)
    year_val = event.get("year")
    if isinstance(year_val, bool) or not isinstance(year_val, int):
        return "INVALID_VALUES", event, "Field 'year' must be a valid integer."
    if year_val < 1900 or year_val > 2100:
        return "INVALID_VALUES", event, f"Field 'year' out of valid range: {year_val}"

    # 5. Validate all 6 model features (valid finite numeric values >= 0.0)
    for feat in MODEL_FEATURES:
        feat_val = event.get(feat)
        if not is_valid_finite_number(feat_val):
            return (
                "INVALID_VALUES",
                event,
                f"Model feature '{feat}' has invalid or missing numeric value: {feat_val!r}",
            )
        if float(feat_val) < 0.0 or float(feat_val) > 10.0:
            return (
                "INVALID_VALUES",
                event,
                f"Model feature '{feat}' out of plausible range [0, 10]: {feat_val}",
            )

    # 6. Validate 'actual_happiness_score' (used ONLY for evaluation, never as model feature)
    actual_score = event.get("actual_happiness_score")
    if not is_valid_finite_number(actual_score) or float(actual_score) <= 0.0:
        return (
            "INVALID_VALUES",
            event,
            f"Invalid 'actual_happiness_score': {actual_score!r}",
        )

    # 7. Validate 'event_time' (valid ISO-8601 timestamp)
    event_time_str = event.get("event_time")
    if not isinstance(event_time_str, str) or not event_time_str.strip():
        return "INVALID_VALUES", event, "Field 'event_time' must be an ISO-8601 string."
    try:
        datetime.fromisoformat(event_time_str.replace("Z", "+00:00"))
    except ValueError as exc:
        return (
            "INVALID_VALUES",
            event,
            f"Invalid ISO-8601 'event_time' ({event_time_str!r}): {exc}",
        )

    return "VALID", event, "Event satisfies all contract requirements."


def get_or_create_country_id(conn: psycopg2.extensions.connection, country_name: str) -> int:
    """Upsert country in dim_country and return its surrogate key country_id."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dim_country (country_name)
            VALUES (%s)
            ON CONFLICT (country_name) DO UPDATE
               SET country_name = EXCLUDED.country_name
            RETURNING country_id;
            """,
            (country_name.strip(),),
        )
        return int(cur.fetchone()[0])


def get_or_create_date_id(
    conn: psycopg2.extensions.connection, observation_year: int, event_dt: datetime
) -> int:
    """Upsert temporal record in dim_date and return its surrogate key date_id."""
    event_date = event_dt.date()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dim_date (
                observation_year,
                event_date,
                event_year,
                event_month,
                event_day
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (observation_year, event_date) DO UPDATE
               SET observation_year = EXCLUDED.observation_year
            RETURNING date_id;
            """,
            (
                observation_year,
                event_date,
                event_date.year,
                event_date.month,
                event_date.day,
            ),
        )
        return int(cur.fetchone()[0])


def store_prediction_result(
    conn: psycopg2.extensions.connection,
    raw_event_id: int,
    event: Dict[str, Any],
    predicted_score: float,
    prediction_error: float,
) -> int:
    """
    Step 8 Requirement: Insert prediction result into fact_predictions linked
    to raw_happiness_events via raw_event_id and referencing dim_country and dim_date.
    """
    event_dt = datetime.fromisoformat(event["event_time"].replace("Z", "+00:00"))
    country_id = get_or_create_country_id(conn, event["country"])
    date_id = get_or_create_date_id(conn, int(event["year"]), event_dt)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO fact_predictions (
                raw_event_id,
                country_id,
                date_id,
                actual_score,
                predicted_score,
                prediction_error,
                event_time,
                prediction_timestamp
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            RETURNING prediction_id;
            """,
            (
                raw_event_id,
                country_id,
                date_id,
                float(event["actual_happiness_score"]),
                float(predicted_score),
                float(prediction_error),
                event_dt,
            ),
        )
        prediction_id = int(cur.fetchone()[0])

        cur.execute(
            """
            UPDATE raw_happiness_events
               SET processing_status = 'VALID',
                   event_id = %s,
                   validation_notes = 'Processed and stored in fact_predictions'
             WHERE raw_event_id = %s;
            """,
            (event["event_id"], raw_event_id),
        )
    conn.commit()
    return prediction_id


def run_consumer(
    bootstrap_servers: str,
    model_path: Path,
    ddl_path: Path,
    auto_offset_reset: str = "earliest",
    max_messages: int | None = None,
    consumer_timeout_ms: int | None = None,
) -> None:
    """Main loop for consuming Kafka events, validating contracts, running ML inference, and storing results."""
    if not model_path.exists():
        raise FileNotFoundError(f"Serialized model not found at: {model_path}")

    model_pipeline = joblib.load(model_path)
    logger.info("Loaded serialized ML pipeline from '%s'.", model_path.name)

    db_conn = get_postgres_connection()
    ensure_database_schema(db_conn, ddl_path)

    consumer_kwargs: Dict[str, Any] = {
        "bootstrap_servers": bootstrap_servers,
        "group_id": CONSUMER_GROUP_ID,
        "auto_offset_reset": auto_offset_reset,
        "enable_auto_commit": True,
    }
    if consumer_timeout_ms is not None and consumer_timeout_ms > 0:
        consumer_kwargs["consumer_timeout_ms"] = consumer_timeout_ms

    consumer = KafkaConsumer(TOPIC_NAME, **consumer_kwargs)
    logger.info(
        "Started Kafka consumer in group '%s' subscribed to topic '%s'...",
        CONSUMER_GROUP_ID,
        TOPIC_NAME,
    )

    stats = {
        "total": 0,
        "VALID": 0,
        "INVALID_SCHEMA": 0,
        "INVALID_VALUES": 0,
        "PREDICTION_ERROR": 0,
    }

    try:
        for msg in consumer:
            stats["total"] += 1
            raw_payload_str = (
                msg.value.decode("utf-8", errors="replace")
                if isinstance(msg.value, (bytes, bytearray))
                else str(msg.value)
            )
            kafka_key_str = (
                msg.key.decode("utf-8", errors="replace")
                if isinstance(msg.key, (bytes, bytearray))
                else (str(msg.key) if msg.key is not None else None)
            )

            # 1. IMMEDIATELY store raw event in PostgreSQL BEFORE validation or prediction
            raw_event_id = insert_raw_event(
                db_conn, msg, raw_payload_str, kafka_key_str
            )

            # 2. Validate event against the Event Contract
            status, event, validation_notes = validate_event_contract(raw_payload_str)
            event_id_val = (
                str(event.get("event_id"))
                if isinstance(event, dict) and event.get("event_id") is not None
                else None
            )

            # 3. Handle invalid records with fault tolerance (skip prediction, do NOT crash)
            if status != "VALID" or event is None:
                stats[status] += 1
                update_raw_event_status(
                    db_conn,
                    raw_event_id=raw_event_id,
                    status=status,
                    event_id=event_id_val,
                    notes=validation_notes,
                )
                logger.warning(
                    "Skipped invalid event [raw_event_id=%d, event_id=%s] | status=%s | reason=%s",
                    raw_event_id,
                    event_id_val,
                    status,
                    validation_notes,
                )
            else:
                # 4. Prepare features in exact training order and run ML inference
                try:
                    features_df = pd.DataFrame(
                        [{feat: float(event[feat]) for feat in MODEL_FEATURES}],
                        columns=MODEL_FEATURES,
                    )
                    predicted_score = float(model_pipeline.predict(features_df)[0])
                    actual_score = float(event["actual_happiness_score"])
                    prediction_error = abs(actual_score - predicted_score)

                    prediction_id = store_prediction_result(
                        db_conn,
                        raw_event_id=raw_event_id,
                        event=event,
                        predicted_score=predicted_score,
                        prediction_error=prediction_error,
                    )
                    stats["VALID"] += 1

                    if stats["VALID"] == 1 or stats["VALID"] % 50 == 0:
                        logger.info(
                            "Processed [valid=%d] prediction_id=%d | raw_event_id=%d | event_id=%s | country=%s (%d) | actual=%.3f | pred=%.3f | err=%.3f",
                            stats["VALID"],
                            prediction_id,
                            raw_event_id,
                            event["event_id"],
                            event["country"],
                            event["year"],
                            actual_score,
                            predicted_score,
                            prediction_error,
                        )
                except Exception as exc:
                    db_conn.rollback()
                    stats["PREDICTION_ERROR"] += 1
                    update_raw_event_status(
                        db_conn,
                        raw_event_id=raw_event_id,
                        status="PREDICTION_ERROR",
                        event_id=event_id_val,
                        notes=f"Inference/storage exception: {exc}",
                    )
                    logger.error(
                        "Prediction error on raw_event_id=%d (event_id=%s): %s",
                        raw_event_id,
                        event_id_val,
                        exc,
                    )

            if max_messages is not None and stats["total"] >= max_messages:
                logger.info("Reached max_messages=%d. Stopping consumer.", max_messages)
                break

    except KeyboardInterrupt:
        logger.warning("Consumer stopped by user.")
    finally:
        consumer.close()
        db_conn.close()
        logger.info("Consumer finished. Summary statistics: %s", stats)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consume World Happiness events from Kafka, validate contract, run ML inference, and store in PostgreSQL."
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument(
        "--model-path",
        type=Path,
        default=DEFAULT_MODEL_PATH,
        help="Path to serialized scikit-learn pipeline (models/model.pkl)",
    )
    parser.add_argument(
        "--ddl-path",
        type=Path,
        default=DEFAULT_DDL_PATH,
        help="Path to PostgreSQL DDL script (sql/create_tables.sql)",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="Optional maximum number of messages to process before exiting",
    )
    parser.add_argument(
        "--consumer-timeout-ms",
        type=int,
        default=None,
        help="Optional timeout in ms to stop consumer if no new messages arrive",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_consumer(
        bootstrap_servers=args.bootstrap_servers,
        model_path=args.model_path,
        ddl_path=args.ddl_path,
        max_messages=args.max_messages,
        consumer_timeout_ms=args.consumer_timeout_ms,
    )
