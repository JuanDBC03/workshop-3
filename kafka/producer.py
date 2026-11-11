"""
Workshop-3: Streaming ETL with Apache Kafka and Machine Learning
Part B — Step 5: Kafka Producer (kafka/producer.py)

Reads prepared historical World Happiness records one by one from
data/streaming/happiness_streaming.csv and publishes each record as a
JSON-serialized streaming event to the 'happiness-events' Kafka topic
(3 partitions, replication factor 1), using 'country' as the Kafka event key.
"""

import argparse
from datetime import datetime
import json
import logging
import os
from pathlib import Path
import time
from typing import Any, Dict

import pandas as pd
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("HappinessKafkaProducer")

TOPIC_NAME = "happiness-events"
NUM_PARTITIONS = 3
REPLICATION_FACTOR = 1

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_STREAM_CSV = PROJECT_ROOT / "data" / "streaming" / "happiness_streaming.csv"
ENV_FILE_PATH = PROJECT_ROOT / ".env"


def load_env_file(env_path: Path = ENV_FILE_PATH) -> None:
    """Load key=value pairs from .env into os.environ."""
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


load_env_file()


def ensure_topic_exists(bootstrap_servers: str) -> None:
    """Ensure the 'happiness-events' topic exists with 3 partitions and replication factor 1."""
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        existing_topics = admin_client.list_topics()
        if TOPIC_NAME not in existing_topics:
            topic = NewTopic(
                name=TOPIC_NAME,
                num_partitions=NUM_PARTITIONS,
                replication_factor=REPLICATION_FACTOR,
            )
            admin_client.create_topics(new_topics=[topic], validate_only=False)
            logger.info(
                "Created Kafka topic '%s' (partitions=%d, replication_factor=%d).",
                TOPIC_NAME,
                NUM_PARTITIONS,
                REPLICATION_FACTOR,
            )
        else:
            logger.info(
                "Verified Kafka topic '%s' already exists.", TOPIC_NAME
            )
    except TopicAlreadyExistsError:
        logger.info("Kafka topic '%s' already exists.", TOPIC_NAME)
    finally:
        admin_client.close()


def build_event_payload(index: int, row: pd.Series) -> Dict[str, Any]:
    """
    Build a streaming event dictionary strictly adhering to the required JSON contract:
    {
      "event_id": "E001",
      "country": "Colombia",
      "year": 2019,
      "gdp": 1.2,
      "family": 0.8,
      "health": 0.9,
      "freedom": 0.6,
      "generosity": 0.3,
      "corruption": 0.1,
      "actual_happiness_score": 6.2,
      "event_time": "2026-10-30T15:30:00"
    }
    """
    return {
        "event_id": f"E{index:03d}",
        "country": str(row["country"]).strip(),
        "year": int(row["year"]),
        "gdp": round(float(row["gdp"]), 5),
        "family": round(float(row["family"]), 5),
        "health": round(float(row["health"]), 5),
        "freedom": round(float(row["freedom"]), 5),
        "generosity": round(float(row["generosity"]), 5),
        "corruption": round(float(row["corruption"]), 5),
        "actual_happiness_score": round(float(row["actual_happiness_score"]), 5),
        "event_time": datetime.now().isoformat(timespec="seconds"),
    }


def create_kafka_producer(bootstrap_servers: str) -> KafkaProducer:
    """Initialize a KafkaProducer configured for UTF-8 key and JSON value serialization."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, allow_nan=True).encode("utf-8"),
        acks="all",
        retries=3,
    )


def publish_Fault_tolerance_test_events(producer: KafkaProducer, delay: float) -> None:
    """
    Optionally publish synthetic malformed events to verify downstream consumer
    contract validation and fault tolerance (INVALID_SCHEMA and INVALID_VALUES).
    """
    invalid_events = [
        # Case 1: Missing required model features ('freedom', 'corruption') -> INVALID_SCHEMA
        {
            "key": "TestCountrySchema",
            "payload": {
                "event_id": "E_ERR_SCHEMA_01",
                "country": "TestCountrySchema",
                "year": 2019,
                "gdp": 1.1,
                "family": 0.9,
                "health": 0.8,
                "generosity": 0.2,
                "actual_happiness_score": 5.5,
                "event_time": datetime.now().isoformat(timespec="seconds"),
            },
        },
        # Case 2: Invalid data type in 'year' and negative numerical feature -> INVALID_VALUES
        {
            "key": "Colombia",
            "payload": {
                "event_id": "E_ERR_VALUES_01",
                "country": "Colombia",
                "year": 2019,
                "gdp": -999.0,
                "family": 0.8,
                "health": 0.9,
                "freedom": 0.6,
                "generosity": 0.3,
                "corruption": None,
                "actual_happiness_score": 6.2,
                "event_time": datetime.now().isoformat(timespec="seconds"),
            },
        },
    ]

    for item in invalid_events:
        future = producer.send(
            TOPIC_NAME, key=item["key"], value=item["payload"]
        )
        metadata = future.get(timeout=10)
        logger.warning(
            "Published test invalid event_id=%s | key=%s | partition=%d | offset=%d",
            item["payload"].get("event_id"),
            item["key"],
            metadata.partition,
            metadata.offset,
        )
        time.sleep(delay)


def stream_events(
    bootstrap_servers: str,
    csv_path: Path,
    delay: float,
    limit: int | None = None,
    inject_invalid: bool = False,
) -> None:
    """Read historical CSV records sequentially and publish each record to Kafka."""
    if not csv_path.exists():
        raise FileNotFoundError(f"Streaming source CSV not found at: {csv_path}")

    df = pd.read_csv(csv_path)
    if limit is not None and limit > 0:
        df = df.head(limit)

    ensure_topic_exists(bootstrap_servers)
    producer = create_kafka_producer(bootstrap_servers)

    logger.info(
        "Starting Kafka streaming simulation from '%s' (%d records, delay=%.3fs)...",
        csv_path.name,
        len(df),
        delay,
    )

    published_count = 0
    try:
        if inject_invalid:
            publish_Fault_tolerance_test_events(producer, delay)

        for idx, (_, row) in enumerate(df.iterrows(), start=1):
            event_payload = build_event_payload(idx, row)
            event_key = event_payload["country"]

            future = producer.send(
                TOPIC_NAME,
                key=event_key,
                value=event_payload,
            )
            record_metadata = future.get(timeout=10)
            published_count += 1

            if published_count == 1 or published_count % 50 == 0 or published_count == len(df):
                logger.info(
                    "Published [%d/%d] event_id=%s | key=%s | year=%d | partition=%d | offset=%d",
                    published_count,
                    len(df),
                    event_payload["event_id"],
                    event_key,
                    event_payload["year"],
                    record_metadata.partition,
                    record_metadata.offset,
                )

            if delay > 0:
                time.sleep(delay)

    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    finally:
        producer.flush()
        producer.close()
        logger.info(
            "Producer finished cleanly. Total valid events published: %d",
            published_count,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream historical World Happiness records as JSON events to Apache Kafka."
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument(
        "--csv-path",
        type=Path,
        default=DEFAULT_STREAM_CSV,
        help="Path to prepared streaming CSV file",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=float(os.getenv("STREAM_DELAY_SECONDS", "0.05")),
        help="Delay in seconds between consecutive events (default: 0.05)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional maximum number of records to publish (default: all 782 records)",
    )
    parser.add_argument(
        "--inject-invalid",
        action="store_true",
        help="Also emit 2 malformed events to test downstream consumer contract validation",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    stream_events(
        bootstrap_servers=args.bootstrap_servers,
        csv_path=args.csv_path,
        delay=args.delay,
        limit=args.limit,
        inject_invalid=args.inject_invalid,
    )
