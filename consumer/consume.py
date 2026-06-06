import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Consumer, KafkaError

from parquet import flush
from schema import FIELDS

TOPIC = "adsb.raw"
BROKER = os.getenv("REDPANDA_BROKER", "localhost:19092")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "data")
FLUSH_INTERVAL = float(os.getenv("FLUSH_INTERVAL_SECONDS", "300"))


def _parse(raw: str, kafka_ts_ms: int) -> dict[str, Any]:
    parts = raw.rstrip("\n").split(",")
    record: dict[str, Any] = {}
    for i, field in enumerate(FIELDS):
        record[field] = parts[i].strip() if i < len(parts) else None
    record["kafka_timestamp"] = datetime.fromtimestamp(
        kafka_ts_ms / 1000, tz=timezone.utc
    )
    return record


def run() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": BROKER,
            "group.id": "parquet-sink",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])

    buffer: list[dict[str, Any]] = []
    last_flush = time.monotonic()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        sys.stderr.write(f"consumer: error: {msg.error()}\n")
                else:
                    raw = msg.value().decode("utf-8", errors="replace")
                    _, ts_ms = msg.timestamp()
                    buffer.append(_parse(raw, ts_ms))

            if time.monotonic() - last_flush >= FLUSH_INTERVAL:
                flush(buffer, OUTPUT_DIR)
                buffer.clear()
                consumer.commit()
                last_flush = time.monotonic()

    except KeyboardInterrupt:
        sys.stderr.write("consumer: flushing before shutdown...\n")
        flush(buffer, OUTPUT_DIR)
        consumer.commit()
    finally:
        consumer.close()
        sys.stderr.write("consumer: shutting down\n")
