import os
import sys
import time

from confluent_kafka import Producer

from db import delete_batch, fetch_batch, open_db

POLL_INTERVAL = 0.5
BATCH_SIZE = 100

TOPIC = "adsb.raw"
BROKER = os.getenv("REDPANDA_BROKER", "localhost:19092")

DELIVERY_TIMEOUT = 10.0


def _extract_icao(raw: str) -> str | None:
    parts = raw.split(",", 6)
    if len(parts) < 5:
        return None
    return parts[4].strip() or None


def _delivery_callback(err: str | None, msg: object) -> None:
    if err is not None:
        topic = msg.topic() if hasattr(msg, "topic") else TOPIC
        sys.stderr.write(f"publish: delivery failed for {topic}: {err}\n")


def run() -> None:
    producer = Producer({"bootstrap.servers": BROKER})

    try:
        with open_db() as conn:
            while True:
                rows = fetch_batch(conn, limit=BATCH_SIZE)
                if rows:
                    ids = [row[0] for row in rows]
                    for _, raw, _ in rows:
                        key = _extract_icao(raw)
                        producer.produce(
                            TOPIC,
                            key=key,
                            value=raw,
                            callback=_delivery_callback,
                        )
                        producer.poll(0)

                    producer.flush(timeout=DELIVERY_TIMEOUT)
                    delete_batch(conn, ids)
                else:
                    time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        sys.stderr.write("publish: flushing pending messages...\n")
        producer.flush()
        sys.stderr.write("publish: shutting down\n")
