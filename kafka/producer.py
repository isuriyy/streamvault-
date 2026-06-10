"""
producer.py
Replays Olist CSV files into Redpanda topics at a controlled rate.
Each topic runs in its own thread — all 5 topics produce concurrently.

Usage (via Docker):
    # Fast replay — ~20 msg/sec per topic
    docker compose -f docker-compose.redpanda.yml run --rm producer producer.py --data-dir /data/raw

    # Slow replay — watch messages arrive in the Console UI
    docker compose -f docker-compose.redpanda.yml run --rm producer producer.py --data-dir /data/raw --delay 1.0

Arguments:
    --data-dir   Path to CSV folder inside the container (default: /data/raw)
    --delay      Seconds between messages per topic thread (default: 0.05)
    --broker     Kafka broker (default: redpanda:9092 — internal Docker address)
"""

import argparse
import csv
import json
import time
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer

# ── Topic → CSV filename ──────────────────────────────────────────────────────
TOPIC_MAP = {
    "olist.orders":         "olist_orders_dataset.csv",
    "olist.order_items":    "olist_order_items_dataset.csv",
    "olist.order_payments": "olist_order_payments_dataset.csv",
    "olist.customers":      "olist_customers_dataset.csv",
    "olist.products":       "olist_products_dataset.csv",
}

# ── Kafka message key per topic (controls which partition a message lands on) ─
KEY_FIELD = {
    "olist.orders":         "order_id",
    "olist.order_items":    "order_id",
    "olist.order_payments": "order_id",
    "olist.customers":      "customer_id",
    "olist.products":       "product_id",
}


def delivery_report(err, msg):
    """Fired by librdkafka after each message is acknowledged by the broker."""
    if err is not None:
        print(f"  ✗ Delivery failed [{msg.topic()}]: {err}")


def enrich(row: dict, topic: str) -> dict:
    """
    Attach pipeline audit fields to every event before publishing.
    These land in the bronze layer as-is — full audit trail from ingestion.
    """
    row["_topic"]       = topic
    row["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    row["_event_id"]    = str(uuid.uuid4())
    return row


def stream_topic(topic: str, csv_path: Path, broker: str, delay: float):
    """
    Reads one CSV row-by-row and publishes each row as a JSON message.
    Runs in its own thread so all topics produce concurrently.
    """
    producer = Producer({
        "bootstrap.servers": broker,
        "linger.ms": 10,           # small batching window for throughput
        "compression.type": "lz4", # lightweight compression
    })

    key_field = KEY_FIELD[topic]
    count = 0

    print(f"  → [{topic}] starting — {csv_path.name}")

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            payload = enrich(dict(row), topic)
            key     = payload.get(key_field, str(count))

            producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=json.dumps(payload).encode("utf-8"),
                callback=delivery_report,
            )

            count += 1

            # Poll to serve delivery callbacks without blocking the thread
            producer.poll(0)

            # Progress log every 10,000 messages
            if count % 10000 == 0:
                print(f"  ✓ [{topic}] {count:,} messages sent")

            time.sleep(delay)

    # Flush any buffered messages before thread exits
    producer.flush()
    print(f"  ✓ [{topic}] complete — {count:,} total messages")


def main():
    parser = argparse.ArgumentParser(description="StreamVault Kafka replay producer")
    parser.add_argument("--data-dir", default="/data/raw",      help="Folder with Olist CSVs")
    parser.add_argument("--delay",    default=0.05, type=float, help="Seconds between messages per topic")
    parser.add_argument("--broker",   default="redpanda:9092",  help="Kafka broker address")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)

    # Validate all required CSVs exist before starting any thread
    missing = []
    for topic, filename in TOPIC_MAP.items():
        path = data_dir / filename
        if not path.exists():
            missing.append(f"  ✗ {path}")

    if missing:
        print("Cannot start — missing CSV files:")
        print("\n".join(missing))
        print(f"\nMount your data folder to {data_dir} in docker-compose.")
        return

    print("StreamVault — starting replay producer")
    print(f"  Broker   : {args.broker}")
    print(f"  Data dir : {data_dir}")
    print(f"  Delay    : {args.delay}s per message per topic")
    print(f"  Topics   : {len(TOPIC_MAP)}")
    print()

    # One thread per topic — all produce concurrently
    threads = []
    for topic, filename in TOPIC_MAP.items():
        t = threading.Thread(
            target=stream_topic,
            args=(topic, data_dir / filename, args.broker, args.delay),
            name=f"producer-{topic}",
            daemon=True,
        )
        threads.append(t)

    for t in threads:
        t.start()

    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        print("\nInterrupted — shutting down producer.")

    print("\nAll topics replayed.")


if __name__ == "__main__":
    main()
