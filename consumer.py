"""Read Wikipedia edit events from the Kafka topic and print them.

Usage:
    export KAFKA_BOOTSTRAP_SERVERS=<host:port>
    python consumer.py

Optional environment variables:
    KAFKA_TOPIC     topic to read (default: wiki-edits)
    KAFKA_GROUP_ID  consumer group (default: wikipulse-consumers)
"""

import json
import os
import signal
import sys

from confluent_kafka import Consumer, KafkaError

REQUIRED_FIELDS = ("event_id", "wiki", "title", "timestamp", "is_bot")


def parse_event(raw):
    """Turn one Kafka message value into a dict, or return None if it is not a valid edit event."""
    try:
        event = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
        return None
    if not isinstance(event, dict):
        return None
    if any(field not in event for field in REQUIRED_FIELDS):
        return None
    return event


def build_consumer():
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap:
        print("Set KAFKA_BOOTSTRAP_SERVERS to the Kafka address, e.g. 127.0.0.1:9094", file=sys.stderr)
        sys.exit(2)
    return Consumer({
        "bootstrap.servers": bootstrap,
        # Consumers in the same group split the topic's partitions; KEDA scales this group.
        "group.id": os.environ.get("KAFKA_GROUP_ID", "wikipulse-consumers"),
        # With no committed offset yet, start from the oldest message so nothing is skipped.
        "auto.offset.reset": "earliest",
    })


def main():
    topic = os.environ.get("KAFKA_TOPIC", "wiki-edits")
    consumer = build_consumer()
    consumer.subscribe([topic])

    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)  # Kubernetes sends SIGTERM when a pod is deleted

    print(f"Listening on topic '{topic}'. Press Ctrl+C to stop.", flush=True)
    received = skipped = 0
    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Kafka error: {msg.error()}", file=sys.stderr, flush=True)
                continue

            event = parse_event(msg.value())
            if event is None:
                skipped += 1
                print(f"Skipped invalid message at partition {msg.partition()}, offset {msg.offset()}",
                      file=sys.stderr, flush=True)
                continue

            received += 1
            print(f"{event['event_id']}  {event['title']}  "
                  f"(partition {msg.partition()}, offset {msg.offset()})", flush=True)
    finally:
        # close() commits the latest offsets, so a restarted consumer continues where this one stopped.
        consumer.close()
        print(f"Stopped. Received {received} events, skipped {skipped}.", flush=True)


if __name__ == "__main__":
    main()
