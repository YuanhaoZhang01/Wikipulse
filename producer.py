import json
import os
import sys
from urllib.error import URLError
from urllib.request import Request, urlopen

from confluent_kafka import KafkaException, Producer


STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
EDIT_LIMIT = 5


def wikipedia_edits(stream):
    data_lines = []
    for raw_line in stream:
        line = raw_line.rstrip(b"\r\n")
        if line.startswith(b"data:"):
            data_lines.append(line[5:].removeprefix(b" "))
        if line or not data_lines:
            continue

        payload = b"\n".join(data_lines)
        data_lines.clear()
        try:
            event = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue

        if event.get("type") != "edit":
            continue
        if not event.get("server_name", "").endswith(".wikipedia.org"):
            continue

        yield {
            "event_id": event["meta"]["id"],
            "wiki": event["wiki"],
            "title": event["title"],
            "timestamp": event["timestamp"],
            "is_bot": event["bot"],
        }


def send_edit(producer, topic, edit):
    delivery_results = []

    def on_delivery(error, message):
        delivery_results.append(error)

    producer.produce(
        topic,
        key=edit["event_id"].encode("utf-8"),
        value=json.dumps(edit, ensure_ascii=True).encode("utf-8"),
        on_delivery=on_delivery,
    )
    pending = producer.flush(timeout=20)
    if pending or not delivery_results:
        raise RuntimeError(f"Kafka delivery timed out for {edit['event_id']}")
    if delivery_results[0] is not None:
        raise RuntimeError(f"Kafka delivery failed for {edit['event_id']}: {delivery_results[0]}")


def main():
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "").strip()
    if not bootstrap:
        print("Set KAFKA_BOOTSTRAP_SERVERS to the Kafka bootstrap address.", file=sys.stderr)
        return 2
    topic = os.environ.get("KAFKA_TOPIC", "wiki-edits")
    request = Request(
        STREAM_URL,
        headers={
            "Accept": "text/event-stream",
            "User-Agent": "WikiPulse/0.1 (CSE 4207 course project)",
        },
    )
    try:
        producer = Producer({
            "bootstrap.servers": bootstrap,
            "client.id": "wikipulse-producer",
            "enable.idempotence": True,
            "acks": "all",
            "message.timeout.ms": 15000,
        })
        with urlopen(request, timeout=20) as stream:
            for count, edit in enumerate(wikipedia_edits(stream), start=1):
                send_edit(producer, topic, edit)
                print(json.dumps(edit, ensure_ascii=True), flush=True)
                if count == EDIT_LIMIT:
                    return 0
    except (OSError, URLError, KafkaException, BufferError, RuntimeError, ValueError) as error:
        print(f"Producer failed: {error}", file=sys.stderr)
        return 1

    print("The stream ended before five edits arrived.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
