# Wikipulse
Real-time analytics for Wikipedia edits using Kafka and Kubernetes.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run the consumer

The consumer reads JSON edit events from the `wiki-edits` topic and prints each event's `event_id` and `title`, with its partition and offset.

```bash
export KAFKA_BOOTSTRAP_SERVERS=<kafka-host:port>
python consumer.py
```

Optional: `KAFKA_TOPIC` (default `wiki-edits`) and `KAFKA_GROUP_ID` (default `wikipulse-consumers`).
Press Ctrl+C to stop. On shutdown the consumer commits its offsets, so a restarted consumer continues from where it stopped.

Each message must be a JSON object with these fields:

```json
{"event_id": "test-1", "wiki": "enwiki", "title": "Sample edit", "timestamp": 1794528834, "is_bot": false}
```

Messages that are not valid JSON or are missing a field are skipped and logged to stderr.
