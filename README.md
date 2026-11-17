# Wikipulse
Real-time analytics for Wikipedia edits using Kafka and Kubernetes.

## Run the producer on Windows

Install Python 3.9 or newer, then run these commands in PowerShell from the repository folder:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
$env:KAFKA_BOOTSTRAP_SERVERS = "YOUR_KAFKA_HOST:PORT"
.\.venv\Scripts\python.exe producer.py
```

Replace the bootstrap address with one reachable from the computer running the script.
The Kafka topic must already exist. The default topic is `wiki-edits`; override it with `KAFKA_TOPIC`.
This prototype uses a plaintext Kafka connection without authentication.

The producer sends five real Wikipedia edits as JSON and prints each event only after Kafka confirms delivery.
Each event contains `event_id`, `wiki`, `title`, `timestamp`, and `is_bot`.
Kafka delivery failures, stream errors, or a stream ending before five edits produce a nonzero exit status.


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
