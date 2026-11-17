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
