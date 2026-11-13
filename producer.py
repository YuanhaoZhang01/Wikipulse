import json
import sys
from urllib.error import URLError
from urllib.request import Request, urlopen


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


def main():
    request = Request(
        STREAM_URL,
        headers={
            "Accept": "text/event-stream",
            "User-Agent": "WikiPulse/0.1 (CSE 4207 course project)",
        },
    )
    try:
        with urlopen(request, timeout=20) as stream:
            for count, edit in enumerate(wikipedia_edits(stream), start=1):
                print(json.dumps(edit, ensure_ascii=True), flush=True)
                if count == EDIT_LIMIT:
                    return 0
    except (OSError, URLError) as error:
        print(f"Could not read Wikimedia EventStreams: {error}", file=sys.stderr)
        return 1

    print("The stream ended before five edits arrived.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
