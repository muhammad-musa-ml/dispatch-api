"""End-to-end local demo; optionally kill the API after durable acceptance."""
import argparse
import json
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def request(method, path, body=None, key=None, token="local-demo-key-alpha-000001"):
    headers = {"Authorization": "Bearer " + token}
    if body is not None:
        headers["Content-Type"] = "application/json"
    if key:
        headers["Idempotency-Key"] = key
    req = urllib.request.Request("http://127.0.0.1:8080" + path, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as response:
        return response.code, json.load(response)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--crash", action="store_true")
    parser.add_argument("--output")
    args = parser.parse_args()
    body = b'{"type":"note.created","payload":{"note_id":"synthetic-demo"}}'
    key = str(uuid.uuid4())
    code, event = request("POST", "/v1/events", body, key)
    assert code == 201, (code, event)
    path = "/v1/events/" + event["id"]
    started = time.monotonic()
    if args.crash:
        subprocess.run(["docker", "compose", "kill", "-s", "SIGKILL", "api"], cwd=ROOT, check=True)
        subprocess.run(["docker", "compose", "up", "-d", "api"], cwd=ROOT, check=True)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                if request("GET", "/readyz")[0] == 200:
                    break
            except (OSError, urllib.error.URLError):
                pass
            time.sleep(0.25)
    code, replay = request("POST", "/v1/events", body, key)
    assert code == 200 and replay["id"] == event["id"], (code, replay)
    assert request("POST", "/v1/events", body + b" ", key)[0] == 409
    assert request("GET", path, token="local-demo-key-bravo-000002")[0] == 404
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        code, current = request("GET", path)
        assert code == 200
        if current["status"] != "accepted":
            break
        time.sleep(0.25)
    assert current["status"] == "delivered", current
    code, attempts = request("GET", path + "/attempts")
    assert code == 200 and attempts["data"][-1]["outcome"] == "delivered", attempts
    result = {"scenario": "process-kill-recovery" if args.crash else "transient-receiver-failure", "passed": True,
              "elapsed_seconds": round(time.monotonic() - started, 3), "event_id": event["id"],
              "same_key_same_id": True, "different_body_conflict": True, "cross_tenant_hidden": True,
              "attempts": attempts["data"], "scope": "Synthetic local Docker demonstration; no production SLA implied."}
    rendered = json.dumps(result, indent=2) + "\n"
    if args.output:
        Path(args.output).write_text(rendered, encoding="utf-8")
    print(rendered)


if __name__ == "__main__":
    main()
