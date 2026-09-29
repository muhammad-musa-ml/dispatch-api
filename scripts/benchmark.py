"""Bounded closed-loop intake microbenchmark. Not a capacity or delivery SLA test."""
import argparse
import concurrent.futures
import json
import math
import statistics
import time
import uuid
from pathlib import Path

from demo import request


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--requests", type=int, default=200)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--output", default="tmp/benchmark.json")
    args = parser.parse_args()
    if not (1 <= args.requests <= 2000 and 1 <= args.concurrency <= 64):
        parser.error("requests must be 1..2000 and concurrency 1..64")
    prefix = str(uuid.uuid4())
    payload = b'{"type":"benchmark.created","payload":{"synthetic":true}}'

    def send(i):
        start = time.perf_counter()
        code, _ = request("POST", "/v1/events", payload, f"{prefix}-{i}")
        return code, (time.perf_counter() - start) * 1000

    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        results = list(pool.map(send, range(args.requests)))
    elapsed = time.perf_counter() - started
    latencies = sorted(latency for _, latency in results)
    codes = {str(code): sum(c == code for c, _ in results) for code, _ in results}
    report = {"kind": "closed-loop local HTTP intake microbenchmark", "requests": args.requests,
              "concurrency": args.concurrency, "payload_bytes": len(payload), "elapsed_seconds": elapsed,
              "responses": codes, "requests_per_second": args.requests / elapsed,
              "latency_ms": {"median": statistics.median(latencies), "p95": latencies[math.ceil(.95 * len(latencies)) - 1]},
              "limitations": ["One local run; includes client and Docker networking overhead.",
                              "Measures durable acceptance, not completed delivery.",
                              "Closed-loop load can hide overload; this is not maximum sustainable capacity.",
                              "Receiver deliberately fails first two attempts; synthetic data only."]}
    target = Path(args.output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    if codes != {"201": args.requests}:
        raise SystemExit("Some intake requests failed; report is not a passing result")


if __name__ == "__main__":
    main()
