#!/usr/bin/env python3
"""
EventFlow Load Test
-------------------
Fires N concurrent events at the /api/events endpoint and reports:
  - Total throughput (events/sec)
  - p50 / p95 / p99 latency
  - Success / failure counts

Usage:
  pip install requests
  python3 load_test.py                    # default: 1000 events, 20 threads
  python3 load_test.py --events 5000 --threads 50
"""

import argparse
import time
import json
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib.request
import urllib.error

BASE_URL = "http://localhost:8080/api/events"


def send_event(i: int) -> dict:
    payload = json.dumps({
        "type": "load.test.event",
        "source": f"load-tester-{i % 10}",   # 10 different sources to spread the load
        "payload": {"index": i, "data": "benchmark-payload"}
    }).encode("utf-8")

    req = urllib.request.Request(
        BASE_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    start = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            latency_ms = (time.monotonic() - start) * 1000
            return {"success": True, "status": resp.status, "latency_ms": latency_ms}
    except urllib.error.HTTPError as e:
        latency_ms = (time.monotonic() - start) * 1000
        return {"success": False, "status": e.code, "latency_ms": latency_ms}
    except Exception as e:
        latency_ms = (time.monotonic() - start) * 1000
        return {"success": False, "status": 0, "latency_ms": latency_ms, "error": str(e)}


def run(total_events: int, threads: int):
    print(f"\nEventFlow Load Test")
    print(f"  Target : {BASE_URL}")
    print(f"  Events : {total_events}")
    print(f"  Threads: {threads}")
    print(f"  Starting...\n")

    results = []
    wall_start = time.monotonic()

    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = {executor.submit(send_event, i): i for i in range(total_events)}
        completed = 0
        for future in as_completed(futures):
            results.append(future.result())
            completed += 1
            if completed % 100 == 0:
                print(f"  Progress: {completed}/{total_events}")

    wall_elapsed = time.monotonic() - wall_start

    successes = [r for r in results if r["success"]]
    failures = [r for r in results if not r["success"]]
    latencies = [r["latency_ms"] for r in successes]

    if not latencies:
        print("No successful requests — is the server running?")
        return

    latencies.sort()
    p50 = statistics.median(latencies)
    p95 = latencies[int(len(latencies) * 0.95)]
    p99 = latencies[int(len(latencies) * 0.99)]
    throughput = len(successes) / wall_elapsed

    print(f"\n{'=' * 45}")
    print(f"  RESULTS")
    print(f"{'=' * 45}")
    print(f"  Total sent      : {total_events}")
    print(f"  Successful      : {len(successes)}")
    print(f"  Failed          : {len(failures)}")
    print(f"  Elapsed time    : {wall_elapsed:.2f}s")
    print(f"  Throughput      : {throughput:.1f} events/sec")
    print(f"  Latency p50     : {p50:.1f} ms")
    print(f"  Latency p95     : {p95:.1f} ms")
    print(f"  Latency p99     : {p99:.1f} ms")
    print(f"  Min latency     : {min(latencies):.1f} ms")
    print(f"  Max latency     : {max(latencies):.1f} ms")
    print(f"{'=' * 45}\n")

    if failures:
        status_counts = {}
        for f in failures:
            status_counts[f["status"]] = status_counts.get(f["status"], 0) + 1
        print(f"  Failure breakdown: {status_counts}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EventFlow load test")
    parser.add_argument("--events", type=int, default=1000, help="Total events to send")
    parser.add_argument("--threads", type=int, default=20, help="Concurrent threads")
    args = parser.parse_args()

    run(args.events, args.threads)
