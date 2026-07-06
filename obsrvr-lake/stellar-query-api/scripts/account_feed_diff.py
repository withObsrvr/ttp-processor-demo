#!/usr/bin/env python3
"""Compare account transaction feed results against the federated path.

This is a deployment verification harness for ACCOUNT_TX_FEED_ENABLED=true.
It requests the same account/page from:

  source=federated
  source=feed

and compares the transaction hash sequence. It intentionally ignores nullable
render fields such as fee/memo while the feed is still in the first rollout.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


@dataclass
class Result:
    account: str
    source: str
    status: int
    duration_ms: int
    hashes: list[str]
    coverage_source: str
    error: str = ""


def get_json(url: str, headers: dict[str, str], timeout: float) -> tuple[int, dict[str, Any]]:
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, json.loads(raw.decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            body = {"error": raw}
        return exc.code, body


def fetch_account(base_url: str, account: str, source: str, limit: int, timeout: float, headers: dict[str, str]) -> Result:
    params = urllib.parse.urlencode({"limit": limit, "source": source})
    url = f"{base_url.rstrip('/')}/api/v1/silver/accounts/{account}/transactions?{params}"
    started = time.perf_counter()
    status, body = get_json(url, headers, timeout)
    duration_ms = int((time.perf_counter() - started) * 1000)
    if status != 200:
        return Result(account, source, status, duration_ms, [], "", json.dumps(body, sort_keys=True))
    txs = body.get("transactions") or []
    coverage = body.get("coverage") or {}
    return Result(
        account=account,
        source=source,
        status=status,
        duration_ms=duration_ms,
        hashes=[str(tx.get("transaction_hash", "")) for tx in txs],
        coverage_source=str(coverage.get("source", "")),
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True, help="Query API base URL, not gateway prefix")
    parser.add_argument("--account", action="append", required=True, help="Account to verify; repeatable")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--api-key", default="", help="Optional Api-Key value for gateway-style auth")
    args = parser.parse_args()

    headers = {"Accept": "application/json"}
    if args.api_key:
        headers["Authorization"] = f"Api-Key {args.api_key}"

    failures = 0
    rows: list[dict[str, Any]] = []
    for account in args.account:
        federated = fetch_account(args.base_url, account, "federated", args.limit, args.timeout, headers)
        feed = fetch_account(args.base_url, account, "feed", args.limit, args.timeout, headers)
        match = federated.status == 200 and feed.status == 200 and federated.hashes == feed.hashes
        if not match:
            failures += 1
        rows.append(
            {
                "account": account,
                "match": match,
                "federated_status": federated.status,
                "feed_status": feed.status,
                "federated_ms": federated.duration_ms,
                "feed_ms": feed.duration_ms,
                "federated_count": len(federated.hashes),
                "feed_count": len(feed.hashes),
                "federated_coverage_source": federated.coverage_source,
                "feed_coverage_source": feed.coverage_source,
                "federated_error": federated.error,
                "feed_error": feed.error,
                "first_mismatch": first_mismatch(federated.hashes, feed.hashes),
            }
        )

    print(json.dumps({"ok": failures == 0, "results": rows}, indent=2, sort_keys=True))
    return 1 if failures else 0


def first_mismatch(left: list[str], right: list[str]) -> dict[str, Any]:
    for idx, (a, b) in enumerate(zip(left, right)):
        if a != b:
            return {"index": idx, "federated": a, "feed": b}
    if len(left) != len(right):
        return {"index": min(len(left), len(right)), "federated_len": len(left), "feed_len": len(right)}
    return {}


if __name__ == "__main__":
    sys.exit(main())
