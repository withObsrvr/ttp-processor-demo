#!/usr/bin/env python3
"""Compare account-ledger-index pruning between DuckLake and Postgres sources.

Run this against two Query API deployments:

  * one with ACCOUNT_LEDGER_INDEX_SOURCE=ducklake
  * one with ACCOUNT_LEDGER_INDEX_SOURCE=postgres

The script asks each deployment for the same account transaction page and
compares the API-exposed account-index coverage metadata, especially
coverage.account_index.pruned_ranges. It does not require direct database
access and is intended as the safe gate before switching production reads to
the Postgres index-plane copy.
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
class CheckResult:
    label: str
    account: str
    status: int
    duration_ms: int
    index_status: str
    pruned_ranges: list[int]
    lookup_failed: bool
    skipped_cold: bool
    tx_hashes: list[str]
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


def fetch(
    label: str,
    base_url: str,
    account: str,
    limit: int,
    timeout: float,
    headers: dict[str, str],
    start_ledger: int | None,
    end_ledger: int | None,
) -> CheckResult:
    params: dict[str, Any] = {"limit": limit, "source": "federated"}
    if start_ledger is not None:
        params["start_ledger"] = start_ledger
    if end_ledger is not None:
        params["end_ledger"] = end_ledger
    url = f"{base_url.rstrip('/')}/api/v1/silver/accounts/{account}/transactions?{urllib.parse.urlencode(params)}"

    started = time.perf_counter()
    status, body = get_json(url, headers, timeout)
    duration_ms = int((time.perf_counter() - started) * 1000)
    if status != 200:
        return CheckResult(label, account, status, duration_ms, "", [], False, False, [], json.dumps(body, sort_keys=True))

    txs = body.get("transactions") or []
    coverage = body.get("coverage") or {}
    account_index = coverage.get("account_index") or {}
    return CheckResult(
        label=label,
        account=account,
        status=status,
        duration_ms=duration_ms,
        index_status=str(account_index.get("status", "")),
        pruned_ranges=[int(v) for v in account_index.get("pruned_ranges") or []],
        lookup_failed=bool(account_index.get("lookup_failed", False)),
        skipped_cold=bool(account_index.get("skipped_cold", False)),
        tx_hashes=[str(tx.get("transaction_hash", "")) for tx in txs],
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ducklake-url", required=True, help="Query API base URL using ACCOUNT_LEDGER_INDEX_SOURCE=ducklake")
    parser.add_argument("--postgres-url", required=True, help="Query API base URL using ACCOUNT_LEDGER_INDEX_SOURCE=postgres")
    parser.add_argument("--account", action="append", required=True, help="Account to check; repeatable")
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--start-ledger", type=int)
    parser.add_argument("--end-ledger", type=int)
    parser.add_argument("--api-key", default="", help="Optional Api-Key value for gateway-style auth")
    args = parser.parse_args()

    headers = {"Accept": "application/json"}
    if args.api_key:
        headers["Authorization"] = f"Api-Key {args.api_key}"

    failures = 0
    rows: list[dict[str, Any]] = []
    for account in args.account:
        ducklake = fetch("ducklake", args.ducklake_url, account, args.limit, args.timeout, headers, args.start_ledger, args.end_ledger)
        postgres = fetch("postgres", args.postgres_url, account, args.limit, args.timeout, headers, args.start_ledger, args.end_ledger)
        match = (
            ducklake.status == 200
            and postgres.status == 200
            and not ducklake.lookup_failed
            and not postgres.lookup_failed
            and ducklake.pruned_ranges == postgres.pruned_ranges
        )
        if not match:
            failures += 1
        rows.append({
            "account": account,
            "match": match,
            "ducklake": result_row(ducklake),
            "postgres": result_row(postgres),
            "range_diff": range_diff(ducklake.pruned_ranges, postgres.pruned_ranges),
        })

    print(json.dumps({"ok": failures == 0, "results": rows}, indent=2, sort_keys=True))
    return 1 if failures else 0


def result_row(result: CheckResult) -> dict[str, Any]:
    return {
        "status": result.status,
        "duration_ms": result.duration_ms,
        "index_status": result.index_status,
        "lookup_failed": result.lookup_failed,
        "skipped_cold": result.skipped_cold,
        "pruned_ranges": result.pruned_ranges,
        "tx_count": len(result.tx_hashes),
        "error": result.error,
    }


def range_diff(left: list[int], right: list[int]) -> dict[str, list[int]]:
    left_set = set(left)
    right_set = set(right)
    return {
        "ducklake_only": sorted(left_set - right_set),
        "postgres_only": sorted(right_set - left_set),
    }


if __name__ == "__main__":
    sys.exit(main())
