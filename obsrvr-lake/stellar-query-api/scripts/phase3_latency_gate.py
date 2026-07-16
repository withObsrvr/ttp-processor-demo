#!/usr/bin/env python3
"""Run the focused Phase 3 mainnet serving latency acceptance gate."""

import argparse
import csv
import json
import math
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class Route:
    name: str
    route_class: str
    path: str
    target_ms: int
    hard_timeout_ms: int


@dataclass(frozen=True)
class Target:
    name: str
    base_url: str
    headers: dict[str, str]


CSV_FIELDS = [
    "target",
    "base_url",
    "route",
    "path",
    "route_class",
    "phase",
    "run",
    "status",
    "total_ms",
    "bytes",
    "target_ms",
    "hard_timeout_ms",
    "error",
    "hard_pass",
]


def fetch(target: Target, path: str, timeout_ms: int) -> dict:
    url = target.base_url.rstrip("/") + path
    request = urllib.request.Request(url, headers=target.headers)
    started = time.perf_counter()
    status = 0
    body = b""
    error = ""
    try:
        with urllib.request.urlopen(request, timeout=timeout_ms / 1000) as response:
            status = response.status
            body = response.read()
    except urllib.error.HTTPError as exc:
        status = exc.code
        body = exc.read()
        error = f"http_{exc.code}"
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
    total_ms = round((time.perf_counter() - started) * 1000)
    return {
        "url": url,
        "status": status,
        "body": body,
        "bytes": len(body),
        "total_ms": total_ms,
        "error": error,
    }


def parse_json(result: dict, description: str) -> dict:
    if result["status"] < 200 or result["status"] >= 300:
        raise RuntimeError(
            f"{description} failed with HTTP {result['status']}: "
            f"{result['body'][:300].decode('utf-8', errors='replace')}"
        )
    try:
        value = json.loads(result["body"].decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"{description} returned invalid JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"{description} returned a non-object JSON response")
    return value


def discover_fixtures(
    target: Target,
    discovery_timeout_ms: int,
    tx_hash_override: str = "",
    account_override: str = "",
    smart_wallet_override: str = "",
) -> dict:
    ledgers = parse_json(
        fetch(target, "/api/v1/silver/ledgers/recent?limit=3", discovery_timeout_ms),
        "recent-ledger discovery",
    )
    transactions = parse_json(
        fetch(target, "/api/v1/silver/transactions/recent?limit=5", discovery_timeout_ms),
        "recent-transaction discovery",
    )
    wallets = parse_json(
        fetch(target, "/api/v1/silver/smart-wallets?limit=5", discovery_timeout_ms),
        "smart-wallet discovery",
    )

    transaction_rows = transactions.get("transactions") or []
    wallet_rows = wallets.get("wallets") or []
    if not transaction_rows and (not tx_hash_override or not account_override):
        raise RuntimeError("recent-transaction discovery returned no transactions")
    if not wallet_rows and not smart_wallet_override:
        raise RuntimeError("smart-wallet discovery returned no smart-wallet candidates")

    transaction = transaction_rows[0] if transaction_rows else {}
    latest_ledger = int(
        ledgers.get("latest_sequence")
        or transactions.get("latest_sequence")
        or transaction.get("ledger_sequence")
        or 0
    )
    tx_hash = tx_hash_override or str(transaction.get("tx_hash") or "")
    account = account_override or str(transaction.get("source_account") or "")
    smart_wallet = smart_wallet_override or str(
        wallet_rows[0].get("contract_id") if wallet_rows else ""
    )
    if latest_ledger <= 0 or not tx_hash or not account or not smart_wallet:
        raise RuntimeError("fixture discovery returned incomplete ledger/tx/account/wallet data")

    return {
        "latest_ledger": latest_ledger,
        "tx_hash": tx_hash,
        "account": account,
        "smart_wallet": smart_wallet,
    }


def build_routes(fixtures: dict, historical_ledger: int) -> list[Route]:
    tx_hash = urllib.parse.quote(fixtures["tx_hash"], safe="")
    account = urllib.parse.quote(fixtures["account"], safe="")
    smart_wallet = urllib.parse.quote(fixtures["smart_wallet"], safe="")
    return [
        Route("health", "fast_serving", "/health", 500, 2_000),
        Route(
            "recent_ledgers",
            "fast_serving",
            "/api/v1/silver/ledgers/recent?limit=10",
            500,
            2_000,
        ),
        Route(
            "recent_transactions",
            "fast_serving",
            "/api/v1/silver/transactions/recent?limit=10",
            500,
            2_000,
        ),
        Route(
            "transaction_receipt",
            "fast_serving",
            f"/api/v1/silver/tx/{tx_hash}/receipt",
            500,
            2_000,
        ),
        Route(
            "account_balances",
            "fast_serving",
            f"/api/v1/silver/accounts/{account}/balances",
            500,
            2_000,
        ),
        Route(
            "account_overview",
            "bounded_interactive",
            f"/api/v1/silver/explorer/account?account_id={account}",
            2_000,
            4_000,
        ),
        Route(
            "decoded_transaction_batch",
            "bounded_interactive",
            f"/api/v1/silver/tx/batch/decoded?hashes={tx_hash}",
            2_000,
            4_000,
        ),
        Route(
            "home_summary",
            "home",
            "/api/v1/home/summary",
            3_000,
            4_000,
        ),
        Route(
            "smart_wallet_lookup",
            "smart_wallet",
            f"/api/v1/silver/smart-wallet/{smart_wallet}",
            2_000,
            4_000,
        ),
        Route(
            "historical_ledger_detail",
            "historical",
            f"/api/v1/silver/ledger/{historical_ledger}/full",
            4_000,
            6_000,
        ),
    ]


def percentile(values: list[int], quantile: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    index = max(0, math.ceil(quantile * len(ordered)) - 1)
    return ordered[index]


def hard_pass(result: dict, route: Route) -> bool:
    return (
        200 <= result["status"] < 300
        and not result["error"]
        and result["status"] not in (502, 504)
        and result["total_ms"] <= route.hard_timeout_ms
    )


def run_gate(targets: list[Target], routes: list[Route], warm_runs: int) -> list[dict]:
    rows = []
    for target in targets:
        for route in routes:
            phases = [("cold", 1)] + [("warm", run) for run in range(1, warm_runs + 1)]
            for phase, run in phases:
                result = fetch(target, route.path, route.hard_timeout_ms)
                passed = hard_pass(result, route)
                row = {
                    "target": target.name,
                    "base_url": target.base_url,
                    "route": route.name,
                    "path": route.path,
                    "route_class": route.route_class,
                    "phase": phase,
                    "run": run,
                    "status": result["status"],
                    "total_ms": result["total_ms"],
                    "bytes": result["bytes"],
                    "target_ms": route.target_ms,
                    "hard_timeout_ms": route.hard_timeout_ms,
                    "error": result["error"],
                    "hard_pass": passed,
                }
                rows.append(row)
                verdict = "PASS" if passed else "FAIL"
                print(
                    f"{target.name:8} {route.name:27} {phase:4} "
                    f"run={run} status={result['status']} ms={result['total_ms']} {verdict} "
                    f"{result['error']}"
                )
                sys.stdout.flush()
    return rows


def summarize(rows: list[dict]) -> list[dict]:
    grouped: dict[tuple[str, str], list[dict]] = {}
    for row in rows:
        grouped.setdefault((row["target"], row["route"]), []).append(row)

    summary = []
    for (target, route), group in grouped.items():
        cold = [row for row in group if row["phase"] == "cold"]
        warm = [row for row in group if row["phase"] == "warm"]
        warm_times = [int(row["total_ms"]) for row in warm]
        target_ms = int(group[0]["target_ms"])
        summary.append(
            {
                "target": target,
                "route": route,
                "route_class": group[0]["route_class"],
                "base_url": group[0]["base_url"],
                "path": group[0]["path"],
                "cold_ms": int(cold[0]["total_ms"]) if cold else 0,
                "warm_min_ms": min(warm_times) if warm_times else 0,
                "warm_p50_ms": percentile(warm_times, 0.50),
                "warm_p95_ms": percentile(warm_times, 0.95),
                "warm_max_ms": max(warm_times) if warm_times else 0,
                "target_ms": target_ms,
                "hard_timeout_ms": int(group[0]["hard_timeout_ms"]),
                "target_pass": bool(warm_times) and percentile(warm_times, 0.95) < target_ms,
                "hard_pass": all(bool(row["hard_pass"]) for row in group),
                "statuses": sorted({int(row["status"]) for row in group}),
                "errors": sorted({str(row["error"]) for row in group if row["error"]}),
            }
        )
    return sorted(summary, key=lambda row: (row["target"], row["route"]))


def write_outputs(out_prefix: Path, metadata: dict, rows: list[dict], summary: list[dict]) -> None:
    out_prefix.parent.mkdir(parents=True, exist_ok=True)
    csv_path = out_prefix.with_suffix(".csv")
    json_path = out_prefix.with_suffix(".json")
    markdown_path = out_prefix.with_suffix(".md")

    with csv_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    document = {"metadata": metadata, "summary": summary}
    json_path.write_text(json.dumps(document, indent=2) + "\n")

    lines = [
        "# Phase 3 Mainnet Serving Latency Gate",
        "",
        f"- Recorded: `{metadata['recorded_at']}`",
        f"- Warm runs per route: `{metadata['warm_runs']}`",
        f"- Historical ledger fixture: `{metadata['historical_ledger']}`",
        f"- Transaction fixture: `{metadata['fixtures']['tx_hash']}`",
        f"- Account fixture: `{metadata['fixtures']['account']}`",
        f"- Smart-wallet fixture: `{metadata['fixtures']['smart_wallet']}`",
        "",
        "The cold value is the first request made by this audit process; it does not",
        "purge upstream, DuckDB, PostgreSQL, CDN, or Gateway caches. The acceptance",
        "gate requires every request to return 2xx within its hard timeout and rejects",
        "Gateway 502/504 responses. Initial p95 targets are reported separately.",
        "",
        "| Target | Route | Cold | Warm p50 | Warm p95 | Target | Hard | Result |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in summary:
        result = "PASS" if row["hard_pass"] else "FAIL"
        target_result = "target" if row["target_pass"] else "above target"
        lines.append(
            f"| {row['target']} | `{row['route']}` | {row['cold_ms']}ms | "
            f"{row['warm_p50_ms']}ms | {row['warm_p95_ms']}ms | "
            f"<{row['target_ms']}ms | {row['hard_timeout_ms']}ms | "
            f"{result}, {target_result} |"
        )
    lines.extend(
        [
            "",
            f"Hard gate: **{'PASS' if metadata['hard_gate_pass'] else 'FAIL'}**",
            f"Initial p95 targets: **{'PASS' if metadata['target_gate_pass'] else 'NOT YET MET'}**",
            "",
        ]
    )
    markdown_path.write_text("\n".join(lines))

    print(f"\nCSV: {csv_path}")
    print(f"JSON: {json_path}")
    print(f"Markdown: {markdown_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--direct-base",
        default="https://obsrvr-lake-mainnet.withobsrvr.com",
    )
    parser.add_argument(
        "--gateway-base",
        default="https://gateway.withobsrvr.com/lake/v1/mainnet",
    )
    parser.add_argument(
        "--gateway-api-key",
        default=os.environ.get("OBSRVR_GATEWAY_API_KEY", ""),
    )
    parser.add_argument("--warm-runs", type=int, default=5)
    parser.add_argument("--discovery-timeout-ms", type=int, default=6_000)
    parser.add_argument("--historical-ledger", type=int, default=40_000_000)
    parser.add_argument("--tx-hash", default="")
    parser.add_argument("--account", default="")
    parser.add_argument("--smart-wallet", default="")
    parser.add_argument(
        "--out-prefix",
        default="/tmp/mainnet-serving-phase3-latency",
    )
    parser.add_argument(
        "--strict-targets",
        action="store_true",
        help="Return nonzero when an initial p95 target is missed, not only the hard gate.",
    )
    args = parser.parse_args()

    if args.warm_runs < 1:
        parser.error("--warm-runs must be at least 1")
    if args.historical_ledger < 1:
        parser.error("--historical-ledger must be positive")

    direct = Target("direct", args.direct_base, {})
    gateway_headers = {}
    if args.gateway_api_key:
        gateway_headers["Authorization"] = "Api-Key " + args.gateway_api_key
    gateway = Target("gateway", args.gateway_base, gateway_headers)

    try:
        fixtures = discover_fixtures(
            direct,
            args.discovery_timeout_ms,
            tx_hash_override=args.tx_hash,
            account_override=args.account,
            smart_wallet_override=args.smart_wallet,
        )
    except RuntimeError as exc:
        print(f"fixture discovery failed: {exc}", file=sys.stderr)
        return 2

    routes = build_routes(fixtures, args.historical_ledger)
    rows = run_gate([direct, gateway], routes, args.warm_runs)
    summary = summarize(rows)
    hard_gate_pass = all(row["hard_pass"] for row in summary)
    target_gate_pass = all(row["target_pass"] for row in summary)
    metadata = {
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "warm_runs": args.warm_runs,
        "historical_ledger": args.historical_ledger,
        "fixtures": fixtures,
        "targets": [asdict(target) | {"headers": sorted(target.headers)} for target in [direct, gateway]],
        "hard_gate_pass": hard_gate_pass,
        "target_gate_pass": target_gate_pass,
    }
    write_outputs(Path(args.out_prefix), metadata, rows, summary)

    if not hard_gate_pass:
        return 1
    if args.strict_targets and not target_gate_pass:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
