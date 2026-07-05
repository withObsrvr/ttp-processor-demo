#!/usr/bin/env python3
import argparse
import csv
import json
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Endpoint:
    category: str
    name: str
    path: str


CSV_FIELDNAMES = [
    "category",
    "name",
    "path",
    "run",
    "status",
    "total_ms",
    "bytes",
    "error",
    "class",
]


def fetch(base, path, timeout, headers=None):
    url = base.rstrip("/") + path
    req = urllib.request.Request(url, headers=headers or {})
    start = time.perf_counter()
    status = 0
    body = b""
    error = ""
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = resp.status
            body = resp.read()
    except urllib.error.HTTPError as e:
        status = e.code
        body = e.read()
        error = f"http_{e.code}"
    except Exception as e:
        error = type(e).__name__ + ": " + str(e)
    total_ms = int((time.perf_counter() - start) * 1000)
    return {
        "url": url,
        "status": status,
        "bytes": len(body),
        "total_ms": total_ms,
        "error": error,
        "body": body,
    }


def parse_json(result, fallback):
    if result["status"] < 200 or result["status"] >= 300:
        return fallback
    try:
        return json.loads(result["body"].decode("utf-8"))
    except Exception:
        return fallback


def first_item(obj, key):
    value = obj.get(key)
    if isinstance(value, list) and value:
        return value[0]
    return {}


def build_endpoints(base, timeout, headers):
    ledgers = parse_json(fetch(base, "/api/v1/silver/ledgers/recent?limit=3", timeout, headers), {})
    txs = parse_json(fetch(base, "/api/v1/silver/transactions/recent?limit=5", timeout, headers), {})
    boundaries = parse_json(fetch(base, "/api/v1/silver/data-boundaries", timeout, headers), {})

    latest = int(ledgers.get("latest_sequence") or boundaries.get("available_ledgers", {}).get("latest") or 0)
    hot_ledger = latest
    older_ledger = max(162, latest - 50000) if latest else 162
    very_old_ledger = 40000000 if latest > 40000000 else 162

    tx = first_item(txs, "transactions")
    tx_hash = tx.get("tx_hash") or "fb92b84c633be2d4643acf55772238a77695d2cd23cabb849700ddd7a14ba115"
    account = tx.get("source_account") or "GCFOH4PUYAXJH75SLBXT7NZWOT2JWXGCBI6YF3RXV6LXUHJCCKA4HH4I"
    contracts = tx.get("summary", {}).get("involved_contracts") or []
    contract = contracts[0] if contracts else "CALAG3KKOP7QA4XTIUHPU3DJ5GGJE2HRWK5MAVD326XHEGEE5EVHFF7L"
    asset = "XLM"

    endpoints = [
        Endpoint("core", "health", "/health"),
        Endpoint("core", "data_boundaries", "/api/v1/silver/data-boundaries"),
        Endpoint("home", "home_summary", "/api/v1/home/summary"),
        Endpoint("home", "explorer_summary", "/api/v1/explorer/summary"),
        Endpoint("silver_recent", "ledgers_recent", "/api/v1/silver/ledgers/recent?limit=10"),
        Endpoint("silver_recent", "transactions_recent", "/api/v1/silver/transactions/recent?limit=10"),
        Endpoint("silver_recent", "transaction_summaries", "/api/v1/silver/transactions/summaries?limit=10"),
        Endpoint("stats", "network", "/api/v1/silver/stats/network"),
        Endpoint("stats", "fees", "/api/v1/silver/stats/fees"),
        Endpoint("stats", "soroban", "/api/v1/silver/stats/soroban"),
        Endpoint("index", "index_health", "/api/v1/index/health"),
        Endpoint("index", "tx_index_lookup", f"/api/v1/index/transactions/{tx_hash}"),
        Endpoint("index", "contract_index_health", "/api/v1/index/contracts/health"),
        Endpoint("index", "contract_index_ledgers", f"/api/v1/index/contracts/{contract}/ledgers?limit=10"),
        Endpoint("index", "contract_index_summary", f"/api/v1/index/contracts/{contract}/summary"),
        Endpoint("ledger_hot", "ledger_summary_hot", f"/api/v1/silver/ledger/{hot_ledger}/summary"),
        Endpoint("ledger_hot", "ledger_full_hot", f"/api/v1/silver/ledger/{hot_ledger}/full"),
        Endpoint("ledger_cold", "ledger_summary_older", f"/api/v1/silver/ledger/{older_ledger}/summary"),
        Endpoint("ledger_cold", "ledger_full_older", f"/api/v1/silver/ledger/{older_ledger}/full"),
        Endpoint("ledger_cold", "ledger_full_very_old", f"/api/v1/silver/ledger/{very_old_ledger}/full"),
        Endpoint("accounts", "accounts_list", "/api/v1/silver/accounts?limit=5"),
        Endpoint("accounts", "account_current", f"/api/v1/silver/accounts/current?account_id={account}"),
        Endpoint("accounts", "account_history", f"/api/v1/silver/accounts/history?account_id={account}&limit=10"),
        Endpoint("accounts", "account_balances", f"/api/v1/silver/accounts/{account}/balances"),
        Endpoint("accounts", "account_transactions", f"/api/v1/silver/accounts/{account}/transactions?limit=5"),
        Endpoint("accounts", "account_offers", f"/api/v1/silver/accounts/{account}/offers?limit=5"),
        Endpoint("accounts", "account_contracts", f"/api/v1/silver/accounts/{account}/contracts?limit=5"),
        Endpoint("accounts", "account_overview", f"/api/v1/silver/explorer/account?account_id={account}"),
        Endpoint("accounts", "address_balances", f"/api/v1/silver/addresses/{account}/balances"),
        Endpoint("accounts", "address_token_portfolio", f"/api/v1/silver/address/{account}/token-balances"),
        Endpoint("activity", "operations_enriched", "/api/v1/silver/operations/enriched?limit=10"),
        Endpoint("activity", "payments", "/api/v1/silver/payments?limit=10"),
        Endpoint("activity", "soroban_ops", "/api/v1/silver/operations/soroban?limit=10"),
        Endpoint("activity", "token_transfers", "/api/v1/silver/transfers?limit=10"),
        Endpoint("activity", "transfer_stats", "/api/v1/silver/transfers/stats?group_by=asset"),
        Endpoint("activity", "events", "/api/v1/silver/events?limit=10"),
        Endpoint("activity", "generic_events", "/api/v1/silver/events/generic?limit=10"),
        Endpoint("tx", "tx_receipt", f"/api/v1/silver/tx/{tx_hash}/receipt"),
        Endpoint("tx", "tx_diffs", f"/api/v1/silver/tx/{tx_hash}/diffs"),
        Endpoint("tx", "tx_events", f"/api/v1/silver/tx/{tx_hash}/events"),
        Endpoint("tx", "tx_effects", f"/api/v1/silver/tx/{tx_hash}/effects"),
        Endpoint("tx", "tx_decoded", f"/api/v1/silver/tx/{tx_hash}/decoded"),
        Endpoint("tx", "tx_semantic", f"/api/v1/silver/tx/{tx_hash}/semantic"),
        Endpoint("tx", "tx_full", f"/api/v1/silver/tx/{tx_hash}/full"),
        Endpoint("tx", "tx_contracts_summary", f"/api/v1/silver/tx/{tx_hash}/contracts-summary"),
        Endpoint("contracts", "contract_storage", f"/api/v1/silver/contracts/{contract}/storage?limit=25"),
        Endpoint("contracts", "contract_recent_calls", f"/api/v1/silver/contracts/{contract}/recent-calls?limit=10"),
        Endpoint("contracts", "contract_analytics", f"/api/v1/silver/contracts/{contract}/analytics"),
        Endpoint("contracts", "contract_metadata", f"/api/v1/silver/contracts/{contract}/metadata"),
        Endpoint("contracts", "contract_events", f"/api/v1/silver/events/contract/{contract}?limit=10"),
        Endpoint("assets", "assets_list", "/api/v1/silver/assets?limit=10"),
        Endpoint("assets", "asset_detail_xlm", f"/api/v1/silver/assets/{urllib.parse.quote(asset)}"),
        Endpoint("assets", "asset_stats_xlm", f"/api/v1/silver/assets/{urllib.parse.quote(asset)}/stats"),
        Endpoint("assets", "asset_holders_xlm", f"/api/v1/silver/assets/{urllib.parse.quote(asset)}/holders?limit=10"),
        Endpoint("assets", "asset_pairs_xlm", f"/api/v1/silver/assets/{urllib.parse.quote(asset)}/pairs?limit=10"),
        Endpoint("semantic", "semantic_activities", "/api/v1/semantic/activities?limit=10"),
        Endpoint("semantic", "semantic_contracts", "/api/v1/semantic/contracts?limit=10"),
        Endpoint("semantic", "semantic_flows", "/api/v1/semantic/flows?limit=10"),
        Endpoint("semantic", "semantic_assets", "/api/v1/semantic/assets?limit=10"),
        Endpoint("semantic", "defi_protocols", "/api/v1/semantic/defi/protocols"),
        Endpoint("semantic", "defi_markets", "/api/v1/semantic/defi/markets"),
        Endpoint("semantic", "defi_status", "/api/v1/semantic/defi/status"),
        Endpoint("bronze", "bronze_stats_network", "/api/v1/bronze/stats/network"),
        Endpoint("bronze", "bronze_ledgers_recent", f"/api/v1/bronze/ledgers?start={hot_ledger}&end={hot_ledger}&limit=1"),
        Endpoint("bronze", "bronze_transactions", "/api/v1/bronze/transactions?limit=10"),
        Endpoint("bronze", "bronze_operations", "/api/v1/bronze/operations?limit=10"),
        Endpoint("bronze", "bronze_contract_events", "/api/v1/bronze/contract_events?limit=10"),
    ]
    return endpoints, {
        "latest_ledger": latest,
        "hot_ledger": hot_ledger,
        "older_ledger": older_ledger,
        "very_old_ledger": very_old_ledger,
        "tx_hash": tx_hash,
        "account": account,
        "contract": contract,
    }


def bucket(ms, status, error):
    if error:
        return "error"
    if status >= 500:
        return "server_error"
    if status >= 400:
        return "client_error"
    if ms < 250:
        return "fast"
    if ms < 1000:
        return "ok"
    if ms < 3000:
        return "slow"
    return "critical"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="https://obsrvr-lake-testnet.withobsrvr.com")
    parser.add_argument("--runs", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=12.0)
    parser.add_argument("--out", default="/tmp/stellar-api-latency-audit.csv")
    parser.add_argument("--api-key", default="")
    args = parser.parse_args()

    headers = {}
    if args.api_key:
        headers["Authorization"] = "Api-Key " + args.api_key

    endpoints, seeds = build_endpoints(args.base, args.timeout, headers)
    rows = []
    print(json.dumps({"base": args.base, "runs": args.runs, "timeout": args.timeout, "seeds": seeds}, indent=2))
    for endpoint in endpoints:
        for run in range(1, args.runs + 1):
            result = fetch(args.base, endpoint.path, args.timeout, headers)
            rows.append({
                "category": endpoint.category,
                "name": endpoint.name,
                "path": endpoint.path,
                "run": run,
                "status": result["status"],
                "total_ms": result["total_ms"],
                "bytes": result["bytes"],
                "error": result["error"],
                "class": bucket(result["total_ms"], result["status"], result["error"]),
            })
            print(f"{endpoint.category:12} {endpoint.name:28} run={run} status={result['status']} ms={result['total_ms']} class={rows[-1]['class']} err={result['error']}")
            sys.stdout.flush()

    out = Path(args.out)
    with out.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    grouped = {}
    for row in rows:
        grouped.setdefault(row["name"], []).append(row)

    summary = []
    for name, group in grouped.items():
        times = [int(r["total_ms"]) for r in group]
        statuses = sorted({str(r["status"]) for r in group})
        errors = sorted({r["error"] for r in group if r["error"]})
        summary.append({
            "category": group[0]["category"],
            "name": name,
            "status": ",".join(statuses),
            "min_ms": min(times),
            "p50_ms": int(statistics.median(times)),
            "max_ms": max(times),
            "class": bucket(max(times), max(int(r["status"]) for r in group), errors[0] if errors else ""),
            "errors": "; ".join(errors),
        })

    summary.sort(key=lambda x: ({"error": 0, "server_error": 1, "critical": 2, "slow": 3, "ok": 4, "fast": 5, "client_error": 6}.get(x["class"], 9), -x["max_ms"]))
    print("\nSUMMARY")
    print("class,category,name,status,min_ms,p50_ms,max_ms,errors")
    for row in summary:
        print(f"{row['class']},{row['category']},{row['name']},{row['status']},{row['min_ms']},{row['p50_ms']},{row['max_ms']},{row['errors']}")
    print(f"\nCSV written to {out}")


if __name__ == "__main__":
    main()
