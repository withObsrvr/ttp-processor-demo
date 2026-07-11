#!/usr/bin/env python3
"""Compare key Horizon compatibility routes against SDF Horizon.

This is a pragmatic rollout gate, not a byte-for-byte Horizon verifier. It
checks status codes and selected high-signal fields for the routes that have
historically failed or returned partial data.
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


DEFAULT_ACCOUNT = "GBTHMMFWTAPFAHRGS33LKETZYJKBTNEENRN47EDZMZPT2BNCJO47GVQG"
DEFAULT_TX_HASH = "366bc4543a8fe66e09c021af35377c78df6e90e57f85582a0aad1617fcc027e8"
DEFAULT_OPERATION_ID = "13647365957242881"


@dataclass(frozen=True)
class Route:
    name: str
    path: str
    kind: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--obsrvr-base-url",
        default="https://obsrvr-lake-testnet.withobsrvr.com/api/v1/horizon-compat",
        help="Base URL for the Obsrvr Horizon compatibility API",
    )
    parser.add_argument(
        "--horizon-base-url",
        default="https://horizon-testnet.stellar.org",
        help="Base URL for SDF Horizon",
    )
    parser.add_argument("--account", default=DEFAULT_ACCOUNT)
    parser.add_argument("--tx-hash", default=DEFAULT_TX_HASH)
    parser.add_argument("--operation-id", default=DEFAULT_OPERATION_ID)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--json", action="store_true", help="Emit JSON result only")
    return parser.parse_args()


def join_url(base_url: str, path: str) -> str:
    return base_url.rstrip("/") + "/" + path.lstrip("/")


def fetch_json(url: str, timeout: float) -> tuple[int, float, Any]:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    started = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            elapsed = time.monotonic() - started
            return resp.status, elapsed, json.loads(raw.decode("utf-8")) if raw else None
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        elapsed = time.monotonic() - started
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            body = {"error": raw}
        return exc.code, elapsed, body
    except urllib.error.URLError as exc:
        elapsed = time.monotonic() - started
        return 0, elapsed, {"error": str(exc.reason)}


def hal_records(body: Any) -> list[dict[str, Any]]:
    if not isinstance(body, dict):
        return []
    embedded = body.get("_embedded")
    if not isinstance(embedded, dict):
        return []
    records = embedded.get("records")
    if not isinstance(records, list):
        return []
    return [record for record in records if isinstance(record, dict)]


def required_tx_fields(record: dict[str, Any]) -> list[str]:
    missing: list[str] = []
    for field in ("envelope_xdr", "result_xdr", "fee_meta_xdr", "signatures"):
        if not record.get(field):
            missing.append(field)
    return missing


def first_summary(body: Any) -> dict[str, Any]:
    records = hal_records(body)
    if records:
        return summarize_record(records[0])
    return {}


def summarize_record(record: dict[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for field in (
        "id",
        "paging_token",
        "hash",
        "ledger",
        "sequence",
        "type",
        "type_i",
        "created_at",
        "function",
        "address",
    ):
        if field in record:
            summary[field] = record[field]
    if "parameters" in record:
        params = record["parameters"]
        summary["parameters_count"] = len(params) if isinstance(params, list) else None
    return summary


def summarize(kind: str, body: Any) -> dict[str, Any]:
    if not isinstance(body, dict):
        return {"body_type": type(body).__name__}
    if kind == "collection":
        return {"records": len(hal_records(body)), "first": first_summary(body)}
    if kind == "account":
        balances = body.get("balances")
        return {
            "sequence": body.get("sequence"),
            "sequence_ledger": body.get("sequence_ledger"),
            "sequence_time": body.get("sequence_time"),
            "last_modified_ledger": body.get("last_modified_ledger"),
            "balances": len(balances) if isinstance(balances, list) else None,
        }
    if kind == "fee_stats":
        return {
            "last_ledger": body.get("last_ledger"),
            "last_ledger_base_fee": body.get("last_ledger_base_fee"),
            "ledger_capacity_usage": body.get("ledger_capacity_usage"),
        }
    if kind == "transaction":
        return {
            "hash": body.get("hash"),
            "ledger": body.get("ledger"),
            "paging_token": body.get("paging_token"),
            "missing_xdr": required_tx_fields(body),
            "has_preconditions": "preconditions" in body,
        }
    if kind == "operation":
        return summarize_record(body)
    return summarize_record(body)


def compare(kind: str, obs_status: int, obs_body: Any, hor_status: int, hor_body: Any) -> tuple[str, str]:
    # Status 0 means the request never reached the server (DNS/TLS/refused).
    # A run that could not fetch anything has verified nothing — it must never
    # gate a rollout green ("both status=0" used to compare equal and pass).
    if obs_status == 0 or hor_status == 0:
        return "fail", f"network failure obsrvr={obs_status} horizon={hor_status}"
    if obs_status != hor_status:
        if hor_status == 200 and obs_status != 200:
            return "fail", f"status obsrvr={obs_status} horizon={hor_status}"
        return "partial", f"status obsrvr={obs_status} horizon={hor_status}"
    if obs_status != 200:
        # Matching non-200s (e.g. both 404 on a rotted fixture) compared no data;
        # that is at most inconclusive, never a pass.
        return "partial", f"no data compared: both status={obs_status}"

    obs = summarize(kind, obs_body)
    hor = summarize(kind, hor_body)

    if kind == "collection":
        obs_records = obs.get("records")
        hor_records = hor.get("records")
        if obs_records != hor_records:
            return "partial", f"record_count obsrvr={obs_records} horizon={hor_records}"
        obs_first = obs.get("first") or {}
        hor_first = hor.get("first") or {}
        for field in ("hash", "id", "type", "type_i"):
            if field in hor_first and obs_first.get(field) != hor_first.get(field):
                return "partial", f"first.{field} obsrvr={obs_first.get(field)} horizon={hor_first.get(field)}"
        return "pass", f"records={obs_records}"

    if kind == "account":
        mismatches = []
        for field in ("sequence", "sequence_ledger", "sequence_time", "balances"):
            if obs.get(field) != hor.get(field):
                mismatches.append(field)
        if mismatches:
            return "partial", "mismatch " + ",".join(mismatches)
        return "pass", "selected account fields match"

    if kind == "fee_stats":
        if obs == hor:
            return "pass", "selected fee stats match"
        return "partial", f"obsrvr={obs} horizon={hor}"

    if kind == "transaction":
        if obs.get("hash") != hor.get("hash"):
            return "partial", f"hash obsrvr={obs.get('hash')} horizon={hor.get('hash')}"
        if obs.get("missing_xdr"):
            return "fail", f"missing_xdr={','.join(obs['missing_xdr'])}"
        if obs.get("has_preconditions") != hor.get("has_preconditions"):
            return "partial", "preconditions presence differs"
        return "pass", "transaction core fields present"

    if kind == "operation":
        obs_id = obs.get("id")
        hor_id = hor.get("id")
        if obs_id != hor_id:
            return "partial", f"id obsrvr={obs_id} horizon={hor_id}"
        for field in ("type", "type_i", "function", "address"):
            if field in hor and obs.get(field) != hor.get(field):
                return "partial", f"{field} obsrvr={obs.get(field)} horizon={hor.get(field)}"
        return "pass", "selected operation fields match"

    return "pass", "status=200"


def build_routes(args: argparse.Namespace) -> list[Route]:
    account = urllib.parse.quote(args.account)
    tx_hash = urllib.parse.quote(args.tx_hash)
    op_id = urllib.parse.quote(args.operation_id)
    return [
        Route("fee_stats", "fee_stats", "fee_stats"),
        Route("ledgers_latest", "ledgers?limit=1&order=desc", "collection"),
        Route("ledger_historical", "ledgers/3177525", "operation"),
        Route("account", f"accounts/{account}", "account"),
        Route("account_transactions", f"accounts/{account}/transactions?limit=1&order=desc", "collection"),
        Route("transaction_by_hash", f"transactions/{tx_hash}", "transaction"),
        Route("transaction_operations", f"transactions/{tx_hash}/operations?limit=5", "collection"),
        Route("transaction_effects", f"transactions/{tx_hash}/effects?limit=5", "collection"),
        Route("operations_latest", "operations?limit=1&order=desc", "collection"),
        Route("operation_by_id", f"operations/{op_id}", "operation"),
        Route("operation_effects", f"operations/{op_id}/effects?limit=5", "collection"),
        Route("payments_latest", "payments?limit=1&order=desc", "collection"),
        Route("effects_latest", "effects?limit=1&order=desc", "collection"),
        Route("account_operations", f"accounts/{account}/operations?limit=3&order=desc", "collection"),
        Route("account_payments", f"accounts/{account}/payments?limit=3&order=desc", "collection"),
        Route("account_effects", f"accounts/{account}/effects?limit=3&order=desc", "collection"),
    ]


def main() -> int:
    args = parse_args()
    results: list[dict[str, Any]] = []
    for route in build_routes(args):
        obs_url = join_url(args.obsrvr_base_url, route.path)
        hor_url = join_url(args.horizon_base_url, route.path)
        obs_status, obs_elapsed, obs_body = fetch_json(obs_url, args.timeout)
        hor_status, hor_elapsed, hor_body = fetch_json(hor_url, args.timeout)
        verdict, detail = compare(route.kind, obs_status, obs_body, hor_status, hor_body)
        results.append(
            {
                "name": route.name,
                "verdict": verdict,
                "detail": detail,
                "obsrvr_status": obs_status,
                "obsrvr_elapsed_seconds": round(obs_elapsed, 3),
                "horizon_status": hor_status,
                "horizon_elapsed_seconds": round(hor_elapsed, 3),
                "obsrvr_summary": summarize(route.kind, obs_body),
                "horizon_summary": summarize(route.kind, hor_body),
            }
        )

    compared = sum(
        1
        for result in results
        if result["obsrvr_status"] == 200 and result["horizon_status"] == 200
    )
    # ok requires every route to pass AND real 200-vs-200 comparisons to have
    # happened; a run that compared nothing must not green-light a rollout even
    # if a future change relaxes per-route verdicts.
    ok = compared > 0 and all(result["verdict"] == "pass" for result in results)
    output = {"ok": ok, "compared_200": compared, "results": results}
    if args.json:
        print(json.dumps(output, indent=2, sort_keys=True))
    else:
        for result in results:
            print(
                f"{result['verdict']:<7} {result['name']:<24} "
                f"obs={result['obsrvr_status']:>3} {result['obsrvr_elapsed_seconds']:>6.3f}s "
                f"hor={result['horizon_status']:>3} {result['horizon_elapsed_seconds']:>6.3f}s "
                f"{result['detail']}"
            )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
