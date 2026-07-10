#!/usr/bin/env python3
"""Smoke test the Horizon compatibility API.

The script intentionally uses only the Python standard library so it can run
from an operator shell without installing project dependencies.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


DEFAULT_ACCOUNT = "GBTHMMFWTAPFAHRGS33LKETZYJKBTNEENRN47EDZMZPT2BNCJO47GVQG"
DEFAULT_TX_HASH = "366bc4543a8fe66e09c021af35377c78df6e90e57f85582a0aad1617fcc027e8"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url",
        default="https://obsrvr-lake-testnet.withobsrvr.com/api/v1/horizon-compat",
        help="Base URL for /api/v1/horizon-compat",
    )
    parser.add_argument("--account", default=DEFAULT_ACCOUNT, help="Account used for account route smoke")
    parser.add_argument("--tx-hash", default=DEFAULT_TX_HASH, help="Transaction hash used for transaction hydration smoke")
    parser.add_argument("--api-key", default="", help="Optional API key")
    parser.add_argument(
        "--api-key-header",
        default="Authorization",
        choices=("Authorization", "Api-Key"),
        help="Header to use when --api-key is provided",
    )
    parser.add_argument("--timeout", type=float, default=15.0, help="HTTP timeout in seconds")
    parser.add_argument("--health-url", default="", help="Optional service health URL to check before Horizon routes")
    parser.add_argument("--json", action="store_true", help="Emit JSON result only")
    return parser.parse_args()


def join_url(base_url: str, path: str) -> str:
    return base_url.rstrip("/") + "/" + path.lstrip("/")


def auth_headers(args: argparse.Namespace) -> dict[str, str]:
    headers: dict[str, str] = {"Accept": "application/json"}
    if not args.api_key:
        return headers
    if args.api_key_header == "Authorization":
        headers["Authorization"] = f"Api-Key {args.api_key}"
    else:
        headers["Api-Key"] = args.api_key
    return headers


def fetch_json(url: str, headers: dict[str, str], timeout: float) -> tuple[int, float, Any]:
    req = urllib.request.Request(url, headers=headers)
    started = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            elapsed = time.monotonic() - started
            body = json.loads(raw.decode("utf-8")) if raw else None
            return resp.status, elapsed, body
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


def first_hal_record(body: Any) -> dict[str, Any]:
    if not isinstance(body, dict):
        return {}
    embedded = body.get("_embedded")
    if not isinstance(embedded, dict):
        return {}
    records = embedded.get("records") or embedded.get("Records") or []
    if not records:
        return {}
    first = records[0]
    return first if isinstance(first, dict) else {}


def tx_hydration_errors(record: dict[str, Any]) -> list[str]:
    missing: list[str] = []
    for field in ("envelope_xdr", "result_xdr", "result_meta_xdr", "fee_meta_xdr"):
        if not record.get(field):
            missing.append(field)
    if not record.get("signatures"):
        missing.append("signatures")
    return missing


def result(name: str, url: str, status: int, elapsed: float, ok: bool, detail: str = "") -> dict[str, Any]:
    return {
        "name": name,
        "url": url,
        "status": status,
        "elapsed_seconds": round(elapsed, 3),
        "ok": ok,
        "detail": detail,
    }


def main() -> int:
    args = parse_args()
    headers = auth_headers(args)
    checks: list[dict[str, Any]] = []

    if args.health_url:
        status, elapsed, body = fetch_json(args.health_url, headers, args.timeout)
        detail = ""
        if isinstance(body, dict) and body.get("status"):
            detail = f"status={body['status']}"
        checks.append(result("health", args.health_url, status, elapsed, status == 200, detail))

    simple_routes = [
        ("fee_stats", "fee_stats"),
        ("ledgers", "ledgers?limit=1&order=desc"),
        ("account", f"accounts/{urllib.parse.quote(args.account)}"),
    ]
    for name, path in simple_routes:
        url = join_url(args.base_url, path)
        status, elapsed, body = fetch_json(url, headers, args.timeout)
        ok = status == 200 and isinstance(body, dict)
        checks.append(result(name, url, status, elapsed, ok))

    account_txs_url = join_url(
        args.base_url,
        f"accounts/{urllib.parse.quote(args.account)}/transactions?limit=1&order=desc",
    )
    status, elapsed, body = fetch_json(account_txs_url, headers, args.timeout)
    account_tx = first_hal_record(body)
    missing = tx_hydration_errors(account_tx)
    checks.append(
        result(
            "account_transactions",
            account_txs_url,
            status,
            elapsed,
            status == 200 and not missing,
            f"hash={account_tx.get('hash', '')} missing={','.join(missing)}",
        )
    )

    tx_url = join_url(args.base_url, f"transactions/{urllib.parse.quote(args.tx_hash)}")
    status, elapsed, body = fetch_json(tx_url, headers, args.timeout)
    tx_record = body if isinstance(body, dict) else {}
    missing = tx_hydration_errors(tx_record)
    checks.append(
        result(
            "transaction_by_hash",
            tx_url,
            status,
            elapsed,
            status == 200 and not missing,
            f"hash={tx_record.get('hash', '')} missing={','.join(missing)}",
        )
    )

    ok = all(check["ok"] for check in checks)
    output = {"ok": ok, "checks": checks}
    if args.json:
        print(json.dumps(output, indent=2, sort_keys=True))
    else:
        for check in checks:
            marker = "ok" if check["ok"] else "FAIL"
            detail = f" {check['detail']}" if check["detail"] else ""
            print(f"{marker:4} {check['name']:24} {check['status']:>3} {check['elapsed_seconds']:>6.3f}s{detail}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
