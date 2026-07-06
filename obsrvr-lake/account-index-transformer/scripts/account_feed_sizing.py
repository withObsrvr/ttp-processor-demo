#!/usr/bin/env python3
"""Estimate account serving-feed size from silver_hot.

This is the M0 gate for the account transaction serving feed. It counts distinct
(account_id, transaction_hash) pairs over a ledger window using the same hot
participant sources as the account index transformer, then extrapolates to a
target ledger span.

The script shells out to psql so it can use the standard PGHOST/PGPORT/PGUSER/
PGPASSWORD/PGDATABASE environment variables already used operationally.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass


@dataclass
class Bounds:
    min_ledger: int
    max_ledger: int


@dataclass
class Sample:
    rows: int
    accounts: int
    transactions: int


def run_psql(args: argparse.Namespace, sql: str) -> str:
    cmd = [
        args.psql,
        "-X",
        "-v",
        "ON_ERROR_STOP=1",
        "-At",
        "-F",
        "\t",
    ]
    if args.dsn:
        cmd.extend([args.dsn])
    cmd.extend(["-c", sql])
    try:
        result = subprocess.run(cmd, check=True, text=True, capture_output=True)
    except FileNotFoundError:
        raise SystemExit(f"psql executable not found: {args.psql}")
    except subprocess.CalledProcessError as exc:
        sys.stderr.write(exc.stderr)
        raise SystemExit(exc.returncode)
    return result.stdout.strip()


def fetch_bounds(args: argparse.Namespace) -> Bounds:
    sql = f"""
        SELECT COALESCE(MIN(ledger_sequence), 0), COALESCE(MAX(ledger_sequence), 0)
        FROM {qualified(args.schema, "enriched_history_operations")}
    """
    out = run_psql(args, sql)
    min_ledger, max_ledger = parse_ints(out, 2)
    return Bounds(min_ledger=min_ledger, max_ledger=max_ledger)


def fetch_sample(args: argparse.Namespace, start: int, end: int) -> Sample:
    tables = {
        "eho": qualified(args.schema, "enriched_history_operations"),
        "transfers": qualified(args.schema, "token_transfers_raw"),
        "invocations": qualified(args.schema, "contract_invocations_raw"),
    }
    sql = f"""
        WITH participants AS (
            SELECT source_account AS account_id, transaction_hash
            FROM {tables["eho"]}
            WHERE ledger_sequence BETWEEN {start} AND {end}
              AND source_account IS NOT NULL AND source_account <> ''
            UNION
            SELECT destination AS account_id, transaction_hash
            FROM {tables["eho"]}
            WHERE ledger_sequence BETWEEN {start} AND {end}
              AND destination IS NOT NULL AND destination <> ''
            UNION
            SELECT from_account AS account_id, transaction_hash
            FROM {tables["eho"]}
            WHERE ledger_sequence BETWEEN {start} AND {end}
              AND from_account IS NOT NULL AND from_account <> ''
            UNION
            SELECT to_address AS account_id, transaction_hash
            FROM {tables["eho"]}
            WHERE ledger_sequence BETWEEN {start} AND {end}
              AND to_address IS NOT NULL AND to_address <> ''
            UNION
            SELECT address AS account_id, transaction_hash
            FROM {tables["eho"]}
            WHERE ledger_sequence BETWEEN {start} AND {end}
              AND address IS NOT NULL AND address <> ''
            UNION
            SELECT into_account AS account_id, transaction_hash
            FROM {tables["eho"]}
            WHERE ledger_sequence BETWEEN {start} AND {end}
              AND into_account IS NOT NULL AND into_account <> ''
            UNION
            SELECT from_account AS account_id, transaction_hash
            FROM {tables["transfers"]}
            WHERE ledger_sequence BETWEEN {start} AND {end}
              AND from_account IS NOT NULL AND from_account <> ''
            UNION
            SELECT to_account AS account_id, transaction_hash
            FROM {tables["transfers"]}
            WHERE ledger_sequence BETWEEN {start} AND {end}
              AND to_account IS NOT NULL AND to_account <> ''
            UNION
            SELECT source_account AS account_id, transaction_hash
            FROM {tables["invocations"]}
            WHERE ledger_sequence BETWEEN {start} AND {end}
              AND source_account IS NOT NULL AND source_account <> ''
        )
        SELECT COUNT(*), COUNT(DISTINCT account_id), COUNT(DISTINCT transaction_hash)
        FROM participants
    """
    out = run_psql(args, sql)
    rows, accounts, transactions = parse_ints(out, 3)
    return Sample(rows=rows, accounts=accounts, transactions=transactions)


def parse_ints(out: str, count: int) -> list[int]:
    parts = out.split("\t") if out else []
    if len(parts) != count:
        raise SystemExit(f"unexpected psql output: {out!r}")
    return [int(part) for part in parts]


def qualified(schema: str, table: str) -> str:
    return f"{quote_ident(schema)}.{quote_ident(table)}"


def quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", help="optional libpq connection string; otherwise PG* env vars are used")
    parser.add_argument("--psql", default="psql", help="psql executable path")
    parser.add_argument("--schema", default="public", help="silver_hot schema containing source tables")
    parser.add_argument("--start-ledger", type=int, help="sample start ledger; default is latest window")
    parser.add_argument("--end-ledger", type=int, help="sample end ledger; default is current max ledger")
    parser.add_argument("--sample-ledgers", type=int, default=10000, help="default sample size when start is omitted")
    parser.add_argument("--target-ledgers", type=int, help="ledger count to extrapolate; default is source bounds span")
    args = parser.parse_args()

    bounds = fetch_bounds(args)
    if bounds.max_ledger == 0:
        raise SystemExit("no ledgers found in enriched_history_operations")

    end = args.end_ledger or bounds.max_ledger
    start = args.start_ledger or max(bounds.min_ledger, end - args.sample_ledgers + 1)
    if start > end:
        raise SystemExit(f"invalid sample range: {start}>{end}")

    sample = fetch_sample(args, start, end)
    sample_ledgers = end - start + 1
    target_ledgers = args.target_ledgers or (bounds.max_ledger - bounds.min_ledger + 1)
    rows_per_ledger = sample.rows / sample_ledgers if sample_ledgers else 0.0
    estimated_rows = int(rows_per_ledger * target_ledgers)
    estimated_gb_low = estimated_rows * 80 / 1_000_000_000
    estimated_gb_with_index = estimated_rows * 160 / 1_000_000_000

    print(json.dumps({
        "source_bounds": {"min_ledger": bounds.min_ledger, "max_ledger": bounds.max_ledger},
        "sample": {
            "start_ledger": start,
            "end_ledger": end,
            "ledger_count": sample_ledgers,
            "distinct_account_tx_pairs": sample.rows,
            "distinct_accounts": sample.accounts,
            "distinct_transactions": sample.transactions,
            "rows_per_ledger": rows_per_ledger,
        },
        "estimate": {
            "target_ledgers": target_ledgers,
            "rows": estimated_rows,
            "data_gb_at_80_bytes": round(estimated_gb_low, 3),
            "rough_pg_plus_index_gb_at_160_bytes": round(estimated_gb_with_index, 3),
        },
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
