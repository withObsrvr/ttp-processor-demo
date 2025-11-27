#!/usr/bin/env python3
"""
DuckLake API Gateway - Fast API with Persistent DuckDB Connection

This keeps a persistent DuckDB connection alive, avoiding the 6-minute
startup cost on every request.

Usage:
    python3 server.py

Then query:
    curl "http://localhost:8000/ledgers?limit=10"
    curl "http://localhost:8000/balances/ACCOUNT_ID"
"""

import duckdb
import json
from flask import Flask, request, jsonify
from datetime import datetime
import os
import sys

app = Flask(__name__)

# Global persistent connection
db_conn = None

def init_ducklake_connection():
    """Initialize persistent DuckDB connection with DuckLake catalog"""
    print("🚀 Initializing DuckLake connection...")
    print("⏳ This may take 5-10 minutes on first startup...")

    conn = duckdb.connect(':memory:')

    # Install extensions
    print("📦 Installing extensions...")
    conn.execute("INSTALL ducklake")
    conn.execute("LOAD ducklake")
    conn.execute("INSTALL httpfs")
    conn.execute("LOAD httpfs")

    # Configure Backblaze B2 credentials from environment
    print("🔐 Configuring S3 credentials...")
    s3_key_id = os.getenv('S3_KEY_ID')
    s3_secret = os.getenv('S3_SECRET')
    s3_region = os.getenv('S3_REGION', 'us-west-004')
    s3_endpoint = os.getenv('S3_ENDPOINT', 's3.us-west-004.backblazeb2.com')

    if not s3_key_id or not s3_secret:
        raise ValueError("S3_KEY_ID and S3_SECRET environment variables are required")

    conn.execute(f"""
        CREATE SECRET (
            TYPE S3,
            KEY_ID '{s3_key_id}',
            SECRET '{s3_secret}',
            REGION '{s3_region}',
            ENDPOINT '{s3_endpoint}',
            URL_STYLE 'path'
        )
    """)

    # Attach DuckLake catalog from environment
    print("📊 Attaching DuckLake catalog...")
    catalog_url = os.getenv('CATALOG_URL')
    data_path = os.getenv('DATA_PATH', 's3://obsrvr-test-bucket-1/testnet_4/')
    metadata_schema = os.getenv('METADATA_SCHEMA', 'testnet')

    if not catalog_url:
        raise ValueError("CATALOG_URL environment variable is required")

    conn.execute(f"""
        ATTACH 'ducklake:postgres:{catalog_url}'
        AS catalog
        (DATA_PATH '{data_path}', METADATA_SCHEMA '{metadata_schema}')
    """)

    # Warm up with a simple query
    # Note: DuckLake catalog automatically handles version resolution
    print("🔥 Warming up connection...")
    warmup_query = """
        SELECT COUNT(*)
        FROM catalog.testnet.ledgers_row_v2
        WHERE sequence > 950000
    """
    result = conn.execute(warmup_query).fetchone()
    print(f"✅ Connection ready! Found {result[0]} recent ledgers")

    return conn

def get_connection():
    """Get or create persistent connection"""
    global db_conn
    if db_conn is None:
        db_conn = init_ducklake_connection()
    return db_conn

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'connection': 'active' if db_conn else 'not_initialized',
        'timestamp': datetime.utcnow().isoformat()
    })

@app.route('/ledgers', methods=['GET'])
def get_ledgers():
    """Get recent ledgers

    Query params:
        limit: number of results (default 10)
        sequence_min: minimum ledger sequence

    Note: DuckLake catalog automatically returns latest version
    """
    limit = request.args.get('limit', 10, type=int)
    sequence_min = request.args.get('sequence_min', 0, type=int)

    conn = get_connection()

    # Query directly - DuckLake handles version resolution
    query = f"""
        SELECT
            sequence,
            closed_at,
            transaction_count,
            operation_count,
            successful_tx_count,
            failed_tx_count
        FROM catalog.testnet.ledgers_row_v2
        WHERE sequence > {sequence_min}
        ORDER BY sequence DESC
        LIMIT {limit}
    """

    start_time = datetime.now()
    result = conn.execute(query).fetchall()
    query_time = (datetime.now() - start_time).total_seconds()

    columns = ['sequence', 'closed_at', 'transaction_count', 'operation_count',
               'successful_tx_count', 'failed_tx_count']

    ledgers = [dict(zip(columns, row)) for row in result]

    return jsonify({
        'data': ledgers,
        'count': len(ledgers),
        'query_time_seconds': query_time
    })

@app.route('/ledgers/<int:sequence>', methods=['GET'])
def get_ledger(sequence):
    """Get specific ledger by sequence number

    Note: DuckLake catalog automatically returns latest version
    """
    conn = get_connection()

    # Query directly - DuckLake handles version resolution
    query = f"""
        SELECT
            sequence,
            closed_at,
            ledger_hash,
            previous_ledger_hash,
            transaction_count,
            operation_count,
            successful_tx_count,
            failed_tx_count,
            protocol_version,
            total_coins / 10000000.0 as total_xlm,
            base_fee,
            base_reserve
        FROM catalog.testnet.ledgers_row_v2
        WHERE sequence = {sequence}
    """

    start_time = datetime.now()
    result = conn.execute(query).fetchone()
    query_time = (datetime.now() - start_time).total_seconds()

    if not result:
        return jsonify({'error': 'Ledger not found'}), 404

    columns = ['sequence', 'closed_at', 'ledger_hash', 'previous_ledger_hash',
               'transaction_count', 'operation_count', 'successful_tx_count',
               'failed_tx_count', 'protocol_version', 'total_xlm', 'base_fee', 'base_reserve']

    ledger = dict(zip(columns, result))

    return jsonify({
        'data': ledger,
        'query_time_seconds': query_time
    })

@app.route('/balances/<account_id>', methods=['GET'])
def get_account_balances(account_id):
    """Get all token balances for an account

    Returns both XLM (native) and all trustline balances
    """
    conn = get_connection()

    # Get latest ledger for this account
    # Note: DuckLake catalog automatically returns latest version
    latest_ledger_query = f"""
        SELECT MAX(ledger_sequence) as latest
        FROM catalog.testnet.native_balances_snapshot_v1
        WHERE account_id = '{account_id}'
    """

    result = conn.execute(latest_ledger_query).fetchone()

    # Check if account exists (handle None result)
    if not result or result[0] is None:
        return jsonify({'error': 'Account not found'}), 404

    latest_ledger = result[0]

    # Get XLM balance
    # Note: DuckLake catalog automatically returns latest version
    xlm_query = f"""
        SELECT
            'XLM' as asset_code,
            'native' as asset_issuer,
            balance / 10000000.0 as balance,
            buying_liabilities / 10000000.0 as buying_liabilities,
            selling_liabilities / 10000000.0 as selling_liabilities,
            (balance - buying_liabilities - selling_liabilities) / 10000000.0 as available,
            ledger_sequence
        FROM catalog.testnet.native_balances_snapshot_v1
        WHERE account_id = '{account_id}'
            AND ledger_sequence = {latest_ledger}
    """

    # Get token balances
    # Note: DuckLake catalog automatically returns latest version
    tokens_query = f"""
        SELECT
            asset_code,
            asset_issuer,
            CAST(balance AS DOUBLE) / 10000000.0 as balance,
            CAST(buying_liabilities AS DOUBLE) / 10000000.0 as buying_liabilities,
            CAST(selling_liabilities AS DOUBLE) / 10000000.0 as selling_liabilities,
            (CAST(balance AS DOUBLE) - CAST(buying_liabilities AS DOUBLE) - CAST(selling_liabilities AS DOUBLE)) / 10000000.0 as available,
            ledger_sequence
        FROM catalog.testnet.trustlines_snapshot_v1
        WHERE account_id = '{account_id}'
            AND ledger_sequence = {latest_ledger}
            AND CAST(balance AS DOUBLE) > 0
    """

    start_time = datetime.now()

    xlm_result = conn.execute(xlm_query).fetchall()
    tokens_result = conn.execute(tokens_query).fetchall()

    query_time = (datetime.now() - start_time).total_seconds()

    columns = ['asset_code', 'asset_issuer', 'balance', 'buying_liabilities',
               'selling_liabilities', 'available', 'ledger_sequence']

    balances = []
    balances.extend([dict(zip(columns, row)) for row in xlm_result])
    balances.extend([dict(zip(columns, row)) for row in tokens_result])

    return jsonify({
        'account_id': account_id,
        'ledger_sequence': latest_ledger,
        'balances': balances,
        'count': len(balances),
        'query_time_seconds': query_time
    })

@app.route('/assets/<asset_code>/holders', methods=['GET'])
def get_asset_holders(asset_code):
    """Get top holders of a specific asset

    Query params:
        limit: number of results (default 20)
        min_balance: minimum balance to include (default 0)
    """
    limit = request.args.get('limit', 20, type=int)
    min_balance = request.args.get('min_balance', 0, type=float)

    conn = get_connection()

    # Get latest ledger
    # Note: DuckLake catalog automatically returns latest version
    latest_ledger_query = """
        SELECT MAX(ledger_sequence)
        FROM catalog.testnet.trustlines_snapshot_v1
    """
    latest_ledger = conn.execute(latest_ledger_query).fetchone()[0]

    # Get asset holders
    # Note: DuckLake catalog automatically returns latest version
    query = f"""
        SELECT
            account_id,
            asset_code,
            asset_issuer,
            CAST(balance AS DOUBLE) / 10000000.0 as balance,
            authorized
        FROM catalog.testnet.trustlines_snapshot_v1
        WHERE asset_code = '{asset_code}'
            AND ledger_sequence = {latest_ledger}
            AND CAST(balance AS DOUBLE) / 10000000.0 > {min_balance}
        ORDER BY CAST(balance AS DOUBLE) DESC
        LIMIT {limit}
    """

    start_time = datetime.now()
    result = conn.execute(query).fetchall()
    query_time = (datetime.now() - start_time).total_seconds()

    columns = ['account_id', 'asset_code', 'asset_issuer', 'balance', 'authorized']
    holders = [dict(zip(columns, row)) for row in result]

    return jsonify({
        'asset_code': asset_code,
        'ledger_sequence': latest_ledger,
        'holders': holders,
        'count': len(holders),
        'query_time_seconds': query_time
    })

@app.route('/stats/network', methods=['GET'])
def get_network_stats():
    """Get recent network statistics

    Note: DuckLake catalog automatically returns latest version
    """
    conn = get_connection()

    # Query directly - DuckLake handles version resolution
    query = """
        SELECT
            COUNT(*) as ledger_count,
            MIN(sequence) as first_ledger,
            MAX(sequence) as last_ledger,
            SUM(transaction_count) as total_transactions,
            SUM(successful_tx_count) as successful_transactions,
            SUM(failed_tx_count) as failed_transactions,
            AVG(transaction_count) as avg_tx_per_ledger
        FROM catalog.testnet.ledgers_row_v2
        WHERE sequence > 950000
    """

    start_time = datetime.now()
    result = conn.execute(query).fetchone()
    query_time = (datetime.now() - start_time).total_seconds()

    columns = ['ledger_count', 'first_ledger', 'last_ledger', 'total_transactions',
               'successful_transactions', 'failed_transactions', 'avg_tx_per_ledger']

    stats = dict(zip(columns, result))

    return jsonify({
        'stats': stats,
        'query_time_seconds': query_time
    })

if __name__ == '__main__':
    print("=" * 60)
    print("🚀 DuckLake API Gateway")
    print("=" * 60)
    print()
    print("Starting server on http://localhost:8000")
    print()
    print("Available endpoints:")
    print("  GET /health                      - Health check")
    print("  GET /ledgers                     - Recent ledgers")
    print("  GET /ledgers/<sequence>          - Specific ledger")
    print("  GET /balances/<account_id>       - Account balances")
    print("  GET /assets/<code>/holders       - Asset holders")
    print("  GET /stats/network               - Network statistics")
    print()
    print("⚠️  First startup will take 5-10 minutes to initialize connection")
    print("    Subsequent requests will be fast!")
    print()

    # Initialize connection on startup
    get_connection()

    print()
    print("✅ Ready to accept requests!")
    print()

    app.run(host='0.0.0.0', port=8000, debug=False)
