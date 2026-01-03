#!/usr/bin/env python3
"""
Fix all INSERT statements in writer.go to match Bronze schema.
Strategy: Replace each insertXXX function with Bronze-compatible version.
"""

# Bronze schema column mappings for each table
BRONZE_SCHEMAS = {
    'ledgers_row_v2': [
        'sequence', 'ledger_hash', 'previous_ledger_hash',
        'transaction_count', 'operation_count', 'successful_tx_count',
        'failed_tx_count', 'tx_set_operation_count', 'closed_at',
        'total_coins', 'fee_pool', 'base_fee', 'base_reserve',
        'max_tx_set_size', 'protocol_version', 'ledger_header',
        'soroban_fee_write_1kb', 'soroban_tx_max_read_ledger_entries',
        'soroban_tx_max_read_bytes', 'soroban_tx_max_write_ledger_entries',
        'soroban_tx_max_write_bytes', 'soroban_tx_max_size_bytes',
        'created_at', 'ledger_range', 'era_id', 'version_label'
    ],
    'transactions_row_v2': [
        'transaction_hash', 'ledger_sequence', 'application_order',
        'account', 'account_muxed', 'account_sequence', 'max_fee',
        'operation_count', 'tx_envelope', 'tx_result', 'tx_meta',
        'tx_fee_meta', 'successful', 'closed_at', 'created_at',
        'ledger_range', 'era_id', 'version_label'
    ],
    'operations_row_v2': [
        'operation_id', 'transaction_hash', 'ledger_sequence',
        'application_order', 'type', 'type_string', 'details',
        'source_account', 'source_account_muxed', 'successful',
        'transaction_successful', 'closed_at', 'created_at',
        'ledger_range', 'era_id', 'version_label'
    ],
    'effects_row_v1': [
        'operation_id', 'ledger_sequence', 'account', 'account_muxed',
        'type', 'type_string', 'details', 'created_at', 'ledger_range',
        'era_id', 'version_label'
    ],
    'trades_row_v1': [
        'ledger_sequence', 'offer_id', 'base_offer_id', 'base_account',
        'base_asset_type', 'base_asset_code', 'base_asset_issuer',
        'base_amount', 'counter_offer_id', 'counter_account',
        'counter_asset_type', 'counter_asset_code', 'counter_asset_issuer',
        'counter_amount', 'price_n', 'price_d', 'created_at',
        'ledger_range', 'era_id', 'version_label'
    ]
}

print("Bronze schema column counts:")
for table, columns in BRONZE_SCHEMAS.items():
    print(f"  {table}: {len(columns)} columns")
    
print("\nThis script would generate updated INSERT functions.")
print("For manual implementation, use these column lists.")
