package main

import (
	"context"
	"fmt"
)

// createBronzeTables creates all bronze layer tables in DuckLake.
func (w *DuckLakeWriter) createBronzeTables(ctx context.Context) error {
	cat := w.cfg.DuckLake.CatalogName
	schema := w.cfg.DuckLake.BronzeSchema

	// Replace bare table names with fully qualified names
	sql := bronzeSchemaSQL(cat, schema)
	return w.execSchemaStatements(ctx, sql)
}

// createSilverTables creates all silver layer tables in DuckLake.
func (w *DuckLakeWriter) createSilverTables(ctx context.Context) error {
	cat := w.cfg.DuckLake.CatalogName
	schema := w.cfg.DuckLake.SilverSchema

	sql := silverSchemaSQL(cat, schema)
	return w.execSchemaStatements(ctx, sql)
}

// createIndexTables creates all index layer tables in DuckLake.
func (w *DuckLakeWriter) createIndexTables(ctx context.Context) error {
	cat := w.cfg.DuckLake.CatalogName
	schema := w.cfg.DuckLake.IndexSchema

	sql := indexSchemaSQL(cat, schema)
	return w.execSchemaStatements(ctx, sql)
}

// ---------------------------------------------------------------------------
// Bronze schema DDL
// ---------------------------------------------------------------------------

func bronzeSchemaSQL(cat, schema string) string {
	q := func(table string) string {
		return fmt.Sprintf("%s.%s.%s", cat, schema, table)
	}

	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    sequence BIGINT, ledger_hash TEXT, previous_ledger_hash TEXT,
    closed_at TIMESTAMP, protocol_version INTEGER,
    total_coins BIGINT, fee_pool BIGINT, base_fee INTEGER,
    base_reserve INTEGER, max_tx_set_size INTEGER,
    successful_tx_count INTEGER, failed_tx_count INTEGER,
    ingestion_timestamp TIMESTAMP, ledger_range BIGINT,
    transaction_count INTEGER, operation_count INTEGER,
    tx_set_operation_count INTEGER, pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS %s (
    ledger_sequence BIGINT, transaction_hash TEXT, source_account TEXT,
    source_account_muxed TEXT,
    fee_charged BIGINT, max_fee BIGINT, successful BOOLEAN,
    transaction_result_code TEXT, operation_count INTEGER,
    memo_type TEXT, memo TEXT, created_at TIMESTAMP,
    account_sequence BIGINT, ledger_range BIGINT,
    signatures_count INTEGER, new_account BOOLEAN,
    timebounds_min_time TEXT, timebounds_max_time TEXT,
    soroban_host_function_type TEXT, soroban_contract_id TEXT,
    rent_fee_charged BIGINT,
    soroban_resources_instructions BIGINT,
    soroban_resources_read_bytes BIGINT,
    soroban_resources_write_bytes BIGINT,
    tx_envelope TEXT,
    tx_result TEXT,
    tx_meta TEXT,
    tx_fee_meta TEXT,
    pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS %s (
    transaction_hash TEXT, transaction_index INTEGER, operation_index INTEGER,
    ledger_sequence BIGINT, source_account TEXT, source_account_muxed TEXT,
    op_type INTEGER, type_string TEXT, created_at TIMESTAMP,
    transaction_successful BOOLEAN, operation_result_code TEXT, ledger_range BIGINT,
    amount BIGINT, asset TEXT,
    asset_type TEXT, asset_code TEXT, asset_issuer TEXT,
    destination TEXT,
    source_asset TEXT, source_asset_type TEXT, source_asset_code TEXT, source_asset_issuer TEXT,
    source_amount BIGINT, destination_min BIGINT, starting_balance BIGINT,
    trustline_limit BIGINT,
    offer_id BIGINT, price TEXT, price_r TEXT,
    buying_asset TEXT, buying_asset_type TEXT, buying_asset_code TEXT, buying_asset_issuer TEXT,
    selling_asset TEXT, selling_asset_type TEXT, selling_asset_code TEXT, selling_asset_issuer TEXT,
    set_flags INTEGER, clear_flags INTEGER,
    home_domain TEXT, master_weight INTEGER,
    low_threshold INTEGER, medium_threshold INTEGER, high_threshold INTEGER,
    data_name TEXT, data_value TEXT,
    balance_id TEXT, sponsored_id TEXT, bump_to BIGINT,
    soroban_auth_required BOOLEAN,
    soroban_operation TEXT, soroban_contract_id TEXT,
    soroban_function TEXT, soroban_arguments_json TEXT,
    contract_calls_json TEXT, max_call_depth INTEGER,
    pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS %s (
    ledger_sequence BIGINT, transaction_hash TEXT, operation_index INTEGER,
    effect_index INTEGER, effect_type INTEGER, effect_type_string TEXT,
    account_id TEXT, amount TEXT, asset_code TEXT, asset_issuer TEXT,
    asset_type TEXT,
    trustline_limit TEXT, authorize_flag BOOLEAN, clawback_flag BOOLEAN,
    signer_account TEXT, signer_weight INTEGER,
    offer_id BIGINT, seller_account TEXT,
    created_at TIMESTAMP, ledger_range BIGINT,
    pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS %s (
    ledger_sequence BIGINT, transaction_hash TEXT, operation_index INTEGER,
    trade_index INTEGER, trade_type TEXT, trade_timestamp TIMESTAMP,
    seller_account TEXT, selling_asset_code TEXT, selling_asset_issuer TEXT,
    selling_amount TEXT, buyer_account TEXT, buying_asset_code TEXT,
    buying_asset_issuer TEXT, buying_amount TEXT, price TEXT,
    created_at TIMESTAMP, ledger_range BIGINT, pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS %s (
    account_id TEXT, ledger_sequence BIGINT, closed_at TIMESTAMP,
    balance TEXT, sequence_number BIGINT, num_subentries INTEGER,
    num_sponsoring INTEGER, num_sponsored INTEGER, home_domain TEXT,
    master_weight INTEGER, low_threshold INTEGER, med_threshold INTEGER,
    high_threshold INTEGER, flags INTEGER, auth_required BOOLEAN,
    auth_revocable BOOLEAN, auth_immutable BOOLEAN,
    auth_clawback_enabled BOOLEAN, signers TEXT, sponsor_account TEXT,
    ledger_range BIGINT, pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS %s (
    account_id TEXT, asset_code TEXT, asset_issuer TEXT, asset_type TEXT,
    balance TEXT, trust_limit TEXT, buying_liabilities TEXT,
    selling_liabilities TEXT, authorized BOOLEAN,
    authorized_to_maintain_liabilities BOOLEAN, clawback_enabled BOOLEAN,
    ledger_sequence BIGINT, ledger_range BIGINT, pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS %s (
    offer_id BIGINT, seller_account TEXT, ledger_sequence BIGINT,
    closed_at TIMESTAMP, selling_asset_type TEXT, selling_asset_code TEXT,
    selling_asset_issuer TEXT, buying_asset_type TEXT, buying_asset_code TEXT,
    buying_asset_issuer TEXT, amount TEXT, price TEXT, flags INTEGER,
    ledger_range BIGINT, pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS %s (
    account_id TEXT, signer TEXT, ledger_sequence BIGINT,
    weight INTEGER, sponsor TEXT, deleted BOOLEAN,
    closed_at TIMESTAMP, ledger_range BIGINT, pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS %s (
    balance_id TEXT, sponsor TEXT, ledger_sequence BIGINT,
    closed_at TIMESTAMP, asset_type TEXT, asset_code TEXT,
    asset_issuer TEXT, amount BIGINT, claimants_count INTEGER,
    flags INTEGER, ledger_range BIGINT, pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS %s (
    liquidity_pool_id TEXT, ledger_sequence BIGINT, closed_at TIMESTAMP,
    pool_type TEXT, fee INTEGER, trustline_count INTEGER,
    total_pool_shares BIGINT, asset_a_type TEXT, asset_a_code TEXT,
    asset_a_issuer TEXT, asset_a_amount BIGINT, asset_b_type TEXT,
    asset_b_code TEXT, asset_b_issuer TEXT, asset_b_amount BIGINT,
    ledger_range BIGINT, pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS %s (
    config_setting_id INTEGER, ledger_sequence BIGINT,
    last_modified_ledger INTEGER, deleted BOOLEAN,
    closed_at TIMESTAMP, config_setting_xdr TEXT,
    ledger_range BIGINT, pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS %s (
    key_hash TEXT, ledger_sequence BIGINT, live_until_ledger_seq BIGINT,
    ttl_remaining BIGINT, expired BOOLEAN, last_modified_ledger INTEGER,
    deleted BOOLEAN, closed_at TIMESTAMP, ledger_range BIGINT,
    pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS %s (
    key_hash TEXT, ledger_sequence BIGINT, contract_id TEXT,
    key_type TEXT, durability TEXT, closed_at TIMESTAMP,
    ledger_range BIGINT, pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS %s (
    key_hash TEXT, ledger_sequence BIGINT, contract_id TEXT,
    key_type TEXT, durability TEXT, restored_from_ledger BIGINT,
    closed_at TIMESTAMP, ledger_range BIGINT, pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS %s (
    event_id TEXT, contract_id TEXT, ledger_sequence BIGINT,
    transaction_hash TEXT, closed_at TIMESTAMP, event_type TEXT,
    in_successful_contract_call BOOLEAN, topics_json TEXT,
    topics_decoded TEXT, data_xdr TEXT, data_decoded TEXT,
    topic_count INTEGER, topic0_decoded TEXT, topic1_decoded TEXT,
    topic2_decoded TEXT, topic3_decoded TEXT,
    operation_index INTEGER, event_index INTEGER,
    ledger_range BIGINT, pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS %s (
    contract_id TEXT, ledger_sequence BIGINT, ledger_key_hash TEXT,
    contract_key_type TEXT, contract_durability TEXT,
    asset_code TEXT, asset_issuer TEXT, asset_type TEXT,
    balance_holder TEXT, balance TEXT,
    last_modified_ledger INTEGER, ledger_entry_change INTEGER,
    deleted BOOLEAN, closed_at TIMESTAMP, contract_data_xdr TEXT,
    token_name TEXT, token_symbol TEXT, token_decimals INTEGER,
    ledger_range BIGINT, pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS %s (
    contract_code_hash TEXT, ledger_key_hash TEXT,
    contract_code_ext_v INTEGER, last_modified_ledger INTEGER,
    ledger_entry_change INTEGER, deleted BOOLEAN,
    closed_at TIMESTAMP, ledger_sequence BIGINT,
    n_instructions BIGINT, n_functions BIGINT, n_globals BIGINT,
    n_table_entries BIGINT, n_types BIGINT, n_data_segments BIGINT,
    n_elem_segments BIGINT, n_imports BIGINT, n_exports BIGINT,
    n_data_segment_bytes BIGINT, ledger_range BIGINT, pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS %s (
    account_id TEXT, balance BIGINT, buying_liabilities BIGINT,
    selling_liabilities BIGINT, num_subentries INTEGER,
    num_sponsoring INTEGER, num_sponsored INTEGER,
    sequence_number BIGINT, last_modified_ledger BIGINT,
    ledger_sequence BIGINT, ledger_range BIGINT, pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS %s (
    contract_id TEXT, creator_address TEXT, wasm_hash TEXT,
    created_ledger BIGINT, created_at TIMESTAMP,
    ledger_range BIGINT, pipeline_version TEXT
)`,
		q("ledgers_row_v2"),
		q("transactions_row_v2"),
		q("operations_row_v2"),
		q("effects_row_v1"),
		q("trades_row_v1"),
		q("accounts_snapshot_v1"),
		q("trustlines_snapshot_v1"),
		q("offers_snapshot_v1"),
		q("account_signers_snapshot_v1"),
		q("claimable_balances_snapshot_v1"),
		q("liquidity_pools_snapshot_v1"),
		q("config_settings_snapshot_v1"),
		q("ttl_snapshot_v1"),
		q("evicted_keys_state_v1"),
		q("restored_keys_state_v1"),
		q("contract_events_stream_v1"),
		q("contract_data_snapshot_v1"),
		q("contract_code_snapshot_v1"),
		q("native_balances_snapshot_v1"),
		q("contract_creations_v1"),
	)
}

// ---------------------------------------------------------------------------
// Silver schema DDL
// ---------------------------------------------------------------------------

func silverSchemaSQL(cat, schema string) string {
	q := func(table string) string {
		return fmt.Sprintf("%s.%s.%s", cat, schema, table)
	}

	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    id TEXT,
    transaction_hash TEXT,
    source_account TEXT,
    source_account_muxed TEXT,
    type INTEGER,
    type_string TEXT,
    operation_index INTEGER,
    transaction_index INTEGER,
    ledger_sequence BIGINT,
    closed_at TIMESTAMP,
    transaction_successful BOOLEAN,
    fee_charged BIGINT,
    memo_type TEXT,
    memo TEXT,
    amount BIGINT,
    asset TEXT,
    asset_type TEXT,
    asset_code TEXT,
    asset_issuer TEXT,
    destination TEXT,
    source_asset TEXT,
    source_asset_type TEXT,
    source_asset_code TEXT,
    source_asset_issuer TEXT,
    source_amount BIGINT,
    destination_min BIGINT,
    starting_balance BIGINT,
    trustline_limit BIGINT,
    offer_id BIGINT,
    price TEXT,
    buying_asset TEXT,
    buying_asset_type TEXT,
    buying_asset_code TEXT,
    buying_asset_issuer TEXT,
    selling_asset TEXT,
    selling_asset_type TEXT,
    selling_asset_code TEXT,
    selling_asset_issuer TEXT,
    soroban_operation TEXT,
    soroban_contract_id TEXT,
    soroban_function TEXT,
    ledger_range BIGINT,
    pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS %s (
    ledger_sequence BIGINT,
    transaction_index INTEGER,
    operation_index INTEGER,
    transaction_hash TEXT,
    source_account TEXT,
    contract_id TEXT,
    function_name TEXT,
    arguments_json TEXT,
    successful BOOLEAN,
    closed_at TIMESTAMP,
    ledger_range BIGINT,
    pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS %s (
    contract_id TEXT,
    token_name TEXT,
    token_symbol TEXT,
    token_decimals INTEGER,
    asset_code TEXT,
    asset_issuer TEXT,
    token_type TEXT,
    first_seen_ledger BIGINT,
    last_updated_ledger BIGINT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS %s (
    timestamp TIMESTAMP,
    transaction_hash TEXT,
    ledger_sequence BIGINT,
    source_type TEXT,
    from_account TEXT,
    to_account TEXT,
    asset_code TEXT,
    asset_issuer TEXT,
    amount DOUBLE,
    token_contract_id TEXT,
    operation_type INTEGER,
    transaction_successful BOOLEAN,
    event_index INTEGER,
    ledger_range BIGINT,
    pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS %s (
    contract_id TEXT,
    creator_address TEXT,
    wasm_hash TEXT,
    created_ledger BIGINT,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    pipeline_version TEXT
);

CREATE TABLE IF NOT EXISTS %s (
    id TEXT,
    ledger_sequence BIGINT,
    timestamp TIMESTAMP,
    activity_type TEXT,
    description TEXT,
    source_account TEXT,
    destination_account TEXT,
    contract_id TEXT,
    asset_code TEXT,
    asset_issuer TEXT,
    amount TEXT,
    is_soroban BOOLEAN,
    soroban_function_name TEXT,
    transaction_hash TEXT,
    operation_index INTEGER,
    successful BOOLEAN,
    fee_charged BIGINT,
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS %s (
    contract_id TEXT,
    contract_type TEXT,
    token_name TEXT,
    token_symbol TEXT,
    token_decimals INTEGER,
    deployer_account TEXT,
    deployed_at TIMESTAMP,
    deployed_ledger BIGINT,
    total_invocations BIGINT,
    last_activity TIMESTAMP,
    unique_callers BIGINT,
    observed_functions TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS %s (
    id TEXT,
    ledger_sequence BIGINT,
    timestamp TIMESTAMP,
    flow_type TEXT,
    from_account TEXT,
    to_account TEXT,
    contract_id TEXT,
    asset_code TEXT,
    asset_issuer TEXT,
    asset_type TEXT,
    amount TEXT,
    transaction_hash TEXT,
    operation_type INTEGER,
    successful BOOLEAN,
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS %s (
    contract_id TEXT,
    function_name TEXT,
    total_calls BIGINT,
    successful_calls BIGINT,
    failed_calls BIGINT,
    unique_callers BIGINT,
    first_called TIMESTAMP,
    last_called TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS %s (
    asset_key TEXT,
    asset_code TEXT,
    asset_issuer TEXT,
    asset_type TEXT,
    token_name TEXT,
    token_symbol TEXT,
    token_decimals INTEGER,
    contract_id TEXT,
    holder_count BIGINT,
    transfer_count BIGINT,
    transfer_volume DOUBLE,
    first_seen TIMESTAMP,
    last_transfer TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS %s (
    pair_key TEXT,
    selling_asset_code TEXT,
    selling_asset_issuer TEXT,
    buying_asset_code TEXT,
    buying_asset_issuer TEXT,
    trade_count BIGINT,
    selling_volume DOUBLE,
    buying_volume DOUBLE,
    last_price DOUBLE,
    unique_sellers BIGINT,
    unique_buyers BIGINT,
    first_trade TIMESTAMP,
    last_trade TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS %s (
    account_id TEXT,
    total_operations BIGINT,
    total_payments_sent BIGINT,
    total_payments_received BIGINT,
    total_contract_calls BIGINT,
    unique_contracts_called BIGINT,
    top_contract_id TEXT,
    top_contract_function TEXT,
    is_contract_deployer BOOLEAN,
    contracts_deployed INTEGER,
    first_activity TIMESTAMP,
    last_activity TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS %s (
    account_id TEXT,
    balance TEXT,
    sequence_number BIGINT,
    num_subentries INTEGER,
    num_sponsoring INTEGER,
    num_sponsored INTEGER,
    home_domain TEXT,
    master_weight INTEGER,
    low_threshold INTEGER,
    med_threshold INTEGER,
    high_threshold INTEGER,
    flags INTEGER,
    auth_required BOOLEAN,
    auth_revocable BOOLEAN,
    auth_immutable BOOLEAN,
    auth_clawback_enabled BOOLEAN,
    signers TEXT,
    sponsor_account TEXT,
    last_modified_ledger BIGINT,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS %s (
    account_id TEXT,
    asset_code TEXT,
    asset_issuer TEXT,
    asset_type TEXT,
    balance TEXT,
    trust_limit TEXT,
    buying_liabilities TEXT,
    selling_liabilities TEXT,
    authorized BOOLEAN,
    authorized_to_maintain_liabilities BOOLEAN,
    clawback_enabled BOOLEAN,
    last_modified_ledger BIGINT,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS %s (
    offer_id BIGINT,
    seller_account TEXT,
    selling_asset_type TEXT,
    selling_asset_code TEXT,
    selling_asset_issuer TEXT,
    buying_asset_type TEXT,
    buying_asset_code TEXT,
    buying_asset_issuer TEXT,
    amount TEXT,
    price TEXT,
    flags INTEGER,
    last_modified_ledger BIGINT,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS %s (
    ledger_sequence BIGINT,
    transaction_hash TEXT,
    operation_index INTEGER,
    trade_index INTEGER,
    trade_type TEXT,
    trade_timestamp TIMESTAMP,
    seller_account TEXT,
    selling_asset_code TEXT,
    selling_asset_issuer TEXT,
    selling_amount TEXT,
    buyer_account TEXT,
    buying_asset_code TEXT,
    buying_asset_issuer TEXT,
    buying_amount TEXT,
    price TEXT,
    ledger_range BIGINT,
    pipeline_version TEXT
)`,
		q("enriched_history_operations"),
		q("contract_invocations_raw"),
		q("token_registry"),
		q("token_transfers_raw"),
		q("contract_metadata"),
		q("semantic_activities"),
		q("semantic_entities_contracts"),
		q("semantic_flows_value"),
		q("semantic_contract_functions"),
		q("semantic_asset_stats"),
		q("semantic_dex_pairs"),
		q("semantic_account_summary"),
		q("accounts_current"),
		q("trustlines_current"),
		q("offers_current"),
		q("trades"),
	)
}

// ---------------------------------------------------------------------------
// Index schema DDL
// ---------------------------------------------------------------------------

func indexSchemaSQL(cat, schema string) string {
	q := func(table string) string {
		return fmt.Sprintf("%s.%s.%s", cat, schema, table)
	}

	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    last_processed_ledger BIGINT,
    updated_at TIMESTAMP
)`,
		q("processor_checkpoint"),
	)
}
