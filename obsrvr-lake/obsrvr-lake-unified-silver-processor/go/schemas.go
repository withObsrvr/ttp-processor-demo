package main

import "fmt"

func silverSchemaSQL(cat, schema string) string {
	q := func(table string) string {
		return fmt.Sprintf("%s.%s.%s", cat, schema, table)
	}

	var ddl string

	// ====================================================================
	// 1. enriched_history_operations
	// ====================================================================
	ddl += fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    transaction_hash TEXT,
    operation_index INTEGER,
    ledger_sequence BIGINT,
    source_account TEXT,
    type INTEGER,
    type_string TEXT,
    created_at TIMESTAMP,
    transaction_successful BOOLEAN,
    operation_result_code TEXT,
    operation_trace_code TEXT,
    ledger_range BIGINT,
    source_account_muxed TEXT,
    asset TEXT,
    asset_type TEXT,
    asset_code TEXT,
    asset_issuer TEXT,
    source_asset TEXT,
    source_asset_type TEXT,
    source_asset_code TEXT,
    source_asset_issuer TEXT,
    destination TEXT,
    destination_muxed TEXT,
    amount BIGINT,
    source_amount BIGINT,
    from_account TEXT,
    from_muxed TEXT,
    to_address TEXT,
    to_muxed TEXT,
    limit_amount BIGINT,
    offer_id BIGINT,
    selling_asset TEXT,
    selling_asset_type TEXT,
    selling_asset_code TEXT,
    selling_asset_issuer TEXT,
    buying_asset TEXT,
    buying_asset_type TEXT,
    buying_asset_code TEXT,
    buying_asset_issuer TEXT,
    price_n INTEGER,
    price_d INTEGER,
    price DOUBLE,
    starting_balance BIGINT,
    home_domain TEXT,
    inflation_dest TEXT,
    set_flags INTEGER,
    set_flags_s TEXT,
    clear_flags INTEGER,
    clear_flags_s TEXT,
    master_key_weight INTEGER,
    low_threshold INTEGER,
    med_threshold INTEGER,
    high_threshold INTEGER,
    signer_account_id TEXT,
    signer_key TEXT,
    signer_weight INTEGER,
    data_name TEXT,
    data_value TEXT,
    host_function_type TEXT,
    parameters TEXT,
    address TEXT,
    contract_id TEXT,
    function_name TEXT,
    balance_id TEXT,
    claimant TEXT,
    claimant_muxed TEXT,
    predicate TEXT,
    liquidity_pool_id TEXT,
    reserve_a_asset TEXT,
    reserve_a_amount BIGINT,
    reserve_b_asset TEXT,
    reserve_b_amount BIGINT,
    shares BIGINT,
    shares_received BIGINT,
    into_account TEXT,
    into_muxed TEXT,
    sponsor TEXT,
    sponsored_id TEXT,
    begin_sponsor TEXT,
    tx_successful BOOLEAN,
    tx_fee_charged BIGINT,
    tx_max_fee BIGINT,
    tx_operation_count INTEGER,
    tx_memo_type TEXT,
    tx_memo TEXT,
    ledger_closed_at TIMESTAMP,
    ledger_total_coins BIGINT,
    ledger_fee_pool BIGINT,
    ledger_base_fee INTEGER,
    ledger_base_reserve INTEGER,
    ledger_transaction_count INTEGER,
    ledger_operation_count INTEGER,
    ledger_successful_tx_count INTEGER,
    ledger_failed_tx_count INTEGER,
    is_payment_op BOOLEAN,
    is_soroban_op BOOLEAN,
    inserted_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);`, q("enriched_history_operations"))

	// ====================================================================
	// 2. enriched_history_operations_soroban (duplicate of #1)
	// ====================================================================
	ddl += fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    transaction_hash TEXT,
    operation_index INTEGER,
    ledger_sequence BIGINT,
    source_account TEXT,
    type INTEGER,
    type_string TEXT,
    created_at TIMESTAMP,
    transaction_successful BOOLEAN,
    operation_result_code TEXT,
    operation_trace_code TEXT,
    ledger_range BIGINT,
    source_account_muxed TEXT,
    asset TEXT,
    asset_type TEXT,
    asset_code TEXT,
    asset_issuer TEXT,
    source_asset TEXT,
    source_asset_type TEXT,
    source_asset_code TEXT,
    source_asset_issuer TEXT,
    destination TEXT,
    destination_muxed TEXT,
    amount BIGINT,
    source_amount BIGINT,
    from_account TEXT,
    from_muxed TEXT,
    to_address TEXT,
    to_muxed TEXT,
    limit_amount BIGINT,
    offer_id BIGINT,
    selling_asset TEXT,
    selling_asset_type TEXT,
    selling_asset_code TEXT,
    selling_asset_issuer TEXT,
    buying_asset TEXT,
    buying_asset_type TEXT,
    buying_asset_code TEXT,
    buying_asset_issuer TEXT,
    price_n INTEGER,
    price_d INTEGER,
    price DOUBLE,
    starting_balance BIGINT,
    home_domain TEXT,
    inflation_dest TEXT,
    set_flags INTEGER,
    set_flags_s TEXT,
    clear_flags INTEGER,
    clear_flags_s TEXT,
    master_key_weight INTEGER,
    low_threshold INTEGER,
    med_threshold INTEGER,
    high_threshold INTEGER,
    signer_account_id TEXT,
    signer_key TEXT,
    signer_weight INTEGER,
    data_name TEXT,
    data_value TEXT,
    host_function_type TEXT,
    parameters TEXT,
    address TEXT,
    contract_id TEXT,
    function_name TEXT,
    balance_id TEXT,
    claimant TEXT,
    claimant_muxed TEXT,
    predicate TEXT,
    liquidity_pool_id TEXT,
    reserve_a_asset TEXT,
    reserve_a_amount BIGINT,
    reserve_b_asset TEXT,
    reserve_b_amount BIGINT,
    shares BIGINT,
    shares_received BIGINT,
    into_account TEXT,
    into_muxed TEXT,
    sponsor TEXT,
    sponsored_id TEXT,
    begin_sponsor TEXT,
    tx_successful BOOLEAN,
    tx_fee_charged BIGINT,
    tx_max_fee BIGINT,
    tx_operation_count INTEGER,
    tx_memo_type TEXT,
    tx_memo TEXT,
    ledger_closed_at TIMESTAMP,
    ledger_total_coins BIGINT,
    ledger_fee_pool BIGINT,
    ledger_base_fee INTEGER,
    ledger_base_reserve INTEGER,
    ledger_transaction_count INTEGER,
    ledger_operation_count INTEGER,
    ledger_successful_tx_count INTEGER,
    ledger_failed_tx_count INTEGER,
    is_payment_op BOOLEAN,
    is_soroban_op BOOLEAN,
    inserted_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);`, q("enriched_history_operations_soroban"))

	// ====================================================================
	// 3. token_transfers_raw
	// ====================================================================
	ddl += fmt.Sprintf(`
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
    inserted_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);`, q("token_transfers_raw"))

	// ====================================================================
	// 4. accounts_current
	// ====================================================================
	ddl += fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    account_id TEXT,
    balance BIGINT,
    buying_liabilities BIGINT,
    selling_liabilities BIGINT,
    sequence_number BIGINT,
    num_subentries INTEGER,
    inflation_destination TEXT,
    flags INTEGER,
    home_domain TEXT,
    master_weight INTEGER,
    threshold_low INTEGER,
    threshold_medium INTEGER,
    threshold_high INTEGER,
    last_modified_ledger BIGINT,
    ledger_sequence BIGINT,
    closed_at TIMESTAMP,
    sponsor_account TEXT,
    num_sponsored INTEGER,
    num_sponsoring INTEGER,
    sequence_ledger BIGINT,
    sequence_time BIGINT,
    ledger_range BIGINT,
    inserted_at TIMESTAMP,
    updated_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);`, q("accounts_current"))

	// ====================================================================
	// 5. trustlines_current
	// ====================================================================
	ddl += fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    account_id TEXT,
    asset_type TEXT,
    asset_issuer TEXT,
    asset_code TEXT,
    liquidity_pool_id TEXT,
    balance BIGINT,
    trust_line_limit BIGINT,
    buying_liabilities BIGINT,
    selling_liabilities BIGINT,
    flags INTEGER,
    last_modified_ledger BIGINT,
    ledger_sequence BIGINT,
    created_at TIMESTAMP,
    sponsor TEXT,
    ledger_range BIGINT,
    inserted_at TIMESTAMP,
    updated_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);`, q("trustlines_current"))

	// ====================================================================
	// 6. offers_current
	// ====================================================================
	ddl += fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    offer_id BIGINT,
    seller_id TEXT,
    selling_asset_type TEXT,
    selling_asset_code TEXT,
    selling_asset_issuer TEXT,
    buying_asset_type TEXT,
    buying_asset_code TEXT,
    buying_asset_issuer TEXT,
    amount BIGINT,
    price_n INTEGER,
    price_d INTEGER,
    price DOUBLE,
    flags INTEGER,
    last_modified_ledger BIGINT,
    ledger_sequence BIGINT,
    created_at TIMESTAMP,
    sponsor TEXT,
    ledger_range BIGINT,
    inserted_at TIMESTAMP,
    updated_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);`, q("offers_current"))

	// ====================================================================
	// 7. claimable_balances_current
	// ====================================================================
	ddl += fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    balance_id TEXT,
    sponsor TEXT,
    asset_type TEXT,
    asset_code TEXT,
    asset_issuer TEXT,
    amount BIGINT,
    claimants_count INTEGER,
    flags INTEGER,
    last_modified_ledger BIGINT,
    ledger_sequence BIGINT,
    closed_at TIMESTAMP,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    inserted_at TIMESTAMP,
    updated_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);`, q("claimable_balances_current"))

	// ====================================================================
	// 8. liquidity_pools_current
	// ====================================================================
	ddl += fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    liquidity_pool_id TEXT,
    pool_type TEXT,
    fee INTEGER,
    trustline_count INTEGER,
    total_pool_shares BIGINT,
    asset_a_type TEXT,
    asset_a_code TEXT,
    asset_a_issuer TEXT,
    asset_a_amount BIGINT,
    asset_b_type TEXT,
    asset_b_code TEXT,
    asset_b_issuer TEXT,
    asset_b_amount BIGINT,
    last_modified_ledger BIGINT,
    ledger_sequence BIGINT,
    closed_at TIMESTAMP,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    inserted_at TIMESTAMP,
    updated_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);`, q("liquidity_pools_current"))

	// ====================================================================
	// 9. native_balances_current
	// ====================================================================
	ddl += fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    account_id TEXT,
    balance BIGINT,
    buying_liabilities BIGINT,
    selling_liabilities BIGINT,
    num_subentries INTEGER,
    num_sponsoring INTEGER,
    num_sponsored INTEGER,
    sequence_number BIGINT,
    last_modified_ledger BIGINT,
    ledger_sequence BIGINT,
    ledger_range BIGINT,
    inserted_at TIMESTAMP,
    updated_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);`, q("native_balances_current"))

	// ====================================================================
	// 10. trades
	// ====================================================================
	ddl += fmt.Sprintf(`
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
    selling_amount BIGINT,
    buyer_account TEXT,
    buying_asset_code TEXT,
    buying_asset_issuer TEXT,
    buying_amount BIGINT,
    price DOUBLE,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    inserted_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);`, q("trades"))

	// ====================================================================
	// 11. effects
	// ====================================================================
	ddl += fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    ledger_sequence BIGINT,
    transaction_hash TEXT,
    operation_index INTEGER,
    effect_index INTEGER,
    effect_type INTEGER,
    effect_type_string TEXT,
    account_id TEXT,
    amount TEXT,
    asset_code TEXT,
    asset_issuer TEXT,
    asset_type TEXT,
    trustline_limit TEXT,
    authorize_flag BOOLEAN,
    clawback_flag BOOLEAN,
    signer_account TEXT,
    signer_weight INTEGER,
    offer_id BIGINT,
    seller_account TEXT,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    inserted_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);`, q("effects"))

	// ====================================================================
	// 12. contract_data_current
	// ====================================================================
	ddl += fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    contract_id TEXT,
    key_hash TEXT,
    durability TEXT,
    asset_type TEXT,
    asset_code TEXT,
    asset_issuer TEXT,
    data_value TEXT,
    last_modified_ledger BIGINT,
    ledger_sequence BIGINT,
    closed_at TIMESTAMP,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    updated_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);`, q("contract_data_current"))

	// ====================================================================
	// 13. contract_code_current
	// ====================================================================
	ddl += fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    contract_code_hash TEXT,
    contract_code_ext_v TEXT,
    n_data_segment_bytes INTEGER,
    n_data_segments INTEGER,
    n_elem_segments INTEGER,
    n_exports INTEGER,
    n_functions INTEGER,
    n_globals INTEGER,
    n_imports INTEGER,
    n_instructions INTEGER,
    n_table_entries INTEGER,
    n_types INTEGER,
    last_modified_ledger BIGINT,
    ledger_sequence BIGINT,
    closed_at TIMESTAMP,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    updated_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);`, q("contract_code_current"))

	// ====================================================================
	// 14. ttl_current
	// ====================================================================
	ddl += fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    key_hash TEXT,
    live_until_ledger_seq BIGINT,
    ttl_remaining INTEGER,
    expired BOOLEAN,
    last_modified_ledger BIGINT,
    ledger_sequence BIGINT,
    closed_at TIMESTAMP,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    updated_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);`, q("ttl_current"))

	// ====================================================================
	// 15. evicted_keys
	// ====================================================================
	ddl += fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    contract_id TEXT,
    key_hash TEXT,
    ledger_sequence BIGINT,
    closed_at TIMESTAMP,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    inserted_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);`, q("evicted_keys"))

	// ====================================================================
	// 16. restored_keys
	// ====================================================================
	ddl += fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    contract_id TEXT,
    key_hash TEXT,
    ledger_sequence BIGINT,
    closed_at TIMESTAMP,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    inserted_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);`, q("restored_keys"))

	// ====================================================================
	// 17. config_settings_current
	// ====================================================================
	ddl += fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    config_setting_id INTEGER,
    ledger_max_instructions BIGINT,
    tx_max_instructions BIGINT,
    fee_rate_per_instructions_increment BIGINT,
    tx_memory_limit BIGINT,
    ledger_max_read_ledger_entries BIGINT,
    ledger_max_read_bytes BIGINT,
    ledger_max_write_ledger_entries BIGINT,
    ledger_max_write_bytes BIGINT,
    tx_max_read_ledger_entries BIGINT,
    tx_max_read_bytes BIGINT,
    tx_max_write_ledger_entries BIGINT,
    tx_max_write_bytes BIGINT,
    contract_max_size_bytes BIGINT,
    config_setting_xdr TEXT,
    last_modified_ledger INTEGER,
    ledger_sequence BIGINT,
    closed_at TIMESTAMP,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    updated_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);`, q("config_settings_current"))

	// ====================================================================
	// 18. token_registry
	// ====================================================================
	ddl += fmt.Sprintf(`
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
    updated_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);`, q("token_registry"))

	// ====================================================================
	// 19. contract_invocations_raw
	// ====================================================================
	ddl += fmt.Sprintf(`
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
    inserted_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);`, q("contract_invocations_raw"))

	// ====================================================================
	// 20. contract_metadata
	// ====================================================================
	ddl += fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    contract_id TEXT,
    creator_address TEXT,
    wasm_hash TEXT,
    created_ledger BIGINT,
    created_at TIMESTAMP,
    inserted_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);`, q("contract_metadata"))

	// ====================================================================
	// 21. semantic_activities
	// ====================================================================
	ddl += fmt.Sprintf(`
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
    amount DOUBLE,
    is_soroban BOOLEAN,
    soroban_function_name TEXT,
    transaction_hash TEXT,
    operation_index INTEGER,
    successful BOOLEAN,
    fee_charged BIGINT,
    created_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);`, q("semantic_activities"))

	// ====================================================================
	// 22. semantic_entities_contracts (includes wallet_type, wallet_signers from ALTER TABLE)
	// ====================================================================
	ddl += fmt.Sprintf(`
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
    updated_at TIMESTAMP,
    wallet_type TEXT,
    wallet_signers TEXT,
    era_id TEXT,
    version_label TEXT
);`, q("semantic_entities_contracts"))

	// ====================================================================
	// 23. semantic_flows_value
	// ====================================================================
	ddl += fmt.Sprintf(`
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
    amount DOUBLE,
    transaction_hash TEXT,
    operation_type INTEGER,
    successful BOOLEAN,
    created_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);`, q("semantic_flows_value"))

	// ====================================================================
	// 24. semantic_contract_functions
	// ====================================================================
	ddl += fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    contract_id TEXT,
    function_name TEXT,
    total_calls BIGINT,
    successful_calls BIGINT,
    failed_calls BIGINT,
    unique_callers BIGINT,
    first_called TIMESTAMP,
    last_called TIMESTAMP,
    updated_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);`, q("semantic_contract_functions"))

	// ====================================================================
	// 25. semantic_asset_stats
	// ====================================================================
	ddl += fmt.Sprintf(`
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
    transfer_count_24h BIGINT,
    transfer_volume_24h DOUBLE,
    transfer_count_7d BIGINT,
    transfer_volume_7d DOUBLE,
    mint_count_24h BIGINT,
    burn_count_24h BIGINT,
    first_seen TIMESTAMP,
    last_transfer TIMESTAMP,
    updated_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);`, q("semantic_asset_stats"))

	// ====================================================================
	// 26. semantic_dex_pairs
	// ====================================================================
	ddl += fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    pair_key TEXT,
    selling_asset_code TEXT,
    selling_asset_issuer TEXT,
    buying_asset_code TEXT,
    buying_asset_issuer TEXT,
    trade_count BIGINT,
    trade_count_24h BIGINT,
    trade_count_7d BIGINT,
    selling_volume DOUBLE,
    buying_volume DOUBLE,
    selling_volume_24h DOUBLE,
    buying_volume_24h DOUBLE,
    last_price DOUBLE,
    unique_sellers BIGINT,
    unique_buyers BIGINT,
    first_trade TIMESTAMP,
    last_trade TIMESTAMP,
    updated_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);`, q("semantic_dex_pairs"))

	// ====================================================================
	// 27. semantic_account_summary
	// ====================================================================
	ddl += fmt.Sprintf(`
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
    updated_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);`, q("semantic_account_summary"))

	// ====================================================================
	// 28. accounts_snapshot (append-only from bronze)
	// ====================================================================
	ddl += fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    account_id TEXT,
    balance BIGINT,
    buying_liabilities BIGINT,
    selling_liabilities BIGINT,
    sequence_number BIGINT,
    num_subentries INTEGER,
    inflation_destination TEXT,
    flags INTEGER,
    home_domain TEXT,
    master_weight INTEGER,
    threshold_low INTEGER,
    threshold_medium INTEGER,
    threshold_high INTEGER,
    last_modified_ledger BIGINT,
    ledger_sequence BIGINT,
    closed_at TIMESTAMP,
    sponsor_account TEXT,
    num_sponsored INTEGER,
    num_sponsoring INTEGER,
    sequence_ledger BIGINT,
    sequence_time BIGINT,
    ledger_range BIGINT,
    inserted_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);`, q("accounts_snapshot"))

	// ====================================================================
	// 29. trustlines_snapshot (append-only from bronze)
	// ====================================================================
	ddl += fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    account_id TEXT,
    asset_type TEXT,
    asset_issuer TEXT,
    asset_code TEXT,
    liquidity_pool_id TEXT,
    balance BIGINT,
    trust_line_limit BIGINT,
    buying_liabilities BIGINT,
    selling_liabilities BIGINT,
    flags INTEGER,
    last_modified_ledger BIGINT,
    ledger_sequence BIGINT,
    created_at TIMESTAMP,
    sponsor TEXT,
    ledger_range BIGINT,
    inserted_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);`, q("trustlines_snapshot"))

	// ====================================================================
	// 30. offers_snapshot (append-only from bronze)
	// ====================================================================
	ddl += fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    offer_id BIGINT,
    seller_id TEXT,
    selling_asset_type TEXT,
    selling_asset_code TEXT,
    selling_asset_issuer TEXT,
    buying_asset_type TEXT,
    buying_asset_code TEXT,
    buying_asset_issuer TEXT,
    amount BIGINT,
    price_n INTEGER,
    price_d INTEGER,
    price DOUBLE,
    flags INTEGER,
    last_modified_ledger BIGINT,
    ledger_sequence BIGINT,
    created_at TIMESTAMP,
    sponsor TEXT,
    ledger_range BIGINT,
    inserted_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);`, q("offers_snapshot"))

	// ====================================================================
	// 31. account_signers_snapshot (append-only from bronze)
	// ====================================================================
	ddl += fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    account_id TEXT,
    signer TEXT,
    weight INTEGER,
    sponsor TEXT,
    deleted BOOLEAN,
    last_modified_ledger BIGINT,
    ledger_sequence BIGINT,
    closed_at TIMESTAMP,
    ledger_range BIGINT,
    inserted_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);`, q("account_signers_snapshot"))

	return ddl
}
