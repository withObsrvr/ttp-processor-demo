-- DuckLake Bronze Schema for Stellar Data
-- This schema defines the Bronze layer tables in the DuckLake catalog
-- Tables are partitioned by ledger_range for efficient querying

-- Note: This SQL should be executed via DuckDB after attaching the DuckLake catalog
-- The tables will be stored in Parquet format on S3/B2 storage

-- Core tables
CREATE TABLE IF NOT EXISTS bronze.ledgers_row_v2 (
    sequence BIGINT,
    ledger_hash TEXT,
    previous_ledger_hash TEXT,
    transaction_count INTEGER,
    operation_count INTEGER,
    successful_tx_count INTEGER,
    failed_tx_count INTEGER,
    tx_set_operation_count INTEGER,
    closed_at TIMESTAMP,
    total_coins BIGINT,
    fee_pool BIGINT,
    base_fee INTEGER,
    base_reserve INTEGER,
    max_tx_set_size INTEGER,
    protocol_version INTEGER,
    ledger_header TEXT,
    soroban_fee_write_1kb BIGINT,
    soroban_tx_max_read_ledger_entries INTEGER,
    soroban_tx_max_read_bytes INTEGER,
    soroban_tx_max_write_ledger_entries INTEGER,
    soroban_tx_max_write_bytes INTEGER,
    soroban_tx_max_size_bytes INTEGER,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    era_id TEXT,
    version_label TEXT
);

CREATE TABLE IF NOT EXISTS bronze.transactions_row_v2 (
    transaction_hash TEXT,
    ledger_sequence BIGINT,
    application_order INTEGER,
    account TEXT,
    account_muxed TEXT,
    account_sequence BIGINT,
    max_fee BIGINT,
    operation_count INTEGER,
    tx_envelope TEXT,
    tx_result TEXT,
    tx_meta TEXT,
    tx_fee_meta TEXT,
    successful BOOLEAN,
    closed_at TIMESTAMP,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    era_id TEXT,
    version_label TEXT
);

CREATE TABLE IF NOT EXISTS bronze.operations_row_v2 (
    operation_id TEXT,
    transaction_hash TEXT,
    ledger_sequence BIGINT,
    application_order INTEGER,
    type INTEGER,
    type_string TEXT,
    details TEXT,
    source_account TEXT,
    source_account_muxed TEXT,
    successful BOOLEAN,
    transaction_successful BOOLEAN,
    closed_at TIMESTAMP,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    era_id TEXT,
    version_label TEXT
);

CREATE TABLE IF NOT EXISTS bronze.effects_row_v1 (
    operation_id TEXT,
    ledger_sequence BIGINT,
    account TEXT,
    account_muxed TEXT,
    type INTEGER,
    type_string TEXT,
    details TEXT,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    era_id TEXT,
    version_label TEXT
);

CREATE TABLE IF NOT EXISTS bronze.trades_row_v1 (
    ledger_sequence BIGINT,
    offer_id BIGINT,
    base_offer_id BIGINT,
    base_account TEXT,
    base_asset_type TEXT,
    base_asset_code TEXT,
    base_asset_issuer TEXT,
    base_amount BIGINT,
    counter_offer_id BIGINT,
    counter_account TEXT,
    counter_asset_type TEXT,
    counter_asset_code TEXT,
    counter_asset_issuer TEXT,
    counter_amount BIGINT,
    price_n BIGINT,
    price_d BIGINT,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    era_id TEXT,
    version_label TEXT
);

-- Account tables
CREATE TABLE IF NOT EXISTS bronze.accounts_snapshot_v1 (
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
    last_modified_ledger INTEGER,
    ledger_entry_change INTEGER,
    deleted BOOLEAN,
    sponsor TEXT,
    num_sponsored INTEGER,
    num_sponsoring INTEGER,
    sequence_ledger BIGINT,
    sequence_time BIGINT,
    signers TEXT,
    closed_at TIMESTAMP,
    ledger_sequence BIGINT,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    era_id TEXT,
    version_label TEXT
);

CREATE TABLE IF NOT EXISTS bronze.trustlines_snapshot_v1 (
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
    last_modified_ledger INTEGER,
    ledger_entry_change INTEGER,
    deleted BOOLEAN,
    sponsor TEXT,
    closed_at TIMESTAMP,
    ledger_sequence BIGINT,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    era_id TEXT,
    version_label TEXT
);

CREATE TABLE IF NOT EXISTS bronze.account_signers_snapshot_v1 (
    account_id TEXT,
    signer TEXT,
    weight INTEGER,
    sponsor TEXT,
    last_modified_ledger INTEGER,
    ledger_entry_change INTEGER,
    deleted BOOLEAN,
    closed_at TIMESTAMP,
    ledger_sequence BIGINT,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    era_id TEXT,
    version_label TEXT
);

CREATE TABLE IF NOT EXISTS bronze.native_balances_snapshot_v1 (
    account_id TEXT,
    balance BIGINT,
    buying_liabilities BIGINT,
    selling_liabilities BIGINT,
    num_subentries INTEGER,
    num_sponsoring INTEGER,
    num_sponsored INTEGER,
    sequence_number BIGINT,
    last_modified_ledger INTEGER,
    ledger_entry_change INTEGER,
    deleted BOOLEAN,
    closed_at TIMESTAMP,
    ledger_sequence BIGINT,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    era_id TEXT,
    version_label TEXT
);

-- Market tables
CREATE TABLE IF NOT EXISTS bronze.offers_snapshot_v1 (
    seller_id TEXT,
    offer_id BIGINT,
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
    last_modified_ledger INTEGER,
    ledger_entry_change INTEGER,
    deleted BOOLEAN,
    sponsor TEXT,
    closed_at TIMESTAMP,
    ledger_sequence BIGINT,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    era_id TEXT,
    version_label TEXT
);

CREATE TABLE IF NOT EXISTS bronze.liquidity_pools_snapshot_v1 (
    liquidity_pool_id TEXT,
    type INTEGER,
    fee INTEGER,
    trustline_count BIGINT,
    pool_share_count BIGINT,
    asset_a_type TEXT,
    asset_a_code TEXT,
    asset_a_issuer TEXT,
    asset_a_amount BIGINT,
    asset_b_type TEXT,
    asset_b_code TEXT,
    asset_b_issuer TEXT,
    asset_b_amount BIGINT,
    last_modified_ledger INTEGER,
    ledger_entry_change INTEGER,
    deleted BOOLEAN,
    closed_at TIMESTAMP,
    ledger_sequence BIGINT,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    era_id TEXT,
    version_label TEXT
);

CREATE TABLE IF NOT EXISTS bronze.claimable_balances_snapshot_v1 (
    balance_id TEXT,
    asset_type TEXT,
    asset_code TEXT,
    asset_issuer TEXT,
    asset_amount BIGINT,
    sponsor TEXT,
    flags INTEGER,
    claimants TEXT,
    last_modified_ledger INTEGER,
    ledger_entry_change INTEGER,
    deleted BOOLEAN,
    closed_at TIMESTAMP,
    ledger_sequence BIGINT,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    era_id TEXT,
    version_label TEXT
);

-- Soroban tables
CREATE TABLE IF NOT EXISTS bronze.contract_events_stream_v1 (
    event_id TEXT,
    contract_id TEXT,
    ledger_sequence BIGINT,
    transaction_hash TEXT,
    closed_at TIMESTAMP,
    event_type TEXT,
    in_successful_contract_call BOOLEAN,
    topics_json TEXT,
    topics_decoded TEXT,
    data_xdr TEXT,
    data_decoded TEXT,
    topic_count INTEGER,
    operation_index INTEGER,
    event_index INTEGER,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    era_id TEXT,
    version_label TEXT
);

CREATE TABLE IF NOT EXISTS bronze.contract_data_snapshot_v1 (
    contract_id TEXT,
    ledger_sequence BIGINT,
    ledger_key_hash TEXT,
    contract_key_type TEXT,
    contract_durability TEXT,
    asset_code TEXT,
    asset_issuer TEXT,
    asset_type TEXT,
    balance_holder TEXT,
    balance TEXT,
    last_modified_ledger INTEGER,
    ledger_entry_change INTEGER,
    deleted BOOLEAN,
    closed_at TIMESTAMP,
    contract_data_xdr TEXT,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    era_id TEXT,
    version_label TEXT
);

CREATE TABLE IF NOT EXISTS bronze.contract_code_snapshot_v1 (
    contract_code_hash TEXT,
    contract_code_ext_v INTEGER,
    last_modified_ledger INTEGER,
    ledger_entry_change INTEGER,
    deleted BOOLEAN,
    closed_at TIMESTAMP,
    n_instructions BIGINT,
    n_functions INTEGER,
    n_globals INTEGER,
    n_table_entries INTEGER,
    n_types INTEGER,
    n_data_segments INTEGER,
    n_elem_segments INTEGER,
    n_imports INTEGER,
    n_exports INTEGER,
    n_data_segment_bytes BIGINT,
    ledger_sequence BIGINT,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    era_id TEXT,
    version_label TEXT
);

CREATE TABLE IF NOT EXISTS bronze.restored_keys_state_v1 (
    key_hash TEXT,
    ledger_sequence INTEGER,
    contract_id TEXT,
    key_type TEXT,
    durability TEXT,
    restored_from_ledger INTEGER,
    closed_at TIMESTAMP,
    ledger_range INTEGER,
    created_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);

-- State tables
CREATE TABLE IF NOT EXISTS bronze.ttl_snapshot_v1 (
    key_hash TEXT,
    live_until_ledger_seq INTEGER,
    last_modified_ledger INTEGER,
    ledger_entry_change INTEGER,
    deleted BOOLEAN,
    closed_at TIMESTAMP,
    ledger_sequence BIGINT,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    era_id TEXT,
    version_label TEXT
);

CREATE TABLE IF NOT EXISTS bronze.evicted_keys_state_v1 (
    key_hash TEXT,
    contract_id TEXT,
    key_type TEXT,
    durability TEXT,
    evicted_from_ledger INTEGER,
    closed_at TIMESTAMP,
    ledger_sequence BIGINT,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    era_id TEXT,
    version_label TEXT
);

CREATE TABLE IF NOT EXISTS bronze.config_settings_snapshot_v1 (
    config_setting_id INTEGER,
    contract_max_size_bytes BIGINT,
    ledger_max_instructions BIGINT,
    tx_max_instructions BIGINT,
    fee_rate_per_instructions_increment BIGINT,
    tx_memory_limit INTEGER,
    ledger_max_read_ledger_entries INTEGER,
    ledger_max_read_bytes INTEGER,
    ledger_max_write_ledger_entries INTEGER,
    ledger_max_write_bytes INTEGER,
    tx_max_read_ledger_entries INTEGER,
    tx_max_read_bytes INTEGER,
    tx_max_write_ledger_entries INTEGER,
    tx_max_write_bytes INTEGER,
    tx_max_contract_events_size_bytes INTEGER,
    ledger_max_transaction_sizes_bytes INTEGER,
    tx_max_size_bytes INTEGER,
    bucket_list_size_window_sample_size INTEGER,
    eviction_scan_size BIGINT,
    last_modified_ledger INTEGER,
    ledger_entry_change INTEGER,
    deleted BOOLEAN,
    closed_at TIMESTAMP,
    ledger_sequence BIGINT,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    era_id TEXT,
    version_label TEXT
);
