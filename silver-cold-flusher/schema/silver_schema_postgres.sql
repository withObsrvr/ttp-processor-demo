-- DuckLake Silver Schema for Stellar Data
-- Created for Cycle 4: Silver Cold Flusher
--
-- This schema defines 18 silver tables in DuckLake (Parquet storage)
-- All tables partitioned by ledger_range for efficient queries
--
-- Usage:
--   Run this in DuckDB with ducklake extension loaded and catalog attached

-- Note: This assumes you've already attached the DuckLake catalog:
-- ATTACH 'ducklake:postgres://...' AS testnet_catalog (DATA_PATH '...', METADATA_SCHEMA 'silver_meta');

-- =============================================================================
-- ENRICHED / EVENT TABLES (3 tables)
-- =============================================================================

-- 1. enriched_history_operations - Enriched operations with denormalized data
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.enriched_history_operations (
    transaction_hash VARCHAR NOT NULL,
    operation_index INTEGER NOT NULL,
    ledger_sequence BIGINT NOT NULL,
    source_account VARCHAR,
    type INTEGER,
    type_string VARCHAR,
    created_at TIMESTAMP,
    transaction_successful BOOLEAN,
    operation_result_code VARCHAR,
    operation_trace_code VARCHAR,
    ledger_range BIGINT,

    -- Asset fields
    source_account_muxed VARCHAR,
    asset VARCHAR,
    asset_type VARCHAR,
    asset_code VARCHAR,
    asset_issuer VARCHAR,
    source_asset VARCHAR,
    source_asset_type VARCHAR,
    source_asset_code VARCHAR,
    source_asset_issuer VARCHAR,

    -- Payment fields
    destination VARCHAR,
    destination_muxed VARCHAR,
    amount BIGINT,
    source_amount BIGINT,
    from_account VARCHAR,
    from_muxed VARCHAR,
    to_address VARCHAR,
    to_muxed VARCHAR,

    -- Trust/Offer fields
    limit_amount BIGINT,
    offer_id BIGINT,
    selling_asset VARCHAR,
    selling_asset_type VARCHAR,
    selling_asset_code VARCHAR,
    selling_asset_issuer VARCHAR,
    buying_asset VARCHAR,
    buying_asset_type VARCHAR,
    buying_asset_code VARCHAR,
    buying_asset_issuer VARCHAR,
    price_n INTEGER,
    price_d INTEGER,
    price DECIMAL(20,7),

    -- Account management
    starting_balance BIGINT,
    home_domain VARCHAR,
    inflation_dest VARCHAR,
    set_flags INTEGER,
    set_flags_s VARCHAR[],
    clear_flags INTEGER,
    clear_flags_s VARCHAR[],
    master_key_weight INTEGER,
    low_threshold INTEGER,
    med_threshold INTEGER,
    high_threshold INTEGER,

    -- Signer fields
    signer_account_id VARCHAR,
    signer_key VARCHAR,
    signer_weight INTEGER,

    -- Data entry
    data_name VARCHAR,
    data_value VARCHAR,

    -- Soroban fields
    host_function_type VARCHAR,
    parameters VARCHAR,
    address VARCHAR,
    contract_id VARCHAR,
    function_name VARCHAR,

    -- Claimable balance
    balance_id VARCHAR,
    claimant VARCHAR,
    claimant_muxed VARCHAR,
    predicate VARCHAR,

    -- Liquidity pool
    liquidity_pool_id VARCHAR,
    reserve_a_asset VARCHAR,
    reserve_a_amount BIGINT,
    reserve_b_asset VARCHAR,
    reserve_b_amount BIGINT,
    shares BIGINT,
    shares_received BIGINT,

    -- Account merge
    into_account VARCHAR,
    into_muxed VARCHAR,

    -- Sponsorship
    sponsor VARCHAR,
    sponsored_id VARCHAR,
    begin_sponsor VARCHAR,

    -- Transaction enrichment
    tx_successful BOOLEAN,
    tx_fee_charged BIGINT,
    tx_max_fee BIGINT,
    tx_operation_count INTEGER,
    tx_memo_type VARCHAR,
    tx_memo VARCHAR,

    -- Ledger enrichment
    ledger_closed_at TIMESTAMP,
    ledger_total_coins BIGINT,
    ledger_fee_pool BIGINT,
    ledger_base_fee INTEGER,
    ledger_base_reserve INTEGER,
    ledger_transaction_count INTEGER,
    ledger_operation_count INTEGER,
    ledger_successful_tx_count INTEGER,
    ledger_failed_tx_count INTEGER,

    -- Derived fields
    is_payment_op BOOLEAN,
    is_soroban_op BOOLEAN,

    PRIMARY KEY (transaction_hash, operation_index)
);

-- 2. token_transfers_raw - Token transfer events
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.token_transfers_raw (
    timestamp TIMESTAMP NOT NULL,
    transaction_hash VARCHAR NOT NULL,
    ledger_sequence BIGINT NOT NULL,
    source_type VARCHAR NOT NULL,
    from_account VARCHAR,
    to_account VARCHAR,
    asset_code VARCHAR,
    asset_issuer VARCHAR,
    amount BIGINT,
    token_contract_id VARCHAR,
    operation_type INTEGER NOT NULL,
    transaction_successful BOOLEAN NOT NULL,
    ledger_range BIGINT,
    PRIMARY KEY (transaction_hash, timestamp)
);

-- 3. soroban_history_operations - Soroban-specific operations
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.soroban_history_operations (
    transaction_hash VARCHAR NOT NULL,
    operation_index INTEGER NOT NULL,
    ledger_sequence BIGINT NOT NULL,
    source_account VARCHAR,
    created_at TIMESTAMP,
    transaction_successful BOOLEAN,
    host_function_type VARCHAR,
    contract_id VARCHAR,
    function_name VARCHAR,
    parameters VARCHAR,
    ledger_range BIGINT,
    PRIMARY KEY (transaction_hash, operation_index)
);

-- =============================================================================
-- CURRENT STATE TABLES (10 tables)
-- =============================================================================

-- 4. accounts_current - Latest account state
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.accounts_current (
    account_id VARCHAR PRIMARY KEY,
    balance VARCHAR NOT NULL,
    sequence_number BIGINT NOT NULL,
    num_subentries INTEGER NOT NULL,
    num_sponsoring INTEGER NOT NULL,
    num_sponsored INTEGER NOT NULL,
    home_domain VARCHAR,
    master_weight INTEGER NOT NULL,
    low_threshold INTEGER NOT NULL,
    med_threshold INTEGER NOT NULL,
    high_threshold INTEGER NOT NULL,
    flags INTEGER NOT NULL,
    auth_required BOOLEAN NOT NULL,
    auth_revocable BOOLEAN NOT NULL,
    auth_immutable BOOLEAN NOT NULL,
    auth_clawback_enabled BOOLEAN NOT NULL,
    signers VARCHAR,
    sponsor_account VARCHAR,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    last_modified_ledger BIGINT NOT NULL,
    ledger_range BIGINT NOT NULL,
    era_id VARCHAR,
    version_label VARCHAR
);

-- 5. trustlines_current - Latest trustline state
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.trustlines_current (
    account_id VARCHAR NOT NULL,
    asset_code VARCHAR NOT NULL,
    asset_issuer VARCHAR NOT NULL,
    asset_type VARCHAR NOT NULL,
    balance VARCHAR NOT NULL,
    trust_limit VARCHAR NOT NULL,
    buying_liabilities VARCHAR NOT NULL,
    selling_liabilities VARCHAR NOT NULL,
    authorized BOOLEAN NOT NULL,
    authorized_to_maintain_liabilities BOOLEAN NOT NULL,
    clawback_enabled BOOLEAN NOT NULL,
    ledger_sequence BIGINT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    last_modified_ledger BIGINT NOT NULL,
    ledger_range BIGINT NOT NULL,
    era_id VARCHAR,
    version_label VARCHAR,
    PRIMARY KEY (account_id, asset_code, asset_issuer, asset_type)
);

-- 6. offers_current - Latest DEX offers
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.offers_current (
    offer_id BIGINT PRIMARY KEY,
    seller_account VARCHAR NOT NULL,
    closed_at TIMESTAMP NOT NULL,
    selling_asset_type VARCHAR NOT NULL,
    selling_asset_code VARCHAR,
    selling_asset_issuer VARCHAR,
    buying_asset_type VARCHAR NOT NULL,
    buying_asset_code VARCHAR,
    buying_asset_issuer VARCHAR,
    amount VARCHAR NOT NULL,
    price VARCHAR NOT NULL,
    flags INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL,
    last_modified_ledger BIGINT NOT NULL,
    ledger_range BIGINT NOT NULL,
    era_id VARCHAR,
    version_label VARCHAR
);

-- 7. account_signers_current - Latest account signers
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.account_signers_current (
    account_id VARCHAR NOT NULL,
    signer VARCHAR NOT NULL,
    weight INTEGER NOT NULL,
    sponsor VARCHAR,
    last_modified_ledger BIGINT NOT NULL,
    ledger_range BIGINT NOT NULL,
    era_id VARCHAR,
    version_label VARCHAR,
    PRIMARY KEY (account_id, signer)
);

-- 8. claimable_balances_current - Latest claimable balances
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.claimable_balances_current (
    balance_id VARCHAR PRIMARY KEY,
    asset_type VARCHAR NOT NULL,
    asset_code VARCHAR NOT NULL,
    asset_issuer VARCHAR NOT NULL,
    amount VARCHAR NOT NULL,
    claimants VARCHAR NOT NULL,
    flags INTEGER NOT NULL,
    sponsor_account VARCHAR,
    last_modified_ledger BIGINT NOT NULL,
    ledger_range BIGINT NOT NULL,
    era_id VARCHAR,
    version_label VARCHAR
);

-- 9. liquidity_pools_current - Latest liquidity pool state
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.liquidity_pools_current (
    liquidity_pool_id VARCHAR PRIMARY KEY,
    pool_type VARCHAR NOT NULL,
    fee INTEGER NOT NULL,
    trustline_count INTEGER NOT NULL,
    total_pool_shares BIGINT NOT NULL,
    asset_a_type VARCHAR NOT NULL,
    asset_a_code VARCHAR,
    asset_a_issuer VARCHAR,
    asset_a_amount BIGINT NOT NULL,
    asset_b_type VARCHAR NOT NULL,
    asset_b_code VARCHAR,
    asset_b_issuer VARCHAR,
    asset_b_amount BIGINT NOT NULL,
    last_modified_ledger BIGINT NOT NULL,
    ledger_range BIGINT NOT NULL,
    era_id VARCHAR,
    version_label VARCHAR
);

-- 10. contract_data_current - Latest Soroban contract data
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.contract_data_current (
    contract_id VARCHAR NOT NULL,
    contract_key_type VARCHAR NOT NULL,
    contract_durability VARCHAR NOT NULL,
    asset_code VARCHAR,
    asset_issuer VARCHAR,
    last_modified_ledger BIGINT NOT NULL,
    ledger_range BIGINT NOT NULL,
    era_id VARCHAR,
    version_label VARCHAR,
    PRIMARY KEY (contract_id, contract_key_type)
);

-- 11. contract_code_current - Latest Soroban contract code
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.contract_code_current (
    contract_code_hash VARCHAR PRIMARY KEY,
    contract_code_ext_v INTEGER NOT NULL,
    last_modified_ledger BIGINT NOT NULL,
    ledger_range BIGINT NOT NULL,
    n_instructions BIGINT,
    n_functions BIGINT,
    n_globals BIGINT,
    n_table_entries BIGINT,
    n_types BIGINT,
    n_data_segments BIGINT,
    n_elem_segments BIGINT,
    n_imports BIGINT,
    n_exports BIGINT,
    n_data_segment_bytes BIGINT,
    era_id VARCHAR,
    version_label VARCHAR
);

-- 12. config_settings_current - Latest network config
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.config_settings_current (
    config_setting_id INTEGER PRIMARY KEY,
    contract_max_size_bytes BIGINT,
    ledger_max_instructions BIGINT,
    tx_max_instructions BIGINT,
    fee_rate_per_instructions_increment BIGINT,
    tx_memory_limit BIGINT,
    last_modified_ledger BIGINT NOT NULL,
    ledger_range BIGINT NOT NULL,
    era_id VARCHAR,
    version_label VARCHAR
);

-- 13. ttl_current - Latest TTL entries
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.ttl_current (
    key_hash VARCHAR PRIMARY KEY,
    live_until_ledger_seq BIGINT NOT NULL,
    last_modified_ledger BIGINT NOT NULL,
    ledger_range BIGINT NOT NULL,
    era_id VARCHAR,
    version_label VARCHAR
);

-- =============================================================================
-- SNAPSHOT TABLES (4 tables) - SCD Type 2
-- =============================================================================

-- 14. accounts_snapshot - Full account history
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.accounts_snapshot (
    account_id VARCHAR NOT NULL,
    ledger_sequence BIGINT NOT NULL,
    closed_at TIMESTAMP NOT NULL,
    balance VARCHAR NOT NULL,
    sequence_number BIGINT NOT NULL,
    num_subentries INTEGER NOT NULL,
    num_sponsoring INTEGER NOT NULL,
    num_sponsored INTEGER NOT NULL,
    home_domain VARCHAR,
    master_weight INTEGER NOT NULL,
    low_threshold INTEGER NOT NULL,
    med_threshold INTEGER NOT NULL,
    high_threshold INTEGER NOT NULL,
    flags INTEGER NOT NULL,
    auth_required BOOLEAN NOT NULL,
    auth_revocable BOOLEAN NOT NULL,
    auth_immutable BOOLEAN NOT NULL,
    auth_clawback_enabled BOOLEAN NOT NULL,
    signers VARCHAR,
    sponsor_account VARCHAR,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    ledger_range BIGINT NOT NULL,
    era_id VARCHAR,
    version_label VARCHAR,
    valid_to TIMESTAMP,
    PRIMARY KEY (account_id, ledger_sequence)
);

-- 15. trustlines_snapshot - Full trustline history
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.trustlines_snapshot (
    account_id VARCHAR NOT NULL,
    asset_code VARCHAR NOT NULL,
    asset_issuer VARCHAR NOT NULL,
    asset_type VARCHAR NOT NULL,
    balance VARCHAR NOT NULL,
    trust_limit VARCHAR NOT NULL,
    buying_liabilities VARCHAR NOT NULL,
    selling_liabilities VARCHAR NOT NULL,
    authorized BOOLEAN NOT NULL,
    authorized_to_maintain_liabilities BOOLEAN NOT NULL,
    clawback_enabled BOOLEAN NOT NULL,
    ledger_sequence BIGINT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    ledger_range BIGINT NOT NULL,
    era_id VARCHAR,
    version_label VARCHAR,
    valid_to TIMESTAMP,
    PRIMARY KEY (account_id, asset_code, asset_issuer, asset_type, ledger_sequence)
);

-- 16. offers_snapshot - Full offer history
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.offers_snapshot (
    offer_id BIGINT NOT NULL,
    seller_account VARCHAR NOT NULL,
    ledger_sequence BIGINT NOT NULL,
    closed_at TIMESTAMP NOT NULL,
    selling_asset_type VARCHAR NOT NULL,
    selling_asset_code VARCHAR,
    selling_asset_issuer VARCHAR,
    buying_asset_type VARCHAR NOT NULL,
    buying_asset_code VARCHAR,
    buying_asset_issuer VARCHAR,
    amount VARCHAR NOT NULL,
    price VARCHAR NOT NULL,
    flags INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL,
    ledger_range BIGINT NOT NULL,
    era_id VARCHAR,
    version_label VARCHAR,
    valid_to TIMESTAMP,
    PRIMARY KEY (offer_id, ledger_sequence)
);

-- 17. account_signers_snapshot - Full signer history
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.account_signers_snapshot (
    account_id VARCHAR NOT NULL,
    signer VARCHAR NOT NULL,
    ledger_sequence BIGINT NOT NULL,
    closed_at TIMESTAMP NOT NULL,
    weight INTEGER NOT NULL,
    sponsor VARCHAR,
    ledger_range BIGINT NOT NULL,
    era_id VARCHAR,
    version_label VARCHAR,
    valid_to TIMESTAMP,
    PRIMARY KEY (account_id, signer, ledger_sequence)
);

-- =============================================================================
-- INDEXES (optional - DuckDB handles these differently than PostgreSQL)
-- =============================================================================

-- Note: DuckDB automatically creates indexes for PRIMARY KEY columns
-- Additional indexes can be created if needed for specific query patterns

-- =============================================================================
-- COMMENTS
-- =============================================================================

COMMENT ON TABLE testnet_catalog.silver.enriched_history_operations IS 'Enriched operations with denormalized transaction and ledger data';
COMMENT ON TABLE testnet_catalog.silver.token_transfers_raw IS 'All token transfer events (classic and Soroban)';
COMMENT ON TABLE testnet_catalog.silver.accounts_snapshot IS 'SCD Type 2: Full history of account state changes';
COMMENT ON TABLE testnet_catalog.silver.accounts_current IS 'Latest account state (UPSERT pattern)';

-- =============================================================================
-- PARTITIONING NOTES
-- =============================================================================

-- All tables include ledger_range column for partitioning:
--   ledger_range = FLOOR(ledger_sequence / 100000)
--
-- Benefits:
-- - Partition pruning for range queries
-- - Efficient TTL enforcement (drop old partitions)
-- - Optimized for both point and range queries
--
-- Example queries:
--   WHERE ledger_range = 21  -- Single partition
--   WHERE ledger_range BETWEEN 20 AND 22  -- Multi-partition scan
