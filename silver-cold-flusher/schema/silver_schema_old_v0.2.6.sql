-- DuckLake Silver Schema (Auto-generated for DuckDB)
-- Compatible with Parquet storage - no/NOT NULL/DEFAULT constraints

-- =============================================================================
-- EVENT TABLES (2 tables)
-- =============================================================================

CREATE TABLE IF NOT EXISTS testnet_catalog.silver.enriched_history_operations (
    -- Operation fields (from bronze.operations_row_v2)
    transaction_hash VARCHAR(64),
    operation_index INTEGER,
    ledger_sequence BIGINT,
    source_account VARCHAR(56),
    type INTEGER,
    type_string VARCHAR(50),
    created_at TIMESTAMP,
    transaction_successful BOOLEAN,
    operation_result_code VARCHAR(50),
    operation_trace_code VARCHAR(50),
    ledger_range BIGINT,

    -- Asset fields (various operations)
    source_account_muxed VARCHAR(100),
    asset VARCHAR(100),
    asset_type VARCHAR(20),
    asset_code VARCHAR(12),
    asset_issuer VARCHAR(56),
    source_asset VARCHAR(100),
    source_asset_type VARCHAR(20),
    source_asset_code VARCHAR(12),
    source_asset_issuer VARCHAR(56),

    -- Operation-specific fields (payments, path payments, etc.)
    destination VARCHAR(56),
    destination_muxed VARCHAR(100),
    amount BIGINT,
    source_amount BIGINT,
    from_account VARCHAR(56),
    from_muxed VARCHAR(100),
    to_address VARCHAR(56),  -- 'to' is reserved word in PostgreSQL
    to_muxed VARCHAR(100),

    -- Trust line fields
    limit_amount BIGINT,

    -- Offer fields
    offer_id BIGINT,
    selling_asset VARCHAR(100),
    selling_asset_type VARCHAR(20),
    selling_asset_code VARCHAR(12),
    selling_asset_issuer VARCHAR(56),
    buying_asset VARCHAR(100),
    buying_asset_type VARCHAR(20),
    buying_asset_code VARCHAR(12),
    buying_asset_issuer VARCHAR(56),
    price_n INTEGER,
    price_d INTEGER,
    price DECIMAL(20,7),

    -- Account management fields
    starting_balance BIGINT,
    home_domain VARCHAR(255),
    inflation_dest VARCHAR(56),

    -- Flags and thresholds
    set_flags INTEGER,
    set_flags_s TEXT[],
    clear_flags INTEGER,
    clear_flags_s TEXT[],
    master_key_weight INTEGER,
    low_threshold INTEGER,
    med_threshold INTEGER,
    high_threshold INTEGER,

    -- Signer fields
    signer_account_id VARCHAR(56),
    signer_key VARCHAR(100),
    signer_weight INTEGER,

    -- Data entry fields
    data_name VARCHAR(100),
    data_value TEXT,

    -- Soroban fields (contract invocations)
    host_function_type VARCHAR(50),
    parameters TEXT,
    address VARCHAR(100),
    contract_id VARCHAR(100),
    function_name VARCHAR(100),

    -- Claimable balance fields
    balance_id VARCHAR(100),
    claimant VARCHAR(56),
    claimant_muxed VARCHAR(100),
    predicate TEXT,

    -- Liquidity pool fields
    liquidity_pool_id VARCHAR(100),
    reserve_a_asset VARCHAR(100),
    reserve_a_amount BIGINT,
    reserve_b_asset VARCHAR(100),
    reserve_b_amount BIGINT,
    shares BIGINT,
    shares_received BIGINT,

    -- Account merge
    into_account VARCHAR(56),  -- 'into' is reserved word in PostgreSQL
    into_muxed VARCHAR(100),

    -- Sponsorship
    sponsor VARCHAR(56),
    sponsored_id VARCHAR(56),

    -- Protocol version
    begin_sponsor VARCHAR(56),

    -- Transaction fields (enriched from bronze.transactions_row_v2)
    tx_successful BOOLEAN,
    tx_fee_charged BIGINT,
    tx_max_fee BIGINT,
    tx_operation_count INTEGER,
    tx_memo_type VARCHAR(20),
    tx_memo TEXT,

    -- Ledger fields (enriched from bronze.ledgers_row_v2)
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

    -- Metadata
    inserted_at TIMESTAMP
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_enriched_ops_ledger_sequence ON enriched_history_operations(ledger_sequence);
CREATE INDEX IF NOT EXISTS idx_enriched_ops_source_account ON enriched_history_operations(source_account);
CREATE INDEX IF NOT EXISTS idx_enriched_ops_type ON enriched_history_operations(type);
CREATE INDEX IF NOT EXISTS idx_enriched_ops_created_at ON enriched_history_operations(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_enriched_ops_ledger_range ON enriched_history_operations(ledger_range);

-- ============================================================================
-- PHASE 4: ANALYTICS TABLES (1 table)
-- ============================================================================

-- Table: token_transfers_raw
-- Unified view of classic Stellar payments + Soroban token transfers
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.token_transfers_raw (
    -- Common fields
    timestamp TIMESTAMP,
    transaction_hash VARCHAR(64),
    ledger_sequence BIGINT,
    source_type VARCHAR(10), -- 'classic' or 'soroban'

    -- Classic payment fields
    from_account VARCHAR(56),
    to_account VARCHAR(56),
    asset_code VARCHAR(12),
    asset_issuer VARCHAR(56),
    amount BIGINT,

    -- Soroban token fields
    token_contract_id VARCHAR(100),

    -- Transaction context
    operation_type INTEGER,
    transaction_successful BOOLEAN,

    -- Metadata
    inserted_at TIMESTAMP

    -- Note: No natural primary key exists due to UNION of classic + soroban
    -- Use unique index instead
);

-- Index for token transfers (handles NULLs properly)
CREATE INDEX IF NOT EXISTS idx_token_transfers_unique
ON token_transfers_raw(
    transaction_hash,
    ledger_sequence,
    source_type,
    COALESCE(from_account, ''),
    COALESCE(token_contract_id, '')
);

-- Indexes for token transfer queries
CREATE INDEX IF NOT EXISTS idx_token_transfers_timestamp ON token_transfers_raw(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_token_transfers_from ON token_transfers_raw(from_account);
CREATE INDEX IF NOT EXISTS idx_token_transfers_to ON token_transfers_raw(to_account);
CREATE INDEX IF NOT EXISTS idx_token_transfers_asset ON token_transfers_raw(asset_code, asset_issuer);
CREATE INDEX IF NOT EXISTS idx_token_transfers_contract ON token_transfers_raw(token_contract_id);
CREATE INDEX IF NOT EXISTS idx_token_transfers_ledger ON token_transfers_raw(ledger_sequence);

-- ============================================================================
-- PHASE 1: CURRENT STATE TABLES (10 tables) - Starting with 5 most important
-- ============================================================================

-- Table: accounts_current
-- Current state of all accounts (latest snapshot per account)

-- =============================================================================
-- CURRENT STATE TABLES (10 tables)
-- =============================================================================

-- Silver Hot Buffer: Current State Tables (FIXED SCHEMA)
-- These tables track the current/latest state of entities using UPSERT pattern
-- Updated to use Stellar internal naming convention (low_threshold, med_threshold, high_threshold)

-- 1. accounts_current - Latest state of all accounts
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.accounts_current (
    account_id            TEXT,
    balance               TEXT,
    sequence_number       BIGINT,
    num_subentries        INTEGER,
    num_sponsoring        INTEGER,
    num_sponsored         INTEGER,
    home_domain           TEXT,
    master_weight         INTEGER,
    low_threshold         INTEGER,
    med_threshold         INTEGER,
    high_threshold        INTEGER,
    flags                 INTEGER,
    auth_required         BOOLEAN,
    auth_revocable        BOOLEAN,
    auth_immutable        BOOLEAN,
    auth_clawback_enabled BOOLEAN,
    signers               TEXT,
    sponsor_account       TEXT,
    created_at            TIMESTAMP WITH TIME ZONE,
    updated_at            TIMESTAMP WITH TIME ZONE,
    last_modified_ledger  BIGINT,
    ledger_range          BIGINT,
    era_id                TEXT,
    version_label         TEXT
);

CREATE INDEX IF NOT EXISTS idx_accounts_current_modified ON accounts_current(last_modified_ledger);
CREATE INDEX IF NOT EXISTS idx_accounts_current_range ON accounts_current(ledger_range);

-- 2. account_signers_current - Latest signers for each account
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.account_signers_current (
    account_id            TEXT,
    signer                TEXT,
    weight                INTEGER,
    sponsor               TEXT,
    last_modified_ledger  BIGINT,
    ledger_range          BIGINT,
    era_id                TEXT,
    version_label         TEXT
);

CREATE INDEX IF NOT EXISTS idx_account_signers_current_modified ON account_signers_current(last_modified_ledger);

-- 3. trustlines_current - Latest trustlines for all accounts
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.trustlines_current (
    account_id            TEXT,
    asset_code            TEXT,
    asset_issuer          TEXT,
    asset_type            TEXT,
    balance               TEXT,
    trust_limit           TEXT,
    buying_liabilities    TEXT,
    selling_liabilities   TEXT,
    authorized            BOOLEAN,
    authorized_to_maintain_liabilities BOOLEAN,
    clawback_enabled      BOOLEAN,
    created_at            TIMESTAMP WITH TIME ZONE,
    last_modified_ledger  BIGINT,
    ledger_range          BIGINT,
    era_id                TEXT,
    version_label         TEXT
);

CREATE INDEX IF NOT EXISTS idx_trustlines_current_modified ON trustlines_current(last_modified_ledger);
CREATE INDEX IF NOT EXISTS idx_trustlines_current_account ON trustlines_current(account_id);

-- 4. offers_current - Latest active offers
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.offers_current (
    offer_id              BIGINT,
    seller_account        TEXT,
    closed_at             TIMESTAMP WITH TIME ZONE,
    selling_asset_type    TEXT,
    selling_asset_code    TEXT,
    selling_asset_issuer  TEXT,
    buying_asset_type     TEXT,
    buying_asset_code     TEXT,
    buying_asset_issuer   TEXT,
    amount                TEXT,
    price                 TEXT,
    flags                 INTEGER,
    created_at            TIMESTAMP WITH TIME ZONE,
    last_modified_ledger  BIGINT,
    ledger_range          BIGINT,
    era_id                TEXT,
    version_label         TEXT
);

CREATE INDEX IF NOT EXISTS idx_offers_current_modified ON offers_current(last_modified_ledger);
CREATE INDEX IF NOT EXISTS idx_offers_current_seller ON offers_current(seller_account);

-- 5. claimable_balances_current - Latest claimable balances
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.claimable_balances_current (
    balance_id            TEXT,
    asset_type            TEXT,
    asset_code            TEXT,
    asset_issuer          TEXT,
    amount                TEXT,
    claimants             TEXT,  -- JSON array
    flags                 INTEGER,
    sponsor_account       TEXT,
    last_modified_ledger  BIGINT,
    ledger_range          BIGINT,
    era_id                TEXT,
    version_label         TEXT
);

CREATE INDEX IF NOT EXISTS idx_claimable_balances_current_modified ON claimable_balances_current(last_modified_ledger);

-- 6. liquidity_pools_current - Latest liquidity pools
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.liquidity_pools_current (
    liquidity_pool_id     TEXT,
    pool_type             TEXT,
    fee                   INTEGER,
    asset_a_type          TEXT,
    asset_a_code          TEXT,
    asset_a_issuer        TEXT,
    asset_a_amount        TEXT,
    asset_b_type          TEXT,
    asset_b_code          TEXT,
    asset_b_issuer        TEXT,
    asset_b_amount        TEXT,
    total_pool_shares     TEXT,
    pool_share_count      BIGINT,
    last_modified_ledger  BIGINT,
    ledger_range          BIGINT,
    era_id                TEXT,
    version_label         TEXT
);

CREATE INDEX IF NOT EXISTS idx_liquidity_pools_current_modified ON liquidity_pools_current(last_modified_ledger);

-- 7. contract_data_current - Latest Soroban contract data entries
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.contract_data_current (
    contract_id           TEXT,
    ledger_key_hash       TEXT,
    contract_key_type     TEXT,
    durability            TEXT,
    contract_data         TEXT,  -- Base64 XDR
    expiration_ledger     BIGINT,
    last_modified_ledger  BIGINT,
    ledger_range          BIGINT,
    era_id                TEXT,
    version_label         TEXT
);

CREATE INDEX IF NOT EXISTS idx_contract_data_current_modified ON contract_data_current(last_modified_ledger);
CREATE INDEX IF NOT EXISTS idx_contract_data_current_contract ON contract_data_current(contract_id);

-- 8. contract_code_current - Latest Soroban contract code (WASM)
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.contract_code_current (
    contract_code_hash    TEXT,
    contract_code         TEXT,  -- Base64 XDR WASM bytecode
    expiration_ledger     BIGINT,
    last_modified_ledger  BIGINT,
    ledger_range          BIGINT,
    era_id                TEXT,
    version_label         TEXT
);

CREATE INDEX IF NOT EXISTS idx_contract_code_current_modified ON contract_code_current(last_modified_ledger);

-- 9. config_settings_current - Latest protocol config settings
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.config_settings_current (
    config_setting_id     TEXT,
    config_setting        TEXT,  -- Base64 XDR
    last_modified_ledger  BIGINT,
    ledger_range          BIGINT,
    era_id                TEXT,
    version_label         TEXT
);

CREATE INDEX IF NOT EXISTS idx_config_settings_current_modified ON config_settings_current(last_modified_ledger);

-- 10. ttl_current - Latest Soroban state TTL entries
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.ttl_current (
    key_hash              TEXT,
    live_until_ledger     BIGINT,
    last_modified_ledger  BIGINT,
    ledger_range          BIGINT,
    era_id                TEXT,
    version_label         TEXT
);

CREATE INDEX IF NOT EXISTS idx_ttl_current_modified ON ttl_current(last_modified_ledger);

-- Grant permissions

-- =============================================================================
-- SNAPSHOT TABLES (5 tables)
-- =============================================================================

-- Silver Hot Buffer: Snapshot Tables (SCD Type 2)
-- These tables preserve full history with valid_from/valid_to tracking
-- Created for Cycle 3: Incremental SCD Type 2 Pattern

-- 1. accounts_snapshot - Full history of all account state changes
-- SCD Type 2: Multiple rows per account with validity periods
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.accounts_snapshot (
    account_id            TEXT,
    ledger_sequence       BIGINT,
    closed_at             TIMESTAMP WITH TIME ZONE,  -- valid_from (implicit)
    balance               TEXT,
    sequence_number       BIGINT,
    num_subentries        INTEGER,
    num_sponsoring        INTEGER,
    num_sponsored         INTEGER,
    home_domain           TEXT,
    master_weight         INTEGER,
    low_threshold         INTEGER,
    med_threshold         INTEGER,
    high_threshold        INTEGER,
    flags                 INTEGER,
    auth_required         BOOLEAN,
    auth_revocable        BOOLEAN,
    auth_immutable        BOOLEAN,
    auth_clawback_enabled BOOLEAN,
    signers               TEXT,
    sponsor_account       TEXT,
    created_at            TIMESTAMP WITH TIME ZONE,
    updated_at            TIMESTAMP WITH TIME ZONE,
    ledger_range          BIGINT,
    era_id                TEXT,
    version_label         TEXT,
    valid_to              TIMESTAMP WITH TIME ZONE  -- NULL = current version
);

CREATE INDEX IF NOT EXISTS idx_accounts_snapshot_range ON accounts_snapshot(ledger_range);
CREATE INDEX IF NOT EXISTS idx_accounts_snapshot_account ON accounts_snapshot(account_id);
CREATE INDEX IF NOT EXISTS idx_accounts_snapshot_ledger ON accounts_snapshot(ledger_sequence);

-- 2. trustlines_snapshot - Full history of all trustline changes
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.trustlines_snapshot (
    account_id                         TEXT,
    asset_code                         TEXT,
    asset_issuer                       TEXT,
    asset_type                         TEXT,
    balance                            TEXT,
    trust_limit                        TEXT,
    buying_liabilities                 TEXT,
    selling_liabilities                TEXT,
    authorized                         BOOLEAN,
    authorized_to_maintain_liabilities BOOLEAN,
    clawback_enabled                   BOOLEAN,
    ledger_sequence                    BIGINT,
    created_at                         TIMESTAMP WITH TIME ZONE,
    ledger_range                       BIGINT,
    era_id                             TEXT,
    version_label                      TEXT,
    valid_to                           TIMESTAMP WITH TIME ZONE  -- NULL = current version
);

CREATE INDEX IF NOT EXISTS idx_trustlines_snapshot_range ON trustlines_snapshot(ledger_range);
CREATE INDEX IF NOT EXISTS idx_trustlines_snapshot_account ON trustlines_snapshot(account_id);

-- 3. offers_snapshot - Full history of all offer changes
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.offers_snapshot (
    offer_id             BIGINT,
    seller_account       TEXT,
    ledger_sequence      BIGINT,
    closed_at            TIMESTAMP WITH TIME ZONE,
    selling_asset_type   TEXT,
    selling_asset_code   TEXT,
    selling_asset_issuer TEXT,
    buying_asset_type    TEXT,
    buying_asset_code    TEXT,
    buying_asset_issuer  TEXT,
    amount               TEXT,
    price                TEXT,
    flags                INTEGER,
    created_at           TIMESTAMP WITH TIME ZONE,
    ledger_range         BIGINT,
    era_id               TEXT,
    version_label        TEXT,
    valid_to             TIMESTAMP WITH TIME ZONE  -- NULL = current version
);

CREATE INDEX IF NOT EXISTS idx_offers_snapshot_range ON offers_snapshot(ledger_range);
CREATE INDEX IF NOT EXISTS idx_offers_snapshot_offer ON offers_snapshot(offer_id);
CREATE INDEX IF NOT EXISTS idx_offers_snapshot_seller ON offers_snapshot(seller_account);

-- 4. account_signers_snapshot - Full history of account signer changes
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.account_signers_snapshot (
    account_id       TEXT,
    signer           TEXT,
    ledger_sequence  BIGINT,
    closed_at        TIMESTAMP WITH TIME ZONE,  -- valid_from (implicit)
    weight           INTEGER,
    sponsor          TEXT,
    ledger_range     BIGINT,
    era_id           TEXT,
    version_label    TEXT,
    valid_to         TIMESTAMP WITH TIME ZONE  -- NULL = current version
);

CREATE INDEX IF NOT EXISTS idx_account_signers_snapshot_range ON account_signers_snapshot(ledger_range);
CREATE INDEX IF NOT EXISTS idx_account_signers_snapshot_account ON account_signers_snapshot(account_id);

-- 5. claimable_balances_snapshot - Full history of claimable balance changes
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.claimable_balances_snapshot (
    balance_id       TEXT,
    ledger_sequence  BIGINT,
    asset_type       TEXT,
    asset_code       TEXT,
    asset_issuer     TEXT,
    amount           TEXT,
    claimants        TEXT,  -- JSON array
    flags            INTEGER,
    sponsor_account  TEXT,
    ledger_range     BIGINT,
    era_id           TEXT,
    version_label    TEXT,
    valid_to         TIMESTAMP WITH TIME ZONE  -- NULL = current version
);

CREATE INDEX IF NOT EXISTS idx_claimable_balances_snapshot_range ON claimable_balances_snapshot(ledger_range);
CREATE INDEX IF NOT EXISTS idx_claimable_balances_snapshot_balance ON claimable_balances_snapshot(balance_id);

-- Grant permissions

-- Notes on SCD Type 2 Pattern:
-- 1. Each entity can have multiple rows (one per state change)
-- 2. Primary key includes ledger_sequence for versioning
-- 3. closed_at serves as valid_from (from ledger close time)
-- 4. valid_to is NULL for current version, populated for historical versions
-- 5. To get current state: WHERE valid_to IS NULL
-- 6. To get state at time T: WHERE closed_at <= T AND (valid_to IS NULL OR valid_to > T)
-- 7. Partitioning by ledger_range enables efficient boundary updates

