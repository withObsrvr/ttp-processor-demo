-- Silver Hot Buffer Schema
-- PostgreSQL schema for real-time silver transformations
-- Database: silver_hot
-- Created: 2025-12-16 (Cycle 1 Day 1)

-- ============================================================================
-- PHASE 3: ENRICHED OPERATIONS TABLES (2 tables)
-- ============================================================================

-- Table: enriched_history_operations
-- Primary enriched operations with transaction and ledger context
CREATE TABLE IF NOT EXISTS enriched_history_operations (
    -- Operation fields (from bronze.operations_row_v2)
    transaction_hash VARCHAR(64) NOT NULL,
    operation_index INTEGER NOT NULL,
    ledger_sequence BIGINT NOT NULL,
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
    inserted_at TIMESTAMP DEFAULT NOW(),

    -- Primary key
    PRIMARY KEY (transaction_hash, operation_index)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_enriched_ops_ledger_sequence ON enriched_history_operations(ledger_sequence);
CREATE INDEX IF NOT EXISTS idx_enriched_ops_source_account ON enriched_history_operations(source_account);
CREATE INDEX IF NOT EXISTS idx_enriched_ops_type ON enriched_history_operations(type);
CREATE INDEX IF NOT EXISTS idx_enriched_ops_created_at ON enriched_history_operations(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_enriched_ops_ledger_range ON enriched_history_operations(ledger_range);

-- Table: enriched_history_operations_soroban
-- Filtered view of enriched operations for Soroban contract invocations only
CREATE TABLE IF NOT EXISTS enriched_history_operations_soroban (
    LIKE enriched_history_operations INCLUDING ALL
);

-- Indexes for Soroban-specific queries
CREATE INDEX IF NOT EXISTS idx_enriched_soroban_contract_id ON enriched_history_operations_soroban(contract_id);
CREATE INDEX IF NOT EXISTS idx_enriched_soroban_function ON enriched_history_operations_soroban(function_name);

-- ============================================================================
-- PHASE 4: ANALYTICS TABLES (1 table)
-- ============================================================================

-- Table: token_transfers_raw
-- Unified view of classic Stellar payments + Soroban token transfers
CREATE TABLE IF NOT EXISTS token_transfers_raw (
    -- Common fields
    timestamp TIMESTAMP NOT NULL,
    transaction_hash VARCHAR(64) NOT NULL,
    ledger_sequence BIGINT NOT NULL,
    source_type VARCHAR(10) NOT NULL, -- 'classic' or 'soroban'

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
    inserted_at TIMESTAMP DEFAULT NOW()

    -- Note: No natural primary key exists due to UNION of classic + soroban
    -- Use unique index instead
);

-- Unique index for token transfers (handles NULLs properly)
CREATE UNIQUE INDEX IF NOT EXISTS idx_token_transfers_unique
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
CREATE TABLE IF NOT EXISTS accounts_current (
    account_id VARCHAR(56) PRIMARY KEY,
    balance BIGINT,
    buying_liabilities BIGINT,
    selling_liabilities BIGINT,
    sequence_number BIGINT,
    num_subentries INTEGER,
    inflation_destination VARCHAR(56),
    flags INTEGER,
    home_domain VARCHAR(255),
    master_weight INTEGER,
    threshold_low INTEGER,
    threshold_medium INTEGER,
    threshold_high INTEGER,
    last_modified_ledger BIGINT NOT NULL,
    ledger_sequence BIGINT NOT NULL,
    closed_at TIMESTAMP,
    sponsor VARCHAR(56),
    num_sponsored INTEGER,
    num_sponsoring INTEGER,
    sequence_ledger BIGINT,
    sequence_time BIGINT,
    ledger_range BIGINT,

    -- Metadata
    inserted_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_accounts_last_modified ON accounts_current(last_modified_ledger DESC);
CREATE INDEX IF NOT EXISTS idx_accounts_sponsor ON accounts_current(sponsor);

-- Table: trustlines_current
-- Current state of all trustlines
CREATE TABLE IF NOT EXISTS trustlines_current (
    account_id VARCHAR(56) NOT NULL,
    asset_type VARCHAR(20) NOT NULL,
    asset_issuer VARCHAR(56),
    asset_code VARCHAR(12),
    liquidity_pool_id VARCHAR(100),
    balance BIGINT,
    trust_line_limit BIGINT,
    buying_liabilities BIGINT,
    selling_liabilities BIGINT,
    flags INTEGER,
    last_modified_ledger BIGINT NOT NULL,
    ledger_sequence BIGINT NOT NULL,
    created_at TIMESTAMP,
    sponsor VARCHAR(56),
    ledger_range BIGINT,

    -- Metadata
    inserted_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Unique index for trustlines (handles NULLs properly)
CREATE UNIQUE INDEX IF NOT EXISTS idx_trustlines_unique
ON trustlines_current(
    account_id,
    asset_type,
    COALESCE(asset_code, ''),
    COALESCE(asset_issuer, ''),
    COALESCE(liquidity_pool_id, '')
);

CREATE INDEX IF NOT EXISTS idx_trustlines_account ON trustlines_current(account_id);
CREATE INDEX IF NOT EXISTS idx_trustlines_asset ON trustlines_current(asset_code, asset_issuer);
CREATE INDEX IF NOT EXISTS idx_trustlines_last_modified ON trustlines_current(last_modified_ledger DESC);

-- Table: offers_current
-- Current state of all offers
CREATE TABLE IF NOT EXISTS offers_current (
    offer_id BIGINT PRIMARY KEY,
    seller_id VARCHAR(56) NOT NULL,
    selling_asset_type VARCHAR(20),
    selling_asset_code VARCHAR(12),
    selling_asset_issuer VARCHAR(56),
    buying_asset_type VARCHAR(20),
    buying_asset_code VARCHAR(12),
    buying_asset_issuer VARCHAR(56),
    amount BIGINT,
    price_n INTEGER,
    price_d INTEGER,
    price DECIMAL(20,7),
    flags INTEGER,
    last_modified_ledger BIGINT NOT NULL,
    ledger_sequence BIGINT NOT NULL,
    created_at TIMESTAMP,
    sponsor VARCHAR(56),
    ledger_range BIGINT,

    -- Metadata
    inserted_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_offers_seller ON offers_current(seller_id);
CREATE INDEX IF NOT EXISTS idx_offers_selling_asset ON offers_current(selling_asset_code, selling_asset_issuer);
CREATE INDEX IF NOT EXISTS idx_offers_buying_asset ON offers_current(buying_asset_code, buying_asset_issuer);
CREATE INDEX IF NOT EXISTS idx_offers_last_modified ON offers_current(last_modified_ledger DESC);

-- Table: contract_data_current
-- Current state of Soroban contract data entries
CREATE TABLE IF NOT EXISTS contract_data_current (
    contract_id VARCHAR(100) NOT NULL,
    contract_key_type VARCHAR(50) NOT NULL,
    contract_durability VARCHAR(20),
    asset VARCHAR(100),
    asset_code VARCHAR(12),
    asset_issuer VARCHAR(56),
    asset_type VARCHAR(20),
    balance_holder VARCHAR(100),
    contract_data_xdr TEXT,
    last_modified_ledger BIGINT NOT NULL,
    ledger_sequence BIGINT NOT NULL,
    created_at TIMESTAMP,
    ledger_range BIGINT,

    -- Metadata
    inserted_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Unique index for contract data (handles NULLs properly)
CREATE UNIQUE INDEX IF NOT EXISTS idx_contract_data_unique
ON contract_data_current(
    contract_id,
    contract_key_type,
    COALESCE(contract_durability, '')
);

CREATE INDEX IF NOT EXISTS idx_contract_data_contract ON contract_data_current(contract_id);
CREATE INDEX IF NOT EXISTS idx_contract_data_last_modified ON contract_data_current(last_modified_ledger DESC);

-- Table: claimable_balances_current
-- Current state of claimable balances
CREATE TABLE IF NOT EXISTS claimable_balances_current (
    balance_id VARCHAR(100) PRIMARY KEY,
    claimants TEXT,
    asset_type VARCHAR(20),
    asset_code VARCHAR(12),
    asset_issuer VARCHAR(56),
    asset VARCHAR(100),
    amount BIGINT,
    sponsor VARCHAR(56),
    flags INTEGER,
    last_modified_ledger BIGINT NOT NULL,
    ledger_sequence BIGINT NOT NULL,
    created_at TIMESTAMP,
    ledger_range BIGINT,

    -- Metadata
    inserted_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_claimable_sponsor ON claimable_balances_current(sponsor);
CREATE INDEX IF NOT EXISTS idx_claimable_asset ON claimable_balances_current(asset_code, asset_issuer);
CREATE INDEX IF NOT EXISTS idx_claimable_last_modified ON claimable_balances_current(last_modified_ledger DESC);

-- ============================================================================
-- CHECKPOINT TABLE
-- ============================================================================

-- Table: realtime_transformer_checkpoint
-- Tracks last processed ledger for real-time transformation pipeline
CREATE TABLE IF NOT EXISTS realtime_transformer_checkpoint (
    id INTEGER PRIMARY KEY DEFAULT 1,
    last_ledger_sequence BIGINT NOT NULL,
    last_processed_at TIMESTAMP NOT NULL,
    transformer_version VARCHAR(50),

    -- Ensure only one checkpoint row
    CONSTRAINT single_checkpoint CHECK (id = 1)
);

-- Insert initial checkpoint
INSERT INTO realtime_transformer_checkpoint (id, last_ledger_sequence, last_processed_at, transformer_version)
VALUES (1, 0, NOW(), 'v1.0.0')
ON CONFLICT (id) DO NOTHING;

-- ============================================================================
-- GRANTS AND PERMISSIONS
-- ============================================================================

-- Grant permissions to stellar user (already owner, but explicit for clarity)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO stellar;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO stellar;
