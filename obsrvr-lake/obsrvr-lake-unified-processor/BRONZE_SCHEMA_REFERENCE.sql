-- =============================================================================
-- Obsrvr Lake Unified Processor - Bronze Schema Reference (V3)
-- =============================================================================
--
-- This file is the canonical reference for the bronze layer schema used by
-- the unified processor. It matches the V3 production schema used by
-- postgres-ducklake-flusher and stellar-postgres-ingester exactly.
--
-- When modifying schemas:
--   1. Update this file first as the source of truth
--   2. Update schemas.go bronzeSchemaSQL() to match
--   3. Update bronze_data.go structs if new fields are added
--   4. Update bronze_extractor.go to extract new fields from XDR
--   5. Update bronze_writer.go INSERT statements to write new fields
--
-- Tables: 20
-- Naming: era_id + version_label on every table (replaces pipeline_version)
-- =============================================================================


-- =============================================================================
-- CORE TABLES
-- =============================================================================

-- 1. ledgers_row_v2 (29 columns)
-- One row per ledger close. Contains header metadata, fee info, and Soroban stats.
CREATE TABLE IF NOT EXISTS bronze.ledgers_row_v2 (
    sequence BIGINT,                    -- Ledger sequence number (primary identifier)
    ledger_hash TEXT,                   -- Hash of this ledger
    previous_ledger_hash TEXT,          -- Hash of the previous ledger
    closed_at TIMESTAMP,               -- When this ledger was closed
    protocol_version INTEGER,           -- Stellar protocol version
    total_coins BIGINT,                 -- Total lumens in existence (stroops)
    fee_pool BIGINT,                    -- Fee pool balance (stroops)
    base_fee INTEGER,                   -- Base fee per operation (stroops)
    base_reserve INTEGER,               -- Base reserve per entry (stroops)
    max_tx_set_size INTEGER,            -- Maximum transaction set size
    successful_tx_count INTEGER,        -- Count of successful transactions
    failed_tx_count INTEGER,            -- Count of failed transactions
    ingestion_timestamp TIMESTAMP,      -- When this ledger was ingested
    ledger_range BIGINT,                -- Partition key (sequence / 10000 * 10000)
    transaction_count INTEGER,          -- Total transaction count
    operation_count INTEGER,            -- Total operation count
    tx_set_operation_count INTEGER,     -- Operations in the transaction set
    soroban_fee_write1kb BIGINT,        -- Soroban fee for 1KB write
    node_id TEXT,                       -- Validator node ID that closed ledger
    signature TEXT,                     -- Validator signature
    ledger_header TEXT,                 -- Raw ledger header XDR (base64)
    bucket_list_size BIGINT,            -- Size of the bucket list
    live_soroban_state_size BIGINT,     -- Size of live Soroban state
    evicted_keys_count INTEGER,         -- Number of evicted Soroban keys
    soroban_op_count INTEGER,           -- Count of Soroban operations
    total_fee_charged BIGINT,           -- Total fees charged in ledger
    contract_events_count INTEGER,      -- Count of contract events emitted
    era_id TEXT,                        -- Processing era identifier
    version_label TEXT                  -- Schema/pipeline version label
);


-- 2. transactions_row_v2 (49 columns)
-- One row per transaction. Contains envelope, result, fee breakdown, Soroban details.
CREATE TABLE IF NOT EXISTS bronze.transactions_row_v2 (
    ledger_sequence BIGINT,             -- Parent ledger sequence
    transaction_hash TEXT,              -- Transaction hash (hex)
    source_account TEXT,                -- Source account (G... or C...)
    fee_charged BIGINT,                 -- Actual fee charged (stroops)
    max_fee BIGINT,                     -- Max fee willing to pay
    successful BOOLEAN,                 -- Whether transaction succeeded
    transaction_result_code TEXT,       -- Result code string
    operation_count INTEGER,            -- Number of operations
    memo_type TEXT,                     -- Memo type (none/text/id/hash/return)
    memo TEXT,                          -- Memo value
    created_at TIMESTAMP,              -- Ledger close time
    account_sequence BIGINT,            -- Source account sequence number
    ledger_range BIGINT,                -- Partition key
    source_account_muxed TEXT,          -- Muxed source account (M...)
    fee_account_muxed TEXT,             -- Muxed fee account for fee bumps
    inner_transaction_hash TEXT,        -- Inner tx hash (fee bump transactions)
    fee_bump_fee BIGINT,                -- Fee bump outer fee
    max_fee_bid BIGINT,                 -- Fee bump max fee bid
    inner_source_account TEXT,          -- Inner tx source (fee bump)
    timebounds_min_time BIGINT,         -- Timebound min (unix timestamp)
    timebounds_max_time BIGINT,         -- Timebound max (unix timestamp)
    ledgerbounds_min BIGINT,            -- Ledgerbound min sequence
    ledgerbounds_max BIGINT,            -- Ledgerbound max sequence
    min_sequence_number BIGINT,         -- Precondition: min sequence
    min_sequence_age BIGINT,            -- Precondition: min sequence age
    soroban_resources_instructions BIGINT, -- Soroban CPU instructions budget
    soroban_resources_read_bytes BIGINT,   -- Soroban read bytes budget
    soroban_resources_write_bytes BIGINT,  -- Soroban write bytes budget
    soroban_data_size_bytes INTEGER,    -- Size of Soroban data
    soroban_data_resources TEXT,        -- Soroban resource footprint
    soroban_fee_base BIGINT,            -- Soroban base fee component
    soroban_fee_resources BIGINT,       -- Soroban resource fee component
    soroban_fee_refund BIGINT,          -- Soroban fee refund
    soroban_fee_charged BIGINT,         -- Soroban total fee charged
    soroban_fee_wasted BIGINT,          -- Soroban fee wasted (unused budget)
    soroban_host_function_type TEXT,    -- Host function type (invoke/create/upload)
    soroban_contract_id TEXT,           -- Target contract ID
    soroban_contract_events_count INTEGER, -- Contract events emitted
    signatures_count INTEGER,           -- Number of signatures
    new_account BOOLEAN,                -- Whether tx created a new account
    rent_fee_charged BIGINT,            -- Soroban rent fee
    tx_envelope TEXT,                   -- Raw transaction envelope XDR (base64)
    tx_result TEXT,                     -- Raw transaction result XDR (base64)
    tx_meta TEXT,                       -- Raw transaction meta XDR (base64)
    tx_fee_meta TEXT,                   -- Raw fee meta XDR (base64)
    tx_signers TEXT,                    -- JSON array of signer public keys
    extra_signers TEXT,                 -- Extra signers from preconditions
    era_id TEXT,                        -- Processing era identifier
    version_label TEXT                  -- Schema/pipeline version label
);


-- 3. operations_row_v2 (64 columns)
-- One row per operation. Contains type-specific fields, Soroban invocation details.
CREATE TABLE IF NOT EXISTS bronze.operations_row_v2 (
    transaction_hash TEXT,              -- Parent transaction hash
    operation_index INTEGER,            -- Index within transaction
    ledger_sequence BIGINT,             -- Parent ledger sequence
    source_account TEXT,                -- Operation source account
    type INTEGER,                       -- Operation type code (0-26)
    type_string TEXT,                   -- Human-readable operation type
    created_at TIMESTAMP,              -- Ledger close time
    transaction_successful BOOLEAN,     -- Whether parent tx succeeded
    operation_result_code TEXT,         -- Operation result code
    operation_trace_code TEXT,          -- Detailed trace/error code
    ledger_range BIGINT,                -- Partition key
    source_account_muxed TEXT,          -- Muxed source account
    -- Asset fields (primary)
    asset TEXT,                         -- Full asset string (code:issuer or native)
    asset_type TEXT,                    -- Asset type (native/credit_alphanum4/12)
    asset_code TEXT,                    -- Asset code
    asset_issuer TEXT,                  -- Asset issuer
    -- Source asset fields (path payments)
    source_asset TEXT,
    source_asset_type TEXT,
    source_asset_code TEXT,
    source_asset_issuer TEXT,
    -- Amount fields
    amount BIGINT,                      -- Primary amount (stroops)
    source_amount BIGINT,               -- Source amount for path payments
    destination_min BIGINT,             -- Min destination amount (strict receive)
    starting_balance BIGINT,            -- Starting balance (create account)
    destination TEXT,                   -- Destination account
    -- Trust/authorization fields
    trustline_limit BIGINT,             -- Trustline limit
    trustor TEXT,                       -- Trustor account (allow trust)
    authorize BOOLEAN,                  -- Authorize flag
    authorize_to_maintain_liabilities BOOLEAN,
    trust_line_flags INTEGER,           -- Trustline flags bitmask
    -- Claimable balance
    balance_id TEXT,                    -- Claimable balance ID
    claimants_count INTEGER,            -- Number of claimants
    sponsored_id TEXT,                  -- Sponsored account ID
    -- Offer/DEX fields
    offer_id BIGINT,                    -- Offer ID
    price TEXT,                         -- Price string
    price_r TEXT,                       -- Price as rational (n/d)
    buying_asset TEXT,
    buying_asset_type TEXT,
    buying_asset_code TEXT,
    buying_asset_issuer TEXT,
    selling_asset TEXT,
    selling_asset_type TEXT,
    selling_asset_code TEXT,
    selling_asset_issuer TEXT,
    -- Soroban fields
    soroban_operation TEXT,             -- Soroban operation type
    soroban_function TEXT,              -- Invoked function name
    soroban_contract_id TEXT,           -- Target contract
    soroban_auth_required BOOLEAN,      -- Whether auth was required
    -- Account options
    bump_to BIGINT,                     -- Bump sequence target
    set_flags INTEGER,                  -- Flags to set
    clear_flags INTEGER,                -- Flags to clear
    home_domain TEXT,                   -- Home domain
    master_weight INTEGER,              -- Master key weight
    low_threshold INTEGER,              -- Low threshold
    medium_threshold INTEGER,           -- Medium threshold
    high_threshold INTEGER,             -- High threshold
    -- Data fields
    data_name TEXT,                     -- Manage data key
    data_value TEXT,                    -- Manage data value
    era_id TEXT,                        -- Processing era identifier
    version_label TEXT,                 -- Schema/pipeline version label
    -- Migration fields (contract invocation tracking)
    transaction_index INTEGER,          -- Index of tx within ledger
    soroban_arguments_json TEXT,        -- JSON of invocation arguments
    contract_calls_json TEXT,           -- JSON call graph
    contracts_involved TEXT,            -- Comma-separated contract IDs
    max_call_depth INTEGER              -- Maximum call graph depth
);


-- =============================================================================
-- EVENT TABLES
-- =============================================================================

-- 4. effects_row_v1 (22 columns)
-- One row per effect. Effects are side-effects of operations.
CREATE TABLE IF NOT EXISTS bronze.effects_row_v1 (
    ledger_sequence BIGINT,
    transaction_hash TEXT,
    operation_index INTEGER,
    effect_index INTEGER,
    effect_type INTEGER,                -- Effect type code
    effect_type_string TEXT,            -- Human-readable effect type
    account_id TEXT,                    -- Affected account
    amount TEXT,                        -- Amount (string for precision)
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
    era_id TEXT,
    version_label TEXT
);


-- 5. trades_row_v1 (19 columns)
-- One row per trade execution on the DEX.
CREATE TABLE IF NOT EXISTS bronze.trades_row_v1 (
    ledger_sequence BIGINT,
    transaction_hash TEXT,
    operation_index INTEGER,
    trade_index INTEGER,                -- Index within operation
    trade_type TEXT,                    -- orderbook / liquidity_pool
    trade_timestamp TIMESTAMP,          -- When the trade occurred
    seller_account TEXT,
    selling_asset_code TEXT,
    selling_asset_issuer TEXT,
    selling_amount TEXT,                -- String for precision
    buyer_account TEXT,
    buying_asset_code TEXT,
    buying_asset_issuer TEXT,
    buying_amount TEXT,
    price TEXT,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    era_id TEXT,
    version_label TEXT
);


-- =============================================================================
-- SNAPSHOT TABLES (state changes per ledger)
-- =============================================================================

-- 6. accounts_snapshot_v1 (25 columns)
-- Account state as of each ledger where the account was modified.
CREATE TABLE IF NOT EXISTS bronze.accounts_snapshot_v1 (
    account_id TEXT,                    -- Account public key (G...)
    ledger_sequence BIGINT,
    closed_at TIMESTAMP,
    balance TEXT,                       -- XLM balance (string for precision)
    sequence_number BIGINT,
    num_subentries INTEGER,
    num_sponsoring INTEGER,
    num_sponsored INTEGER,
    home_domain TEXT,
    master_weight INTEGER,
    low_threshold INTEGER,
    med_threshold INTEGER,
    high_threshold INTEGER,
    flags INTEGER,                      -- Account flags bitmask
    auth_required BOOLEAN,
    auth_revocable BOOLEAN,
    auth_immutable BOOLEAN,
    auth_clawback_enabled BOOLEAN,
    signers TEXT,                       -- JSON array of signers
    sponsor_account TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    ledger_range BIGINT,
    era_id TEXT,
    version_label TEXT
);


-- 7. trustlines_snapshot_v1 (16 columns)
-- Trustline state per ledger modification.
CREATE TABLE IF NOT EXISTS bronze.trustlines_snapshot_v1 (
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
    ledger_sequence BIGINT,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    era_id TEXT,
    version_label TEXT
);


-- 8. offers_snapshot_v1 (17 columns)
-- DEX offer state per ledger modification.
CREATE TABLE IF NOT EXISTS bronze.offers_snapshot_v1 (
    offer_id BIGINT,
    seller_account TEXT,
    ledger_sequence BIGINT,
    closed_at TIMESTAMP,
    selling_asset_type TEXT,
    selling_asset_code TEXT,
    selling_asset_issuer TEXT,
    buying_asset_type TEXT,
    buying_asset_code TEXT,
    buying_asset_issuer TEXT,
    amount TEXT,
    price TEXT,
    flags INTEGER,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    era_id TEXT,
    version_label TEXT
);


-- 9. account_signers_snapshot_v1 (11 columns)
-- Account signer changes per ledger.
CREATE TABLE IF NOT EXISTS bronze.account_signers_snapshot_v1 (
    account_id TEXT,
    signer TEXT,                        -- Signer public key
    ledger_sequence BIGINT,
    weight INTEGER,                     -- Signer weight (0 = removed)
    sponsor TEXT,
    deleted BOOLEAN,
    closed_at TIMESTAMP,
    ledger_range BIGINT,
    created_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);


-- 10. claimable_balances_snapshot_v1 (14 columns)
-- Claimable balance state per ledger.
CREATE TABLE IF NOT EXISTS bronze.claimable_balances_snapshot_v1 (
    balance_id TEXT,
    sponsor TEXT,
    ledger_sequence BIGINT,
    closed_at TIMESTAMP,
    asset_type TEXT,
    asset_code TEXT,
    asset_issuer TEXT,
    amount BIGINT,
    claimants_count INTEGER,
    flags INTEGER,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    era_id TEXT,
    version_label TEXT
);


-- 11. liquidity_pools_snapshot_v1 (19 columns)
-- Liquidity pool state per ledger modification.
CREATE TABLE IF NOT EXISTS bronze.liquidity_pools_snapshot_v1 (
    liquidity_pool_id TEXT,
    ledger_sequence BIGINT,
    closed_at TIMESTAMP,
    pool_type TEXT,                     -- constant_product
    fee INTEGER,                        -- Fee in basis points
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
    created_at TIMESTAMP,
    ledger_range BIGINT,
    era_id TEXT,
    version_label TEXT
);


-- =============================================================================
-- SOROBAN TABLES
-- =============================================================================

-- 12. contract_events_stream_v1 (22 columns)
-- One row per Soroban contract event emitted.
CREATE TABLE IF NOT EXISTS bronze.contract_events_stream_v1 (
    event_id TEXT,                      -- Unique event identifier
    contract_id TEXT,                   -- Emitting contract (C...)
    ledger_sequence BIGINT,
    transaction_hash TEXT,
    closed_at TIMESTAMP,
    event_type TEXT,                    -- contract/system/diagnostic
    in_successful_contract_call BOOLEAN,
    topics_json TEXT,                   -- JSON array of all topics
    topics_decoded TEXT,                -- Human-readable topic decode
    data_xdr TEXT,                      -- Event data XDR (base64)
    data_decoded TEXT,                  -- Human-readable data decode
    topic_count INTEGER,
    operation_index INTEGER,
    event_index INTEGER,
    topic0_decoded TEXT,                -- Individual decoded topics
    topic1_decoded TEXT,
    topic2_decoded TEXT,
    topic3_decoded TEXT,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    era_id TEXT,
    version_label TEXT
);


-- 13. contract_data_snapshot_v1 (22 columns)
-- Soroban contract data entry state per ledger modification.
CREATE TABLE IF NOT EXISTS bronze.contract_data_snapshot_v1 (
    contract_id TEXT,
    ledger_sequence BIGINT,
    ledger_key_hash TEXT,
    contract_key_type TEXT,             -- instance/persistent/temporary
    contract_durability TEXT,
    asset_code TEXT,                    -- SAC asset code (if applicable)
    asset_issuer TEXT,                  -- SAC asset issuer (if applicable)
    asset_type TEXT,
    balance_holder TEXT,                -- Token balance holder
    balance TEXT,                       -- Token balance
    last_modified_ledger INTEGER,
    ledger_entry_change INTEGER,        -- 0=created, 1=updated, 2=removed
    deleted BOOLEAN,
    closed_at TIMESTAMP,
    contract_data_xdr TEXT,             -- Raw contract data XDR (base64)
    created_at TIMESTAMP,
    ledger_range BIGINT,
    token_name TEXT,                    -- SEP-41 token name
    token_symbol TEXT,                  -- SEP-41 token symbol
    token_decimals INTEGER,             -- SEP-41 token decimals
    era_id TEXT,
    version_label TEXT
);


-- 14. contract_code_snapshot_v1 (22 columns)
-- Soroban contract WASM code metadata per ledger.
CREATE TABLE IF NOT EXISTS bronze.contract_code_snapshot_v1 (
    contract_code_hash TEXT,            -- WASM code hash
    ledger_key_hash TEXT,
    contract_code_ext_v INTEGER,
    last_modified_ledger INTEGER,
    ledger_entry_change INTEGER,
    deleted BOOLEAN,
    closed_at TIMESTAMP,
    ledger_sequence BIGINT,
    n_instructions BIGINT,             -- WASM instruction count
    n_functions BIGINT,
    n_globals BIGINT,
    n_table_entries BIGINT,
    n_types BIGINT,
    n_data_segments BIGINT,
    n_elem_segments BIGINT,
    n_imports BIGINT,
    n_exports BIGINT,
    n_data_segment_bytes BIGINT,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    era_id TEXT,
    version_label TEXT
);


-- =============================================================================
-- STATE TABLES
-- =============================================================================

-- 15. config_settings_snapshot_v1 (23 columns)
-- Network configuration settings with detailed Soroban resource limits.
CREATE TABLE IF NOT EXISTS bronze.config_settings_snapshot_v1 (
    config_setting_id INTEGER,          -- Setting identifier
    ledger_sequence BIGINT,
    last_modified_ledger INTEGER,
    deleted BOOLEAN,
    closed_at TIMESTAMP,
    -- Soroban resource limit fields
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
    config_setting_xdr TEXT,            -- Full setting XDR (base64)
    created_at TIMESTAMP,
    ledger_range BIGINT,
    era_id TEXT,
    version_label TEXT
);


-- 16. ttl_snapshot_v1 (12 columns)
-- TTL (time-to-live) state for Soroban ledger entries.
CREATE TABLE IF NOT EXISTS bronze.ttl_snapshot_v1 (
    key_hash TEXT,                      -- Ledger key hash
    ledger_sequence BIGINT,
    live_until_ledger_seq BIGINT,       -- Ledger when entry expires
    ttl_remaining BIGINT,               -- Ledgers until expiration
    expired BOOLEAN,
    last_modified_ledger INTEGER,
    deleted BOOLEAN,
    closed_at TIMESTAMP,
    created_at TIMESTAMP,
    ledger_range BIGINT,
    era_id TEXT,
    version_label TEXT
);


-- 17. evicted_keys_state_v1 (10 columns)
-- Soroban keys evicted due to TTL expiration.
CREATE TABLE IF NOT EXISTS bronze.evicted_keys_state_v1 (
    key_hash TEXT,
    ledger_sequence BIGINT,
    contract_id TEXT,
    key_type TEXT,
    durability TEXT,
    closed_at TIMESTAMP,
    ledger_range BIGINT,
    created_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);


-- 18. restored_keys_state_v1 (11 columns)
-- Soroban keys restored after eviction.
CREATE TABLE IF NOT EXISTS bronze.restored_keys_state_v1 (
    key_hash TEXT,
    ledger_sequence BIGINT,
    contract_id TEXT,
    key_type TEXT,
    durability TEXT,
    restored_from_ledger BIGINT,        -- Ledger the key was originally evicted
    closed_at TIMESTAMP,
    ledger_range BIGINT,
    created_at TIMESTAMP,
    era_id TEXT,
    version_label TEXT
);


-- =============================================================================
-- DERIVED TABLES
-- =============================================================================

-- 19. native_balances_snapshot_v1 (13 columns)
-- Denormalized native (XLM) balance per account per ledger.
CREATE TABLE IF NOT EXISTS bronze.native_balances_snapshot_v1 (
    account_id TEXT,
    balance BIGINT,                     -- XLM balance (stroops)
    buying_liabilities BIGINT,
    selling_liabilities BIGINT,
    num_subentries INTEGER,
    num_sponsoring INTEGER,
    num_sponsored INTEGER,
    sequence_number BIGINT,
    last_modified_ledger BIGINT,
    ledger_sequence BIGINT,
    ledger_range BIGINT,
    era_id TEXT,
    version_label TEXT
);


-- 20. contract_creations_v1 (8 columns)
-- Tracks when Soroban contracts are first created.
CREATE TABLE IF NOT EXISTS bronze.contract_creations_v1 (
    contract_id TEXT,                   -- Contract address (C...)
    creator_address TEXT,               -- Account that created the contract
    wasm_hash TEXT,                     -- WASM code hash deployed
    created_ledger BIGINT,              -- Ledger of creation
    created_at TIMESTAMP,               -- Timestamp of creation
    ledger_range BIGINT,
    era_id TEXT,
    version_label TEXT
);
