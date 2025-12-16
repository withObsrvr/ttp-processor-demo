-- Migration 004: Create Soroban (smart contract) tables
-- Tables: contract_events_stream_v1, contract_data_snapshot_v1, contract_code_snapshot_v1,
--         config_settings_snapshot_v1, ttl_snapshot_v1, evicted_keys_state_v1,
--         restored_keys_state_v1
-- Soroban: Smart contracts on Stellar (Protocol 20+)

-- ============================================================================
-- CONTRACT_EVENTS_STREAM_V1 (16 fields)
-- ============================================================================
CREATE UNLOGGED TABLE contract_events_stream_v1 (
    -- Identity (5 fields)
    event_id TEXT NOT NULL,
    contract_id TEXT,
    ledger_sequence BIGINT NOT NULL,
    transaction_hash TEXT NOT NULL,
    closed_at TIMESTAMPTZ NOT NULL,

    -- Event type (2 fields)
    event_type TEXT NOT NULL,
    in_successful_contract_call BOOLEAN NOT NULL,

    -- Event data (5 fields) - Hubble compatible with decoded versions
    topics_json TEXT NOT NULL,
    topics_decoded TEXT NOT NULL,
    data_xdr TEXT NOT NULL,
    data_decoded TEXT NOT NULL,
    topic_count INTEGER NOT NULL,

    -- Context (2 fields)
    operation_index INTEGER NOT NULL,
    event_index INTEGER NOT NULL,

    -- Metadata (2 fields)
    created_at TIMESTAMPTZ NOT NULL,
    ledger_range BIGINT NOT NULL,

    -- Incremental versioning support (2 fields)
    era_id TEXT,
    version_label TEXT,

    PRIMARY KEY (event_id)
);

COMMENT ON TABLE contract_events_stream_v1 IS 'Smart contract events - both XDR and decoded formats for Hubble compatibility';

-- ============================================================================
-- CONTRACT_DATA_SNAPSHOT_V1 (17 fields)
-- ============================================================================
CREATE UNLOGGED TABLE contract_data_snapshot_v1 (
    -- Identity (3 fields)
    contract_id TEXT NOT NULL,
    ledger_sequence BIGINT NOT NULL,
    ledger_key_hash TEXT NOT NULL,

    -- Contract metadata (2 fields)
    contract_key_type TEXT NOT NULL,
    contract_durability TEXT NOT NULL,

    -- Asset information (3 fields, nullable) - SAC detection
    asset_code TEXT,
    asset_issuer TEXT,
    asset_type TEXT,

    -- Balance information (2 fields, nullable) - SAC balance detection
    balance_holder TEXT,
    balance TEXT,

    -- Ledger metadata (4 fields)
    last_modified_ledger INTEGER NOT NULL,
    ledger_entry_change INTEGER NOT NULL,
    deleted BOOLEAN NOT NULL,
    closed_at TIMESTAMPTZ NOT NULL,

    -- XDR data (1 field)
    contract_data_xdr TEXT NOT NULL,

    -- Metadata (2 fields)
    created_at TIMESTAMPTZ NOT NULL,
    ledger_range BIGINT NOT NULL,

    -- Incremental versioning support (2 fields)
    era_id TEXT,
    version_label TEXT,

    PRIMARY KEY (contract_id, ledger_key_hash, ledger_sequence)
) WITH (FILLFACTOR=90);

COMMENT ON TABLE contract_data_snapshot_v1 IS 'Contract storage entries - includes SAC (Stellar Asset Contract) detection';

-- ============================================================================
-- CONTRACT_CODE_SNAPSHOT_V1 (18 fields)
-- ============================================================================
CREATE UNLOGGED TABLE contract_code_snapshot_v1 (
    -- Identity (2 fields)
    contract_code_hash TEXT NOT NULL,
    ledger_key_hash TEXT NOT NULL,

    -- Extension (1 field)
    contract_code_ext_v INTEGER NOT NULL,

    -- Ledger metadata (4 fields)
    last_modified_ledger INTEGER NOT NULL,
    ledger_entry_change INTEGER NOT NULL,
    deleted BOOLEAN NOT NULL,
    closed_at TIMESTAMPTZ NOT NULL,

    -- Ledger tracking (1 field)
    ledger_sequence BIGINT NOT NULL,

    -- WASM metadata (10 fields, nullable) - WebAssembly bytecode analysis
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

    -- Metadata (2 fields)
    created_at TIMESTAMPTZ NOT NULL,
    ledger_range BIGINT NOT NULL,

    -- Incremental versioning support (2 fields)
    era_id TEXT,
    version_label TEXT,

    PRIMARY KEY (contract_code_hash, ledger_sequence)
);

COMMENT ON TABLE contract_code_snapshot_v1 IS 'WASM bytecode with metrics - instruction count, function count, etc.';

-- ============================================================================
-- CONFIG_SETTINGS_SNAPSHOT_V1 (19 fields)
-- ============================================================================
CREATE UNLOGGED TABLE config_settings_snapshot_v1 (
    -- Identity (2 fields)
    config_setting_id INTEGER NOT NULL,
    ledger_sequence BIGINT NOT NULL,

    -- Ledger metadata (3 fields)
    last_modified_ledger INTEGER NOT NULL,
    deleted BOOLEAN NOT NULL,
    closed_at TIMESTAMPTZ NOT NULL,

    -- Soroban compute settings (4 fields, nullable)
    ledger_max_instructions BIGINT,
    tx_max_instructions BIGINT,
    fee_rate_per_instructions_increment BIGINT,
    tx_memory_limit BIGINT,  -- UINTEGER → BIGINT

    -- Soroban ledger cost settings (8 fields, nullable)
    ledger_max_read_ledger_entries BIGINT,  -- UINTEGER → BIGINT
    ledger_max_read_bytes BIGINT,  -- UINTEGER → BIGINT
    ledger_max_write_ledger_entries BIGINT,  -- UINTEGER → BIGINT
    ledger_max_write_bytes BIGINT,  -- UINTEGER → BIGINT
    tx_max_read_ledger_entries BIGINT,  -- UINTEGER → BIGINT
    tx_max_read_bytes BIGINT,  -- UINTEGER → BIGINT
    tx_max_write_ledger_entries BIGINT,  -- UINTEGER → BIGINT
    tx_max_write_bytes BIGINT,  -- UINTEGER → BIGINT

    -- Contract size limit (1 field, nullable)
    contract_max_size_bytes BIGINT,  -- UINTEGER → BIGINT

    -- Raw XDR (1 field)
    config_setting_xdr TEXT NOT NULL,

    -- Metadata (2 fields)
    created_at TIMESTAMPTZ NOT NULL,
    ledger_range BIGINT NOT NULL,

    -- Incremental versioning support (2 fields)
    era_id TEXT,
    version_label TEXT,

    PRIMARY KEY (config_setting_id, ledger_sequence)
);

COMMENT ON TABLE config_settings_snapshot_v1 IS 'Network config - Soroban fees, ledger limits, contract size limits';

-- ============================================================================
-- TTL_SNAPSHOT_V1 (11 fields)
-- ============================================================================
CREATE UNLOGGED TABLE ttl_snapshot_v1 (
    -- Identity (2 fields)
    key_hash TEXT NOT NULL,
    ledger_sequence BIGINT NOT NULL,

    -- TTL tracking (3 fields)
    live_until_ledger_seq BIGINT NOT NULL,
    ttl_remaining BIGINT NOT NULL,
    expired BOOLEAN NOT NULL,

    -- Ledger metadata (3 fields)
    last_modified_ledger INTEGER NOT NULL,
    deleted BOOLEAN NOT NULL,
    closed_at TIMESTAMPTZ NOT NULL,

    -- Metadata (2 fields)
    created_at TIMESTAMPTZ NOT NULL,
    ledger_range BIGINT NOT NULL,

    -- Incremental versioning support (2 fields)
    era_id TEXT,
    version_label TEXT,

    PRIMARY KEY (key_hash, ledger_sequence)
) WITH (FILLFACTOR=90);

COMMENT ON TABLE ttl_snapshot_v1 IS 'Time-to-live entries for contract state - tracks expiration';

-- ============================================================================
-- EVICTED_KEYS_STATE_V1 (9 fields)
-- ============================================================================
CREATE UNLOGGED TABLE evicted_keys_state_v1 (
    -- Identity (2 fields)
    key_hash TEXT NOT NULL,
    ledger_sequence BIGINT NOT NULL,

    -- Eviction details (3 fields)
    contract_id TEXT NOT NULL,
    key_type TEXT NOT NULL,
    durability TEXT NOT NULL,

    -- Metadata (3 fields)
    closed_at TIMESTAMPTZ NOT NULL,
    ledger_range BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,

    -- Incremental versioning support (2 fields)
    era_id TEXT,
    version_label TEXT,

    PRIMARY KEY (key_hash, ledger_sequence)
);

COMMENT ON TABLE evicted_keys_state_v1 IS 'Keys evicted from hot storage to archive (Protocol 23 - CAP-62)';

-- ============================================================================
-- RESTORED_KEYS_STATE_V1 (10 fields)
-- ============================================================================
CREATE UNLOGGED TABLE restored_keys_state_v1 (
    -- Identity (2 fields)
    key_hash TEXT NOT NULL,
    ledger_sequence BIGINT NOT NULL,

    -- Restoration details (4 fields)
    contract_id TEXT NOT NULL,
    key_type TEXT NOT NULL,
    durability TEXT NOT NULL,
    restored_from_ledger BIGINT NOT NULL,

    -- Metadata (3 fields)
    closed_at TIMESTAMPTZ NOT NULL,
    ledger_range BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,

    -- Incremental versioning support (2 fields)
    era_id TEXT,
    version_label TEXT,

    PRIMARY KEY (key_hash, ledger_sequence)
);

COMMENT ON TABLE restored_keys_state_v1 IS 'Keys restored from archive to hot storage (Protocol 23 - CAP-62)';
