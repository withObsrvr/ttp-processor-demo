-- Migration 001: Create core tables (ledgers, transactions, operations)
-- Full Hubble-compatible schemas matching DuckLake Bronze layer exactly
-- Source: ducklake-ingestion-obsrvr-v3/go/tables.go

-- ============================================================================
-- LEDGERS_ROW_V2 (25 fields)
-- ============================================================================
CREATE UNLOGGED TABLE IF NOT EXISTS ledgers_row_v2 (
    sequence BIGINT NOT NULL PRIMARY KEY,
    ledger_hash TEXT NOT NULL,
    previous_ledger_hash TEXT NOT NULL,
    transaction_count INTEGER,
    operation_count INTEGER,
    successful_tx_count INTEGER,
    failed_tx_count INTEGER,
    tx_set_operation_count INTEGER,
    closed_at TIMESTAMPTZ NOT NULL,
    total_coins BIGINT NOT NULL,
    fee_pool BIGINT NOT NULL,
    base_fee INTEGER NOT NULL,
    base_reserve INTEGER NOT NULL,
    max_tx_set_size INTEGER NOT NULL,
    protocol_version INTEGER NOT NULL,
    ledger_header TEXT,
    soroban_fee_write_1kb BIGINT,
    soroban_tx_max_read_ledger_entries INTEGER,
    soroban_tx_max_read_bytes INTEGER,
    soroban_tx_max_write_ledger_entries INTEGER,
    soroban_tx_max_write_bytes INTEGER,
    soroban_tx_max_size_bytes INTEGER,
    created_at TIMESTAMPTZ,
    ledger_range BIGINT NOT NULL,
    era_id TEXT,
    version_label TEXT
);

COMMENT ON TABLE ledgers_row_v2 IS 'Core ledger headers - Hubble-compatible schema';
COMMENT ON COLUMN ledgers_row_v2.ledger_range IS 'Logical partition: (sequence / 10000) * 10000';

-- ============================================================================
-- TRANSACTIONS_ROW_V2 (18 fields)
-- ============================================================================
CREATE UNLOGGED TABLE IF NOT EXISTS transactions_row_v2 (
    transaction_hash TEXT NOT NULL PRIMARY KEY,
    ledger_sequence BIGINT NOT NULL,
    application_order INTEGER NOT NULL,
    account TEXT NOT NULL,
    account_muxed TEXT,
    account_sequence BIGINT NOT NULL,
    max_fee BIGINT NOT NULL,
    operation_count INTEGER NOT NULL,
    tx_envelope TEXT,
    tx_result TEXT,
    tx_meta TEXT,
    tx_fee_meta TEXT,
    successful BOOLEAN NOT NULL,
    closed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ,
    ledger_range BIGINT NOT NULL,
    era_id TEXT,
    version_label TEXT
);

COMMENT ON TABLE transactions_row_v2 IS 'Transaction details with XDR blobs - Hubble-compatible schema';

-- ============================================================================
-- OPERATIONS_ROW_V2 (16 fields)
-- ============================================================================
CREATE UNLOGGED TABLE IF NOT EXISTS operations_row_v2 (
    operation_id TEXT NOT NULL PRIMARY KEY,
    transaction_hash TEXT NOT NULL,
    ledger_sequence BIGINT NOT NULL,
    application_order INTEGER NOT NULL,
    type INTEGER NOT NULL,
    type_string TEXT NOT NULL,
    details TEXT,
    source_account TEXT,
    source_account_muxed TEXT,
    successful BOOLEAN NOT NULL,
    transaction_successful BOOLEAN NOT NULL,
    closed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ,
    ledger_range BIGINT NOT NULL,
    era_id TEXT,
    version_label TEXT
);

COMMENT ON TABLE operations_row_v2 IS 'All operation types with JSON details - Hubble-compatible schema';

-- ============================================================================
-- INDEXES (ledger_range + sequence for flush operations)
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_ledgers_range ON ledgers_row_v2(ledger_range);
CREATE INDEX IF NOT EXISTS idx_ledgers_sequence ON ledgers_row_v2(sequence);

CREATE INDEX IF NOT EXISTS idx_transactions_range ON transactions_row_v2(ledger_range);
CREATE INDEX IF NOT EXISTS idx_transactions_sequence ON transactions_row_v2(ledger_sequence);

CREATE INDEX IF NOT EXISTS idx_operations_range ON operations_row_v2(ledger_range);
CREATE INDEX IF NOT EXISTS idx_operations_sequence ON operations_row_v2(ledger_sequence);
