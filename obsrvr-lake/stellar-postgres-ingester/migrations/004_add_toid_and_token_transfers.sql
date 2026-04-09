-- Migration 004: Add TOID columns and token_transfers_stream_v1 table
-- Applied during full reset (Step 2 runs TRUNCATE, tables survive, then run this)

-- Add TOID columns to transactions
ALTER TABLE transactions_row_v2 ADD COLUMN IF NOT EXISTS transaction_id BIGINT;
CREATE INDEX IF NOT EXISTS idx_transactions_row_v2_transaction_id ON transactions_row_v2 (transaction_id);

-- Add TOID columns to operations
ALTER TABLE operations_row_v2 ADD COLUMN IF NOT EXISTS transaction_id BIGINT;
ALTER TABLE operations_row_v2 ADD COLUMN IF NOT EXISTS operation_id BIGINT;
CREATE INDEX IF NOT EXISTS idx_operations_row_v2_operation_id ON operations_row_v2 (operation_id);
CREATE INDEX IF NOT EXISTS idx_operations_row_v2_transaction_id ON operations_row_v2 (transaction_id);

-- Create token_transfers_stream_v1 table
CREATE TABLE IF NOT EXISTS token_transfers_stream_v1 (
    ledger_sequence   INTEGER NOT NULL,
    transaction_hash  TEXT NOT NULL,
    transaction_id    BIGINT NOT NULL,
    operation_id      BIGINT,
    operation_index   INTEGER,
    event_type        TEXT NOT NULL,
    "from"            TEXT,
    "to"              TEXT,
    asset             TEXT NOT NULL,
    asset_type        TEXT NOT NULL,
    asset_code        TEXT,
    asset_issuer      TEXT,
    amount            DOUBLE PRECISION NOT NULL,
    amount_raw        TEXT NOT NULL,
    contract_id       TEXT NOT NULL,
    closed_at         TIMESTAMPTZ NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ledger_range      INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_token_transfers_ledger_seq ON token_transfers_stream_v1 (ledger_sequence);
CREATE INDEX IF NOT EXISTS idx_token_transfers_tx_hash ON token_transfers_stream_v1 (transaction_hash);
CREATE INDEX IF NOT EXISTS idx_token_transfers_event_type ON token_transfers_stream_v1 (event_type);
CREATE INDEX IF NOT EXISTS idx_token_transfers_asset ON token_transfers_stream_v1 (asset);
CREATE INDEX IF NOT EXISTS idx_token_transfers_from ON token_transfers_stream_v1 ("from");
CREATE INDEX IF NOT EXISTS idx_token_transfers_to ON token_transfers_stream_v1 ("to");
CREATE INDEX IF NOT EXISTS idx_token_transfers_contract_id ON token_transfers_stream_v1 (contract_id);
