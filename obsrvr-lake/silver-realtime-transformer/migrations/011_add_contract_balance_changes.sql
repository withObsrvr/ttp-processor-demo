-- Migration 011: contract-held balance change stream for serving projection.

CREATE TABLE IF NOT EXISTS contract_balance_changes (
    owner_address TEXT NOT NULL,
    owner_type TEXT NOT NULL DEFAULT 'contract',
    asset_key TEXT NOT NULL,
    asset_type TEXT NOT NULL,
    token_contract_id TEXT NOT NULL,
    asset_code TEXT,
    asset_issuer TEXT,
    symbol TEXT,
    decimals INTEGER,
    balance_raw NUMERIC NOT NULL,
    balance_source TEXT NOT NULL,
    key_hash TEXT NOT NULL,
    ledger_sequence BIGINT NOT NULL,
    ledger_closed_at TIMESTAMP NOT NULL,
    deleted BOOLEAN NOT NULL DEFAULT FALSE,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (owner_address, asset_key, ledger_sequence)
);

CREATE INDEX IF NOT EXISTS idx_contract_balance_changes_ledger
    ON contract_balance_changes(ledger_sequence);

CREATE INDEX IF NOT EXISTS idx_contract_balance_changes_owner
    ON contract_balance_changes(owner_address, ledger_sequence DESC);
