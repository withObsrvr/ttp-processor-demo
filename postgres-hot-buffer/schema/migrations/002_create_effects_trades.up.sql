-- Migration 002: Create effects and trades tables
-- Full Hubble-compatible schemas matching DuckLake Bronze layer exactly
-- Source: ducklake-ingestion-obsrvr-v3/go/tables.go

-- ============================================================================
-- EFFECTS_ROW_V1 (11 fields)
-- ============================================================================
CREATE UNLOGGED TABLE IF NOT EXISTS effects_row_v1 (
    operation_id TEXT NOT NULL,
    ledger_sequence BIGINT NOT NULL,
    account TEXT NOT NULL,
    account_muxed TEXT,
    type INTEGER NOT NULL,
    type_string TEXT NOT NULL,
    details TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    ledger_range BIGINT NOT NULL,
    era_id TEXT,
    version_label TEXT,

    PRIMARY KEY (operation_id, account, type)
);

COMMENT ON TABLE effects_row_v1 IS 'State changes from operations - Hubble-compatible schema';

-- ============================================================================
-- TRADES_ROW_V1 (20 fields)
-- ============================================================================
CREATE UNLOGGED TABLE IF NOT EXISTS trades_row_v1 (
    ledger_sequence BIGINT NOT NULL,
    offer_id BIGINT,
    base_offer_id BIGINT NOT NULL,
    base_account TEXT NOT NULL,
    base_asset_type TEXT NOT NULL,
    base_asset_code TEXT,
    base_asset_issuer TEXT,
    base_amount BIGINT NOT NULL,
    counter_offer_id BIGINT NOT NULL,
    counter_account TEXT NOT NULL,
    counter_asset_type TEXT NOT NULL,
    counter_asset_code TEXT,
    counter_asset_issuer TEXT,
    counter_amount BIGINT NOT NULL,
    price_n BIGINT NOT NULL,
    price_d BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    ledger_range BIGINT NOT NULL,
    era_id TEXT,
    version_label TEXT,

    PRIMARY KEY (ledger_sequence, base_offer_id, counter_offer_id)
);

COMMENT ON TABLE trades_row_v1 IS 'DEX trade executions - Hubble-compatible schema';

-- ============================================================================
-- INDEXES
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_effects_range ON effects_row_v1(ledger_range);
CREATE INDEX IF NOT EXISTS idx_effects_sequence ON effects_row_v1(ledger_sequence);

CREATE INDEX IF NOT EXISTS idx_trades_range ON trades_row_v1(ledger_range);
CREATE INDEX IF NOT EXISTS idx_trades_sequence ON trades_row_v1(ledger_sequence);
