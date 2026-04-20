-- Price inputs and protocol operational status for DeFi serving/API
-- Target DB: silver_hot (serving schema)

CREATE SCHEMA IF NOT EXISTS serving;

CREATE TABLE IF NOT EXISTS serving.sv_defi_prices_current (
    asset_key                      TEXT NOT NULL,
    quote_currency                 TEXT NOT NULL DEFAULT 'USD',
    asset_type                     TEXT,
    asset_code                     TEXT,
    asset_issuer                   TEXT,
    asset_contract_id              TEXT,
    symbol                         TEXT,
    price                          NUMERIC(38,18) NOT NULL,
    price_source                   TEXT NOT NULL,                -- oracle, dex_twap, fixed, derived, manual
    confidence                     NUMERIC(20,10),
    source_timestamp               TIMESTAMPTZ,
    source_ledger                  BIGINT,
    stale_after_seconds            INTEGER,
    status                         TEXT NOT NULL DEFAULT 'ok',   -- ok, stale, missing, degraded
    metadata_json                  JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at                     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (asset_key, quote_currency)
);

CREATE INDEX IF NOT EXISTS idx_sv_defi_prices_status
    ON serving.sv_defi_prices_current (status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_sv_defi_prices_contract
    ON serving.sv_defi_prices_current (asset_contract_id) WHERE asset_contract_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS serving.sv_defi_protocol_status (
    protocol_id                    TEXT PRIMARY KEY REFERENCES serving.sv_defi_protocols(protocol_id) ON DELETE CASCADE,
    status                         TEXT NOT NULL,                -- ok, degraded, halted, backfill
    reason                         TEXT,
    last_successful_ledger         BIGINT,
    last_successful_time           TIMESTAMPTZ,
    freshness_seconds              INTEGER,
    source_divergence              BOOLEAN NOT NULL DEFAULT FALSE,
    source_json                    JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at                     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sv_defi_protocol_status_status
    ON serving.sv_defi_protocol_status (status, updated_at DESC);
