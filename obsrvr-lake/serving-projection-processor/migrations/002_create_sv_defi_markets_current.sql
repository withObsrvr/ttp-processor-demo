-- Current DeFi market registry / serving table
-- Target DB: silver_hot (serving schema)

CREATE SCHEMA IF NOT EXISTS serving;

CREATE TABLE IF NOT EXISTS serving.sv_defi_markets_current (
    market_id                      TEXT PRIMARY KEY,
    protocol_id                    TEXT NOT NULL REFERENCES serving.sv_defi_protocols(protocol_id) ON DELETE CASCADE,
    market_type                    TEXT NOT NULL,                -- lending_pool, lp_pool, vault, backstop, stable_pool
    market_address                 TEXT,
    pool_address                   TEXT,
    router_address                 TEXT,
    oracle_contract_id             TEXT,

    input_asset_1_type             TEXT,
    input_asset_1_code             TEXT,
    input_asset_1_issuer           TEXT,
    input_asset_1_contract_id      TEXT,
    input_asset_1_symbol           TEXT,
    input_asset_1_decimals         INTEGER,

    input_asset_2_type             TEXT,
    input_asset_2_code             TEXT,
    input_asset_2_issuer           TEXT,
    input_asset_2_contract_id      TEXT,
    input_asset_2_symbol           TEXT,
    input_asset_2_decimals         INTEGER,

    share_asset_contract_id        TEXT,
    debt_asset_contract_id         TEXT,
    collateral_asset_contract_id   TEXT,

    oracle_source                  TEXT,
    is_active                      BOOLEAN NOT NULL DEFAULT TRUE,
    metadata_json                  JSONB NOT NULL DEFAULT '{}'::jsonb,

    tvl_value_usd                  NUMERIC(38,10),
    total_deposit_value_usd        NUMERIC(38,10),
    total_borrowed_value_usd       NUMERIC(38,10),
    total_rewards_value_usd        NUMERIC(38,10),
    apr_deposit                    NUMERIC(20,10),
    apr_borrow                     NUMERIC(20,10),
    apr_rewards                    NUMERIC(20,10),

    as_of_ledger                   BIGINT NOT NULL,
    as_of_time                     TIMESTAMPTZ NOT NULL,
    updated_at                     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sv_defi_markets_protocol
    ON serving.sv_defi_markets_current (protocol_id, market_type, is_active);
CREATE INDEX IF NOT EXISTS idx_sv_defi_markets_market_address
    ON serving.sv_defi_markets_current (market_address) WHERE market_address IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_sv_defi_markets_pool_address
    ON serving.sv_defi_markets_current (pool_address) WHERE pool_address IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_sv_defi_markets_tvl
    ON serving.sv_defi_markets_current (tvl_value_usd DESC);
