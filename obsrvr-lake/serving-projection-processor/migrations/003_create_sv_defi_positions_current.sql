-- Current DeFi positions and component breakdowns
-- Target DB: silver_hot (serving schema)

CREATE SCHEMA IF NOT EXISTS serving;

CREATE TABLE IF NOT EXISTS serving.sv_defi_positions_current (
    position_id                    TEXT PRIMARY KEY,
    protocol_id                    TEXT NOT NULL REFERENCES serving.sv_defi_protocols(protocol_id) ON DELETE CASCADE,
    protocol_version               TEXT,
    position_type                  TEXT NOT NULL,                -- lending_supply, lending_borrow, lp_position, backstop_position, vault_deposit
    status                         TEXT NOT NULL,                -- open, closed, liquidated, archived
    owner_address                  TEXT NOT NULL,                -- user identity supplied by client (G... or C...)
    account_address                TEXT,                         -- actual holding address if different
    related_address                TEXT,
    market_id                      TEXT REFERENCES serving.sv_defi_markets_current(market_id) ON DELETE SET NULL,
    market_address                 TEXT,
    position_key_hash              TEXT,

    underlying_asset_type          TEXT,
    underlying_asset_code          TEXT,
    underlying_asset_issuer        TEXT,
    underlying_asset_contract_id   TEXT,
    underlying_symbol              TEXT,
    underlying_decimals            INTEGER,

    quote_currency                 TEXT NOT NULL DEFAULT 'USD',
    deposit_amount                 NUMERIC(38,18),
    borrow_amount                  NUMERIC(38,18),
    share_amount                   NUMERIC(38,18),
    claimable_reward_amount        NUMERIC(38,18),

    deposit_value                  NUMERIC(38,10),
    borrowed_value                 NUMERIC(38,10),
    current_value                  NUMERIC(38,10),
    net_value                      NUMERIC(38,10),
    current_return_value           NUMERIC(38,10),
    current_return_percent         NUMERIC(20,10),
    claimable_rewards_value        NUMERIC(38,10),

    health_factor                  NUMERIC(20,10),
    ltv                            NUMERIC(20,10),
    collateral_ratio               NUMERIC(20,10),
    liquidation_threshold          NUMERIC(20,10),
    risk_status                    TEXT,

    opened_at                      TIMESTAMPTZ,
    opened_ledger                  BIGINT,
    closed_at                      TIMESTAMPTZ,
    closed_ledger                  BIGINT,

    protocol_state_json            JSONB NOT NULL DEFAULT '{}'::jsonb,
    valuation_json                 JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_json                    JSONB NOT NULL DEFAULT '{}'::jsonb,

    as_of_ledger                   BIGINT NOT NULL,
    as_of_time                     TIMESTAMPTZ NOT NULL,
    last_updated_ledger            BIGINT NOT NULL,
    last_updated_at                TIMESTAMPTZ NOT NULL,
    updated_at                     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sv_defi_positions_owner
    ON serving.sv_defi_positions_current (owner_address, status, as_of_time DESC);
CREATE INDEX IF NOT EXISTS idx_sv_defi_positions_owner_protocol
    ON serving.sv_defi_positions_current (owner_address, protocol_id, status);
CREATE INDEX IF NOT EXISTS idx_sv_defi_positions_market
    ON serving.sv_defi_positions_current (market_id) WHERE market_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_sv_defi_positions_risk
    ON serving.sv_defi_positions_current (risk_status, health_factor);
CREATE INDEX IF NOT EXISTS idx_sv_defi_positions_current_value
    ON serving.sv_defi_positions_current (current_value DESC);

CREATE TABLE IF NOT EXISTS serving.sv_defi_position_components_current (
    component_id                   TEXT PRIMARY KEY,
    position_id                    TEXT NOT NULL REFERENCES serving.sv_defi_positions_current(position_id) ON DELETE CASCADE,
    protocol_id                    TEXT NOT NULL REFERENCES serving.sv_defi_protocols(protocol_id) ON DELETE CASCADE,
    component_type                 TEXT NOT NULL,                -- collateral, debt, supplied_asset, lp_leg, reward, fee_claim, backstop_share
    asset_type                     TEXT,
    asset_code                     TEXT,
    asset_issuer                   TEXT,
    asset_contract_id              TEXT,
    symbol                         TEXT,
    decimals                       INTEGER,
    amount                         NUMERIC(38,18),
    value                          NUMERIC(38,10),
    price                          NUMERIC(38,18),
    price_source                   TEXT,
    metadata_json                  JSONB NOT NULL DEFAULT '{}'::jsonb,
    as_of_ledger                   BIGINT NOT NULL,
    as_of_time                     TIMESTAMPTZ NOT NULL,
    updated_at                     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sv_defi_position_components_position
    ON serving.sv_defi_position_components_current (position_id, component_type);
CREATE INDEX IF NOT EXISTS idx_sv_defi_position_components_protocol
    ON serving.sv_defi_position_components_current (protocol_id, component_type);
