-- DeFi protocol registry and protocol contract role mapping
-- Target DB: silver_hot (serving schema)

CREATE SCHEMA IF NOT EXISTS serving;

CREATE TABLE IF NOT EXISTS serving.sv_defi_protocols (
    protocol_id                    TEXT PRIMARY KEY,
    network                        TEXT NOT NULL,                 -- testnet, mainnet
    display_name                   TEXT NOT NULL,
    slug                           TEXT NOT NULL,
    version                        TEXT,
    category                       TEXT NOT NULL,                 -- lending, amm, stable, yield, derivatives
    status                         TEXT NOT NULL DEFAULT 'active',-- active, degraded, paused, retired
    adapter_name                   TEXT NOT NULL,                 -- blend_v1, aquarius_v1, etc.
    pricing_model                  TEXT NOT NULL,                 -- oracle_spot, dex_twap, reserve_share, custom
    health_model                   TEXT,                          -- null if protocol has no health factor
    supports_history               BOOLEAN NOT NULL DEFAULT TRUE,
    supports_rewards               BOOLEAN NOT NULL DEFAULT FALSE,
    supports_health_factor         BOOLEAN NOT NULL DEFAULT FALSE,
    website_url                    TEXT,
    docs_url                       TEXT,
    icon_url                       TEXT,
    config_json                    JSONB NOT NULL DEFAULT '{}'::jsonb,
    source                         TEXT NOT NULL DEFAULT 'manual',-- manual, discovered, imported
    verified                       BOOLEAN NOT NULL DEFAULT FALSE,
    last_updated_ledger            BIGINT,
    last_updated_at                TIMESTAMPTZ,
    created_at                     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT sv_defi_protocols_status_chk CHECK (status IN ('active', 'degraded', 'paused', 'retired'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_sv_defi_protocols_network_slug
    ON serving.sv_defi_protocols (network, slug);
CREATE INDEX IF NOT EXISTS idx_sv_defi_protocols_category
    ON serving.sv_defi_protocols (category);
CREATE INDEX IF NOT EXISTS idx_sv_defi_protocols_status
    ON serving.sv_defi_protocols (status);

CREATE TABLE IF NOT EXISTS serving.sv_defi_protocol_contracts (
    protocol_id                    TEXT NOT NULL REFERENCES serving.sv_defi_protocols(protocol_id) ON DELETE CASCADE,
    contract_id                    TEXT NOT NULL,
    role                           TEXT NOT NULL,                 -- router, pool, market, oracle, backstop, factory, vault, emissions
    market_id                      TEXT,
    is_active                      BOOLEAN NOT NULL DEFAULT TRUE,
    verified                       BOOLEAN NOT NULL DEFAULT FALSE,
    source                         TEXT NOT NULL DEFAULT 'manual',
    metadata_json                  JSONB NOT NULL DEFAULT '{}'::jsonb,
    first_seen_ledger              BIGINT,
    last_seen_ledger               BIGINT,
    created_at                     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (protocol_id, contract_id, role)
);

CREATE INDEX IF NOT EXISTS idx_sv_defi_protocol_contracts_contract
    ON serving.sv_defi_protocol_contracts (contract_id);
CREATE INDEX IF NOT EXISTS idx_sv_defi_protocol_contracts_role
    ON serving.sv_defi_protocol_contracts (protocol_id, role, is_active);
CREATE INDEX IF NOT EXISTS idx_sv_defi_protocol_contracts_market
    ON serving.sv_defi_protocol_contracts (market_id) WHERE market_id IS NOT NULL;
