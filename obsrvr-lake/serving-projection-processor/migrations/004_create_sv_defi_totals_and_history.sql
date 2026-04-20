-- User-level aggregates and history snapshots for DeFi positions
-- Target DB: silver_hot (serving schema)

CREATE SCHEMA IF NOT EXISTS serving;

CREATE TABLE IF NOT EXISTS serving.sv_defi_user_totals_current (
    owner_address                  TEXT NOT NULL,
    quote_currency                 TEXT NOT NULL DEFAULT 'USD',
    total_value                    NUMERIC(38,10) NOT NULL DEFAULT 0,
    total_deposit_value            NUMERIC(38,10) NOT NULL DEFAULT 0,
    total_borrowed_value           NUMERIC(38,10) NOT NULL DEFAULT 0,
    net_value                      NUMERIC(38,10) NOT NULL DEFAULT 0,
    total_claimable_rewards_value  NUMERIC(38,10) NOT NULL DEFAULT 0,
    open_position_count            INTEGER NOT NULL DEFAULT 0,
    protocol_count                 INTEGER NOT NULL DEFAULT 0,
    lowest_health_factor           NUMERIC(20,10),
    positions_at_risk              INTEGER NOT NULL DEFAULT 0,
    by_protocol_json               JSONB NOT NULL DEFAULT '[]'::jsonb,
    source_json                    JSONB NOT NULL DEFAULT '{}'::jsonb,
    as_of_ledger                   BIGINT NOT NULL,
    as_of_time                     TIMESTAMPTZ NOT NULL,
    updated_at                     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (owner_address, quote_currency)
);

CREATE INDEX IF NOT EXISTS idx_sv_defi_user_totals_current_value
    ON serving.sv_defi_user_totals_current (total_value DESC);
CREATE INDEX IF NOT EXISTS idx_sv_defi_user_totals_health
    ON serving.sv_defi_user_totals_current (lowest_health_factor);

CREATE TABLE IF NOT EXISTS serving.sv_defi_user_totals_history (
    owner_address                  TEXT NOT NULL,
    bucket_start                   TIMESTAMPTZ NOT NULL,
    interval                       TEXT NOT NULL,                -- 1h, 1d
    quote_currency                 TEXT NOT NULL DEFAULT 'USD',
    total_value                    NUMERIC(38,10) NOT NULL DEFAULT 0,
    total_deposit_value            NUMERIC(38,10) NOT NULL DEFAULT 0,
    total_borrowed_value           NUMERIC(38,10) NOT NULL DEFAULT 0,
    net_value                      NUMERIC(38,10) NOT NULL DEFAULT 0,
    total_claimable_rewards_value  NUMERIC(38,10) NOT NULL DEFAULT 0,
    open_position_count            INTEGER NOT NULL DEFAULT 0,
    as_of_ledger                   BIGINT,
    as_of_time                     TIMESTAMPTZ,
    PRIMARY KEY (owner_address, interval, quote_currency, bucket_start)
);

CREATE INDEX IF NOT EXISTS idx_sv_defi_user_totals_history_lookup
    ON serving.sv_defi_user_totals_history (owner_address, interval, bucket_start DESC);

CREATE TABLE IF NOT EXISTS serving.sv_defi_position_history (
    position_id                    TEXT NOT NULL,
    bucket_start                   TIMESTAMPTZ NOT NULL,
    interval                       TEXT NOT NULL,
    quote_currency                 TEXT NOT NULL DEFAULT 'USD',
    protocol_id                    TEXT NOT NULL,
    owner_address                  TEXT NOT NULL,
    status                         TEXT,
    deposit_value                  NUMERIC(38,10),
    borrowed_value                 NUMERIC(38,10),
    current_value                  NUMERIC(38,10),
    net_value                      NUMERIC(38,10),
    current_return_value           NUMERIC(38,10),
    health_factor                  NUMERIC(20,10),
    risk_status                    TEXT,
    as_of_ledger                   BIGINT,
    as_of_time                     TIMESTAMPTZ,
    PRIMARY KEY (position_id, interval, quote_currency, bucket_start)
);

CREATE INDEX IF NOT EXISTS idx_sv_defi_position_history_owner
    ON serving.sv_defi_position_history (owner_address, interval, bucket_start DESC);
CREATE INDEX IF NOT EXISTS idx_sv_defi_position_history_protocol
    ON serving.sv_defi_position_history (protocol_id, interval, bucket_start DESC);
