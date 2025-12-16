-- Migration 003: Create account and DEX snapshot tables
-- Tables: accounts_snapshot_v1, trustlines_snapshot_v1, native_balances_snapshot_v1,
--         account_signers_snapshot_v1, offers_snapshot_v1, claimable_balances_snapshot_v1,
--         liquidity_pools_snapshot_v1
-- FILLFACTOR=90: Leaves 10% free space for HOT updates (reduces bloat)

-- ============================================================================
-- ACCOUNTS_SNAPSHOT_V1 (23 fields)
-- ============================================================================
CREATE UNLOGGED TABLE accounts_snapshot_v1 (
    -- Identity (3 fields)
    account_id TEXT NOT NULL,
    ledger_sequence BIGINT NOT NULL,
    closed_at TIMESTAMPTZ NOT NULL,

    -- Balance (1 field)
    balance TEXT NOT NULL,

    -- Account settings (5 fields)
    sequence_number BIGINT NOT NULL,
    num_subentries INTEGER NOT NULL,
    num_sponsoring INTEGER NOT NULL,
    num_sponsored INTEGER NOT NULL,
    home_domain TEXT,

    -- Thresholds (4 fields)
    master_weight INTEGER NOT NULL,
    low_threshold INTEGER NOT NULL,
    med_threshold INTEGER NOT NULL,
    high_threshold INTEGER NOT NULL,

    -- Flags (5 fields)
    flags INTEGER NOT NULL,
    auth_required BOOLEAN NOT NULL,
    auth_revocable BOOLEAN NOT NULL,
    auth_immutable BOOLEAN NOT NULL,
    auth_clawback_enabled BOOLEAN NOT NULL,

    -- Signers (1 field) - JSON array
    signers TEXT,

    -- Sponsorship (1 field)
    sponsor_account TEXT,

    -- Metadata (3 fields)
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    ledger_range BIGINT NOT NULL,

    -- Incremental versioning support (2 fields)
    era_id TEXT,
    version_label TEXT,

    PRIMARY KEY (account_id, ledger_sequence)
) WITH (FILLFACTOR=90);

COMMENT ON TABLE accounts_snapshot_v1 IS 'Point-in-time account state - FILLFACTOR=90 for frequent updates';

-- ============================================================================
-- TRUSTLINES_SNAPSHOT_V1 (14 fields)
-- ============================================================================
CREATE UNLOGGED TABLE trustlines_snapshot_v1 (
    -- Identity (4 fields)
    account_id TEXT NOT NULL,
    asset_code TEXT NOT NULL,
    asset_issuer TEXT NOT NULL,
    asset_type TEXT NOT NULL,

    -- Trust & Balance (4 fields)
    balance TEXT NOT NULL,
    trust_limit TEXT NOT NULL,
    buying_liabilities TEXT NOT NULL,
    selling_liabilities TEXT NOT NULL,

    -- Authorization (3 fields)
    authorized BOOLEAN NOT NULL,
    authorized_to_maintain_liabilities BOOLEAN NOT NULL,
    clawback_enabled BOOLEAN NOT NULL,

    -- Metadata (3 fields)
    ledger_sequence BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    ledger_range BIGINT NOT NULL,

    -- Incremental versioning support (2 fields)
    era_id TEXT,
    version_label TEXT,

    PRIMARY KEY (account_id, asset_code, asset_issuer, ledger_sequence)
) WITH (FILLFACTOR=90);

COMMENT ON TABLE trustlines_snapshot_v1 IS 'Asset trustline state - FILLFACTOR=90 for balance updates';

-- ============================================================================
-- NATIVE_BALANCES_SNAPSHOT_V1 (11 fields)
-- ============================================================================
CREATE UNLOGGED TABLE native_balances_snapshot_v1 (
    -- Core fields (7 fields)
    account_id TEXT NOT NULL,
    balance BIGINT NOT NULL,
    buying_liabilities BIGINT NOT NULL,
    selling_liabilities BIGINT NOT NULL,
    num_subentries INTEGER NOT NULL,
    num_sponsoring INTEGER NOT NULL,
    num_sponsored INTEGER NOT NULL,
    sequence_number BIGINT,
    last_modified_ledger BIGINT NOT NULL,
    ledger_sequence BIGINT NOT NULL,
    ledger_range BIGINT NOT NULL,

    -- Incremental versioning support (2 fields)
    era_id TEXT,
    version_label TEXT,

    PRIMARY KEY (account_id, ledger_sequence)
) WITH (FILLFACTOR=90);

COMMENT ON TABLE native_balances_snapshot_v1 IS 'XLM-only balances - FILLFACTOR=90 for frequent balance changes';

-- ============================================================================
-- ACCOUNT_SIGNERS_SNAPSHOT_V1 (9 fields)
-- ============================================================================
CREATE UNLOGGED TABLE account_signers_snapshot_v1 (
    -- Identity (3 fields)
    account_id TEXT NOT NULL,
    signer TEXT NOT NULL,
    ledger_sequence BIGINT NOT NULL,

    -- Signer details (2 fields)
    weight INTEGER NOT NULL,
    sponsor TEXT,

    -- Status (1 field)
    deleted BOOLEAN NOT NULL,

    -- Metadata (3 fields)
    closed_at TIMESTAMPTZ NOT NULL,
    ledger_range BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,

    -- Incremental versioning support (2 fields)
    era_id TEXT,
    version_label TEXT,

    PRIMARY KEY (account_id, signer, ledger_sequence)
);

COMMENT ON TABLE account_signers_snapshot_v1 IS 'Multi-sig signer configuration per account';

-- ============================================================================
-- OFFERS_SNAPSHOT_V1 (15 fields)
-- ============================================================================
CREATE UNLOGGED TABLE offers_snapshot_v1 (
    -- Identity (4 fields)
    offer_id BIGINT NOT NULL,
    seller_account TEXT NOT NULL,
    ledger_sequence BIGINT NOT NULL,
    closed_at TIMESTAMPTZ NOT NULL,

    -- Selling asset (3 fields)
    selling_asset_type TEXT NOT NULL,
    selling_asset_code TEXT,
    selling_asset_issuer TEXT,

    -- Buying asset (3 fields)
    buying_asset_type TEXT NOT NULL,
    buying_asset_code TEXT,
    buying_asset_issuer TEXT,

    -- Offer details (2 fields)
    amount TEXT NOT NULL,
    price TEXT NOT NULL,

    -- Flags (1 field)
    flags INTEGER NOT NULL,

    -- Metadata (2 fields)
    created_at TIMESTAMPTZ NOT NULL,
    ledger_range BIGINT NOT NULL,

    -- Incremental versioning support (2 fields)
    era_id TEXT,
    version_label TEXT,

    PRIMARY KEY (offer_id, ledger_sequence)
) WITH (FILLFACTOR=90);

COMMENT ON TABLE offers_snapshot_v1 IS 'DEX orderbook state - FILLFACTOR=90 for offer updates';

-- ============================================================================
-- CLAIMABLE_BALANCES_SNAPSHOT_V1 (12 fields)
-- ============================================================================
CREATE UNLOGGED TABLE claimable_balances_snapshot_v1 (
    -- Identity (4 fields)
    balance_id TEXT NOT NULL,
    sponsor TEXT NOT NULL,
    ledger_sequence BIGINT NOT NULL,
    closed_at TIMESTAMPTZ NOT NULL,

    -- Asset & Amount (4 fields)
    asset_type TEXT NOT NULL,
    asset_code TEXT,
    asset_issuer TEXT,
    amount BIGINT NOT NULL,

    -- Claimants (1 field)
    claimants_count INTEGER NOT NULL,

    -- Flags (1 field)
    flags INTEGER NOT NULL,

    -- Metadata (2 fields)
    created_at TIMESTAMPTZ NOT NULL,
    ledger_range BIGINT NOT NULL,

    -- Incremental versioning support (2 fields)
    era_id TEXT,
    version_label TEXT,

    PRIMARY KEY (balance_id, ledger_sequence)
) WITH (FILLFACTOR=90);

COMMENT ON TABLE claimable_balances_snapshot_v1 IS 'Claimable balance entries - FILLFACTOR=90';

-- ============================================================================
-- LIQUIDITY_POOLS_SNAPSHOT_V1 (17 fields)
-- ============================================================================
CREATE UNLOGGED TABLE liquidity_pools_snapshot_v1 (
    -- Identity (3 fields)
    liquidity_pool_id TEXT NOT NULL,
    ledger_sequence BIGINT NOT NULL,
    closed_at TIMESTAMPTZ NOT NULL,

    -- Pool type (1 field)
    pool_type TEXT NOT NULL,

    -- Fee (1 field)
    fee INTEGER NOT NULL,

    -- Pool shares (2 fields)
    trustline_count INTEGER NOT NULL,
    total_pool_shares BIGINT NOT NULL,

    -- Asset A (4 fields)
    asset_a_type TEXT NOT NULL,
    asset_a_code TEXT,
    asset_a_issuer TEXT,
    asset_a_amount BIGINT NOT NULL,

    -- Asset B (4 fields)
    asset_b_type TEXT NOT NULL,
    asset_b_code TEXT,
    asset_b_issuer TEXT,
    asset_b_amount BIGINT NOT NULL,

    -- Metadata (2 fields)
    created_at TIMESTAMPTZ NOT NULL,
    ledger_range BIGINT NOT NULL,

    -- Incremental versioning support (2 fields)
    era_id TEXT,
    version_label TEXT,

    PRIMARY KEY (liquidity_pool_id, ledger_sequence)
) WITH (FILLFACTOR=90);

COMMENT ON TABLE liquidity_pools_snapshot_v1 IS 'AMM pool state - FILLFACTOR=90 for liquidity changes';
