-- Silver Hot Buffer: Snapshot Tables (SCD Type 2)
-- These tables preserve full history with valid_from/valid_to tracking
-- Created for Cycle 3: Incremental SCD Type 2 Pattern

-- Connect to silver_hot database
\c silver_hot

-- 1. accounts_snapshot - Full history of all account state changes
-- SCD Type 2: Multiple rows per account with validity periods
CREATE TABLE IF NOT EXISTS accounts_snapshot (
    account_id            TEXT NOT NULL,
    ledger_sequence       BIGINT NOT NULL,
    closed_at             TIMESTAMP WITH TIME ZONE NOT NULL,  -- valid_from (implicit)
    balance               TEXT NOT NULL,
    sequence_number       BIGINT NOT NULL,
    num_subentries        INTEGER NOT NULL,
    num_sponsoring        INTEGER NOT NULL,
    num_sponsored         INTEGER NOT NULL,
    home_domain           TEXT,
    master_weight         INTEGER NOT NULL,
    low_threshold         INTEGER NOT NULL,
    med_threshold         INTEGER NOT NULL,
    high_threshold        INTEGER NOT NULL,
    flags                 INTEGER NOT NULL,
    auth_required         BOOLEAN NOT NULL,
    auth_revocable        BOOLEAN NOT NULL,
    auth_immutable        BOOLEAN NOT NULL,
    auth_clawback_enabled BOOLEAN NOT NULL,
    signers               TEXT,
    sponsor_account       TEXT,
    created_at            TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at            TIMESTAMP WITH TIME ZONE NOT NULL,
    ledger_range          BIGINT NOT NULL,
    era_id                TEXT,
    version_label         TEXT,
    valid_to              TIMESTAMP WITH TIME ZONE,  -- NULL = current version
    PRIMARY KEY (account_id, ledger_sequence)
);

CREATE INDEX IF NOT EXISTS idx_accounts_snapshot_range ON accounts_snapshot(ledger_range);
CREATE INDEX IF NOT EXISTS idx_accounts_snapshot_account ON accounts_snapshot(account_id);
CREATE INDEX IF NOT EXISTS idx_accounts_snapshot_valid ON accounts_snapshot(account_id, valid_to) WHERE valid_to IS NULL;
CREATE INDEX IF NOT EXISTS idx_accounts_snapshot_ledger ON accounts_snapshot(ledger_sequence);

-- 2. trustlines_snapshot - Full history of all trustline changes
CREATE TABLE IF NOT EXISTS trustlines_snapshot (
    account_id                         TEXT NOT NULL,
    asset_code                         TEXT NOT NULL,
    asset_issuer                       TEXT NOT NULL,
    asset_type                         TEXT NOT NULL,
    balance                            TEXT NOT NULL,
    trust_limit                        TEXT NOT NULL,
    buying_liabilities                 TEXT NOT NULL,
    selling_liabilities                TEXT NOT NULL,
    authorized                         BOOLEAN NOT NULL,
    authorized_to_maintain_liabilities BOOLEAN NOT NULL,
    clawback_enabled                   BOOLEAN NOT NULL,
    ledger_sequence                    BIGINT NOT NULL,
    created_at                         TIMESTAMP WITH TIME ZONE NOT NULL,
    ledger_range                       BIGINT NOT NULL,
    era_id                             TEXT,
    version_label                      TEXT,
    valid_to                           TIMESTAMP WITH TIME ZONE,  -- NULL = current version
    PRIMARY KEY (account_id, asset_code, asset_issuer, asset_type, ledger_sequence)
);

CREATE INDEX IF NOT EXISTS idx_trustlines_snapshot_range ON trustlines_snapshot(ledger_range);
CREATE INDEX IF NOT EXISTS idx_trustlines_snapshot_account ON trustlines_snapshot(account_id);
CREATE INDEX IF NOT EXISTS idx_trustlines_snapshot_valid ON trustlines_snapshot(account_id, asset_code, asset_issuer, asset_type, valid_to) WHERE valid_to IS NULL;

-- 3. offers_snapshot - Full history of all offer changes
CREATE TABLE IF NOT EXISTS offers_snapshot (
    offer_id             BIGINT NOT NULL,
    seller_account       TEXT NOT NULL,
    ledger_sequence      BIGINT NOT NULL,
    closed_at            TIMESTAMP WITH TIME ZONE NOT NULL,
    selling_asset_type   TEXT NOT NULL,
    selling_asset_code   TEXT,
    selling_asset_issuer TEXT,
    buying_asset_type    TEXT NOT NULL,
    buying_asset_code    TEXT,
    buying_asset_issuer  TEXT,
    amount               TEXT NOT NULL,
    price                TEXT NOT NULL,
    flags                INTEGER NOT NULL,
    created_at           TIMESTAMP WITH TIME ZONE NOT NULL,
    ledger_range         BIGINT NOT NULL,
    era_id               TEXT,
    version_label        TEXT,
    valid_to             TIMESTAMP WITH TIME ZONE,  -- NULL = current version
    PRIMARY KEY (offer_id, ledger_sequence)
);

CREATE INDEX IF NOT EXISTS idx_offers_snapshot_range ON offers_snapshot(ledger_range);
CREATE INDEX IF NOT EXISTS idx_offers_snapshot_offer ON offers_snapshot(offer_id);
CREATE INDEX IF NOT EXISTS idx_offers_snapshot_seller ON offers_snapshot(seller_account);
CREATE INDEX IF NOT EXISTS idx_offers_snapshot_valid ON offers_snapshot(offer_id, valid_to) WHERE valid_to IS NULL;

-- 4. account_signers_snapshot - Full history of account signer changes
CREATE TABLE IF NOT EXISTS account_signers_snapshot (
    account_id       TEXT NOT NULL,
    signer           TEXT NOT NULL,
    ledger_sequence  BIGINT NOT NULL,
    closed_at        TIMESTAMP WITH TIME ZONE NOT NULL,  -- valid_from (implicit)
    weight           INTEGER NOT NULL,
    sponsor          TEXT,
    ledger_range     BIGINT NOT NULL,
    era_id           TEXT,
    version_label    TEXT,
    valid_to         TIMESTAMP WITH TIME ZONE,  -- NULL = current version
    PRIMARY KEY (account_id, signer, ledger_sequence)
);

CREATE INDEX IF NOT EXISTS idx_account_signers_snapshot_range ON account_signers_snapshot(ledger_range);
CREATE INDEX IF NOT EXISTS idx_account_signers_snapshot_account ON account_signers_snapshot(account_id);
CREATE INDEX IF NOT EXISTS idx_account_signers_snapshot_valid ON account_signers_snapshot(account_id, signer, valid_to) WHERE valid_to IS NULL;

-- 5. claimable_balances_snapshot - Full history of claimable balance changes
CREATE TABLE IF NOT EXISTS claimable_balances_snapshot (
    balance_id       TEXT NOT NULL,
    ledger_sequence  BIGINT NOT NULL,
    asset_type       TEXT NOT NULL,
    asset_code       TEXT NOT NULL,
    asset_issuer     TEXT NOT NULL,
    amount           TEXT NOT NULL,
    claimants        TEXT NOT NULL,  -- JSON array
    flags            INTEGER NOT NULL,
    sponsor_account  TEXT,
    ledger_range     BIGINT NOT NULL,
    era_id           TEXT,
    version_label    TEXT,
    valid_to         TIMESTAMP WITH TIME ZONE,  -- NULL = current version
    PRIMARY KEY (balance_id, ledger_sequence)
);

CREATE INDEX IF NOT EXISTS idx_claimable_balances_snapshot_range ON claimable_balances_snapshot(ledger_range);
CREATE INDEX IF NOT EXISTS idx_claimable_balances_snapshot_balance ON claimable_balances_snapshot(balance_id);
CREATE INDEX IF NOT EXISTS idx_claimable_balances_snapshot_valid ON claimable_balances_snapshot(balance_id, valid_to) WHERE valid_to IS NULL;

-- Grant permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO stellar;

-- Notes on SCD Type 2 Pattern:
-- 1. Each entity can have multiple rows (one per state change)
-- 2. Primary key includes ledger_sequence for versioning
-- 3. closed_at serves as valid_from (from ledger close time)
-- 4. valid_to is NULL for current version, populated for historical versions
-- 5. To get current state: WHERE valid_to IS NULL
-- 6. To get state at time T: WHERE closed_at <= T AND (valid_to IS NULL OR valid_to > T)
-- 7. Partitioning by ledger_range enables efficient boundary updates
