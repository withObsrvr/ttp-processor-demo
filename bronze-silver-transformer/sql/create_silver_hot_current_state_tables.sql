-- Silver Hot Buffer: Current State Tables (FIXED SCHEMA)
-- These tables track the current/latest state of entities using UPSERT pattern
-- Updated to use Stellar internal naming convention (low_threshold, med_threshold, high_threshold)

-- 1. accounts_current - Latest state of all accounts
CREATE TABLE IF NOT EXISTS accounts_current (
    account_id            TEXT PRIMARY KEY,
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
    last_modified_ledger  BIGINT NOT NULL,
    ledger_range          BIGINT NOT NULL,
    era_id                TEXT,
    version_label         TEXT
);

CREATE INDEX IF NOT EXISTS idx_accounts_current_modified ON accounts_current(last_modified_ledger);
CREATE INDEX IF NOT EXISTS idx_accounts_current_range ON accounts_current(ledger_range);

-- 2. account_signers_current - Latest signers for each account
CREATE TABLE IF NOT EXISTS account_signers_current (
    account_id            TEXT NOT NULL,
    signer                TEXT NOT NULL,
    weight                INTEGER NOT NULL,
    sponsor               TEXT,
    last_modified_ledger  BIGINT NOT NULL,
    ledger_range          BIGINT NOT NULL,
    era_id                TEXT,
    version_label         TEXT,
    PRIMARY KEY (account_id, signer)
);

CREATE INDEX IF NOT EXISTS idx_account_signers_current_modified ON account_signers_current(last_modified_ledger);

-- 3. trustlines_current - Latest trustlines for all accounts
CREATE TABLE IF NOT EXISTS trustlines_current (
    account_id            TEXT NOT NULL,
    asset_code            TEXT NOT NULL,
    asset_issuer          TEXT NOT NULL,
    asset_type            TEXT NOT NULL,
    balance               TEXT NOT NULL,
    trust_limit           TEXT NOT NULL,
    buying_liabilities    TEXT NOT NULL,
    selling_liabilities   TEXT NOT NULL,
    authorized            BOOLEAN NOT NULL,
    authorized_to_maintain_liabilities BOOLEAN NOT NULL,
    clawback_enabled      BOOLEAN NOT NULL,
    created_at            TIMESTAMP WITH TIME ZONE NOT NULL,
    last_modified_ledger  BIGINT NOT NULL,
    ledger_range          BIGINT NOT NULL,
    era_id                TEXT,
    version_label         TEXT,
    PRIMARY KEY (account_id, asset_code, asset_issuer, asset_type)
);

CREATE INDEX IF NOT EXISTS idx_trustlines_current_modified ON trustlines_current(last_modified_ledger);
CREATE INDEX IF NOT EXISTS idx_trustlines_current_account ON trustlines_current(account_id);

-- 4. offers_current - Latest active offers
CREATE TABLE IF NOT EXISTS offers_current (
    offer_id              BIGINT PRIMARY KEY,
    seller_account        TEXT NOT NULL,
    closed_at             TIMESTAMP WITH TIME ZONE NOT NULL,
    selling_asset_type    TEXT NOT NULL,
    selling_asset_code    TEXT,
    selling_asset_issuer  TEXT,
    buying_asset_type     TEXT NOT NULL,
    buying_asset_code     TEXT,
    buying_asset_issuer   TEXT,
    amount                TEXT NOT NULL,
    price                 TEXT NOT NULL,
    flags                 INTEGER NOT NULL,
    created_at            TIMESTAMP WITH TIME ZONE NOT NULL,
    last_modified_ledger  BIGINT NOT NULL,
    ledger_range          BIGINT NOT NULL,
    era_id                TEXT,
    version_label         TEXT
);

CREATE INDEX IF NOT EXISTS idx_offers_current_modified ON offers_current(last_modified_ledger);
CREATE INDEX IF NOT EXISTS idx_offers_current_seller ON offers_current(seller_account);

-- 5. claimable_balances_current - Latest claimable balances
CREATE TABLE IF NOT EXISTS claimable_balances_current (
    balance_id            TEXT PRIMARY KEY,
    asset_type            TEXT NOT NULL,
    asset_code            TEXT NOT NULL,
    asset_issuer          TEXT NOT NULL,
    amount                TEXT NOT NULL,
    claimants             TEXT NOT NULL,  -- JSON array
    flags                 INTEGER NOT NULL,
    sponsor_account       TEXT,
    last_modified_ledger  BIGINT NOT NULL,
    ledger_range          BIGINT NOT NULL,
    era_id                TEXT,
    version_label         TEXT
);

CREATE INDEX IF NOT EXISTS idx_claimable_balances_current_modified ON claimable_balances_current(last_modified_ledger);

-- 6. liquidity_pools_current - Latest liquidity pools
CREATE TABLE IF NOT EXISTS liquidity_pools_current (
    liquidity_pool_id     TEXT PRIMARY KEY,
    pool_type             TEXT NOT NULL,
    fee                   INTEGER NOT NULL,
    asset_a_type          TEXT NOT NULL,
    asset_a_code          TEXT NOT NULL,
    asset_a_issuer        TEXT NOT NULL,
    asset_a_amount        TEXT NOT NULL,
    asset_b_type          TEXT NOT NULL,
    asset_b_code          TEXT NOT NULL,
    asset_b_issuer        TEXT NOT NULL,
    asset_b_amount        TEXT NOT NULL,
    total_pool_shares     TEXT NOT NULL,
    pool_share_count      BIGINT NOT NULL,
    last_modified_ledger  BIGINT NOT NULL,
    ledger_range          BIGINT NOT NULL,
    era_id                TEXT,
    version_label         TEXT
);

CREATE INDEX IF NOT EXISTS idx_liquidity_pools_current_modified ON liquidity_pools_current(last_modified_ledger);

-- 7. contract_data_current - Latest Soroban contract data entries
CREATE TABLE IF NOT EXISTS contract_data_current (
    contract_id           TEXT NOT NULL,
    ledger_key_hash       TEXT NOT NULL,
    contract_key_type     TEXT NOT NULL,
    durability            TEXT NOT NULL,
    contract_data         TEXT NOT NULL,  -- Base64 XDR
    expiration_ledger     BIGINT,
    last_modified_ledger  BIGINT NOT NULL,
    ledger_range          BIGINT NOT NULL,
    era_id                TEXT,
    version_label         TEXT,
    PRIMARY KEY (contract_id, ledger_key_hash)
);

CREATE INDEX IF NOT EXISTS idx_contract_data_current_modified ON contract_data_current(last_modified_ledger);
CREATE INDEX IF NOT EXISTS idx_contract_data_current_contract ON contract_data_current(contract_id);

-- 8. contract_code_current - Latest Soroban contract code (WASM)
CREATE TABLE IF NOT EXISTS contract_code_current (
    contract_code_hash    TEXT PRIMARY KEY,
    contract_code         TEXT NOT NULL,  -- Base64 XDR WASM bytecode
    expiration_ledger     BIGINT,
    last_modified_ledger  BIGINT NOT NULL,
    ledger_range          BIGINT NOT NULL,
    era_id                TEXT,
    version_label         TEXT
);

CREATE INDEX IF NOT EXISTS idx_contract_code_current_modified ON contract_code_current(last_modified_ledger);

-- 9. config_settings_current - Latest protocol config settings
CREATE TABLE IF NOT EXISTS config_settings_current (
    config_setting_id     TEXT PRIMARY KEY,
    config_setting        TEXT NOT NULL,  -- Base64 XDR
    last_modified_ledger  BIGINT NOT NULL,
    ledger_range          BIGINT NOT NULL,
    era_id                TEXT,
    version_label         TEXT
);

CREATE INDEX IF NOT EXISTS idx_config_settings_current_modified ON config_settings_current(last_modified_ledger);

-- 10. ttl_current - Latest Soroban state TTL entries
CREATE TABLE IF NOT EXISTS ttl_current (
    key_hash              TEXT PRIMARY KEY,
    live_until_ledger     BIGINT NOT NULL,
    last_modified_ledger  BIGINT NOT NULL,
    ledger_range          BIGINT NOT NULL,
    era_id                TEXT,
    version_label         TEXT
);

CREATE INDEX IF NOT EXISTS idx_ttl_current_modified ON ttl_current(last_modified_ledger);

-- Grant permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO stellar;
