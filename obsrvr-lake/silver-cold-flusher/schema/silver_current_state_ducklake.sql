-- Silver Hot Buffer: Current State Tables (FIXED SCHEMA)
-- These tables track the current/latest state of entities using UPSERT pattern
-- Updated to use Stellar internal naming convention (low_threshold, med_threshold, high_threshold)

-- 1. accounts_current - Latest state of all accounts
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.accounts_current (
    account_id            TEXT,
    balance               TEXT,
    sequence_number       BIGINT,
    num_subentries        INTEGER,
    num_sponsoring        INTEGER,
    num_sponsored         INTEGER,
    home_domain           TEXT,
    master_weight         INTEGER,
    low_threshold         INTEGER,
    med_threshold         INTEGER,
    high_threshold        INTEGER,
    flags                 INTEGER,
    auth_required         BOOLEAN,
    auth_revocable        BOOLEAN,
    auth_immutable        BOOLEAN,
    auth_clawback_enabled BOOLEAN,
    signers               TEXT,
    sponsor_account       TEXT,
    created_at            TIMESTAMP WITH TIME ZONE,
    updated_at            TIMESTAMP WITH TIME ZONE,
    last_modified_ledger  BIGINT,
    ledger_range          BIGINT,
    era_id                TEXT,
    version_label         TEXT
);

CREATE INDEX IF NOT EXISTS idx_accounts_current_modified ON accounts_current(last_modified_ledger);
CREATE INDEX IF NOT EXISTS idx_accounts_current_range ON accounts_current(ledger_range);

-- 2. account_signers_current - Latest signers for each account
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.account_signers_current (
    account_id            TEXT,
    signer                TEXT,
    weight                INTEGER,
    sponsor               TEXT,
    last_modified_ledger  BIGINT,
    ledger_range          BIGINT,
    era_id                TEXT,
    version_label         TEXT, (account_id, signer)
);

CREATE INDEX IF NOT EXISTS idx_account_signers_current_modified ON account_signers_current(last_modified_ledger);

-- 3. trustlines_current - Latest trustlines for all accounts
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.trustlines_current (
    account_id            TEXT,
    asset_code            TEXT,
    asset_issuer          TEXT,
    asset_type            TEXT,
    balance               TEXT,
    trust_limit           TEXT,
    buying_liabilities    TEXT,
    selling_liabilities   TEXT,
    authorized            BOOLEAN,
    authorized_to_maintain_liabilities BOOLEAN,
    clawback_enabled      BOOLEAN,
    created_at            TIMESTAMP WITH TIME ZONE,
    last_modified_ledger  BIGINT,
    ledger_range          BIGINT,
    era_id                TEXT,
    version_label         TEXT, (account_id, asset_code, asset_issuer, asset_type)
);

CREATE INDEX IF NOT EXISTS idx_trustlines_current_modified ON trustlines_current(last_modified_ledger);
CREATE INDEX IF NOT EXISTS idx_trustlines_current_account ON trustlines_current(account_id);

-- 4. offers_current - Latest active offers
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.offers_current (
    offer_id              BIGINT,
    seller_account        TEXT,
    closed_at             TIMESTAMP WITH TIME ZONE,
    selling_asset_type    TEXT,
    selling_asset_code    TEXT,
    selling_asset_issuer  TEXT,
    buying_asset_type     TEXT,
    buying_asset_code     TEXT,
    buying_asset_issuer   TEXT,
    amount                TEXT,
    price                 TEXT,
    flags                 INTEGER,
    created_at            TIMESTAMP WITH TIME ZONE,
    last_modified_ledger  BIGINT,
    ledger_range          BIGINT,
    era_id                TEXT,
    version_label         TEXT
);

CREATE INDEX IF NOT EXISTS idx_offers_current_modified ON offers_current(last_modified_ledger);
CREATE INDEX IF NOT EXISTS idx_offers_current_seller ON offers_current(seller_account);

-- 5. claimable_balances_current - Latest claimable balances
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.claimable_balances_current (
    balance_id            TEXT,
    asset_type            TEXT,
    asset_code            TEXT,
    asset_issuer          TEXT,
    amount                TEXT,
    claimants             TEXT,  -- JSON array
    flags                 INTEGER,
    sponsor_account       TEXT,
    last_modified_ledger  BIGINT,
    ledger_range          BIGINT,
    era_id                TEXT,
    version_label         TEXT
);

CREATE INDEX IF NOT EXISTS idx_claimable_balances_current_modified ON claimable_balances_current(last_modified_ledger);

-- 6. liquidity_pools_current - Latest liquidity pools
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.liquidity_pools_current (
    liquidity_pool_id     TEXT,
    pool_type             TEXT,
    fee                   INTEGER,
    asset_a_type          TEXT,
    asset_a_code          TEXT,
    asset_a_issuer        TEXT,
    asset_a_amount        TEXT,
    asset_b_type          TEXT,
    asset_b_code          TEXT,
    asset_b_issuer        TEXT,
    asset_b_amount        TEXT,
    total_pool_shares     TEXT,
    pool_share_count      BIGINT,
    last_modified_ledger  BIGINT,
    ledger_range          BIGINT,
    era_id                TEXT,
    version_label         TEXT
);

CREATE INDEX IF NOT EXISTS idx_liquidity_pools_current_modified ON liquidity_pools_current(last_modified_ledger);

-- 7. contract_data_current - Latest Soroban contract data entries
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.contract_data_current (
    contract_id           TEXT,
    ledger_key_hash       TEXT,
    contract_key_type     TEXT,
    durability            TEXT,
    contract_data         TEXT,  -- Base64 XDR
    expiration_ledger     BIGINT,
    last_modified_ledger  BIGINT,
    ledger_range          BIGINT,
    era_id                TEXT,
    version_label         TEXT, (contract_id, ledger_key_hash)
);

CREATE INDEX IF NOT EXISTS idx_contract_data_current_modified ON contract_data_current(last_modified_ledger);
CREATE INDEX IF NOT EXISTS idx_contract_data_current_contract ON contract_data_current(contract_id);

-- 8. contract_code_current - Latest Soroban contract code (WASM)
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.contract_code_current (
    contract_code_hash    TEXT,
    contract_code         TEXT,  -- Base64 XDR WASM bytecode
    expiration_ledger     BIGINT,
    last_modified_ledger  BIGINT,
    ledger_range          BIGINT,
    era_id                TEXT,
    version_label         TEXT
);

CREATE INDEX IF NOT EXISTS idx_contract_code_current_modified ON contract_code_current(last_modified_ledger);

-- 9. config_settings_current - Latest protocol config settings
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.config_settings_current (
    config_setting_id     TEXT,
    config_setting        TEXT,  -- Base64 XDR
    last_modified_ledger  BIGINT,
    ledger_range          BIGINT,
    era_id                TEXT,
    version_label         TEXT
);

CREATE INDEX IF NOT EXISTS idx_config_settings_current_modified ON config_settings_current(last_modified_ledger);

-- 10. ttl_current - Latest Soroban state TTL entries
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.ttl_current (
    key_hash              TEXT,
    live_until_ledger     BIGINT,
    last_modified_ledger  BIGINT,
    ledger_range          BIGINT,
    era_id                TEXT,
    version_label         TEXT
);

CREATE INDEX IF NOT EXISTS idx_ttl_current_modified ON ttl_current(last_modified_ledger);

-- Grant permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO stellar;
