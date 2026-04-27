-- Performance indexes for address_balances_current state projection.
-- Run on silver_hot. Uses CONCURRENTLY to avoid blocking hot writes.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_native_balances_last_modified_account
    ON native_balances_current (last_modified_ledger, account_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trustlines_last_modified_account_asset
    ON trustlines_current (
        last_modified_ledger,
        account_id,
        asset_type,
        COALESCE(asset_code, ''),
        COALESCE(asset_issuer, ''),
        COALESCE(liquidity_pool_id, '')
    );

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_address_balances_source_updated
    ON address_balances_current (balance_source, last_updated_ledger DESC);
