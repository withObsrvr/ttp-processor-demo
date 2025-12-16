-- Rollback migration 003: Drop account and DEX snapshot tables

DROP TABLE IF EXISTS liquidity_pools_snapshot_v1;
DROP TABLE IF EXISTS claimable_balances_snapshot_v1;
DROP TABLE IF EXISTS offers_snapshot_v1;
DROP TABLE IF EXISTS account_signers_snapshot_v1;
DROP TABLE IF EXISTS native_balances_snapshot_v1;
DROP TABLE IF EXISTS trustlines_snapshot_v1;
DROP TABLE IF EXISTS accounts_snapshot_v1;
