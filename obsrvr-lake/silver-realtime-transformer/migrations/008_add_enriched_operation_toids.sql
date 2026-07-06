-- Migration 008: Carry canonical TOIDs into enriched_history_operations
-- Purpose: serving/account feeds need cursor-facing Horizon-compatible TOIDs,
-- not hash-derived or query-local transaction ordering.

ALTER TABLE enriched_history_operations
    ADD COLUMN IF NOT EXISTS transaction_id BIGINT,
    ADD COLUMN IF NOT EXISTS operation_id BIGINT;

ALTER TABLE enriched_history_operations_soroban
    ADD COLUMN IF NOT EXISTS transaction_id BIGINT,
    ADD COLUMN IF NOT EXISTS operation_id BIGINT;

CREATE INDEX IF NOT EXISTS idx_enriched_ops_transaction_id
    ON enriched_history_operations(transaction_id)
    WHERE transaction_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_enriched_ops_operation_id
    ON enriched_history_operations(operation_id)
    WHERE operation_id IS NOT NULL;
