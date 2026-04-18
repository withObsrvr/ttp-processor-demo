-- Migration: Capture SorobanAuthorizationEntry credentials on operations
-- Purpose:   Persist the authorizer address(es) and credentials discriminant
--            for each InvokeHostFunction op so smart accounts (C-addresses
--            authorizing via SOROBAN_CREDENTIALS_ADDRESS) can be identified
--            directly from bronze without heuristic inference.
-- Date:      2026-04-15
--
-- Columns added (both parallel arrays, one entry per auth entry on the op):
--   soroban_auth_credentials_types  TEXT[]  'SOURCE_ACCOUNT' or 'ADDRESS'
--   soroban_auth_addresses          TEXT[]  strkey-encoded authorizer
--                                            (empty string for SOURCE_ACCOUNT)
--
-- Both arrays are the same length as operations_row_v2 rows with at least
-- one auth entry. NULL for non-Soroban ops and InvokeHostFunction ops with
-- zero auth entries.

BEGIN;

ALTER TABLE operations_row_v2
  ADD COLUMN IF NOT EXISTS soroban_auth_credentials_types TEXT[];

ALTER TABLE operations_row_v2
  ADD COLUMN IF NOT EXISTS soroban_auth_addresses TEXT[];

-- GIN index on authorizer addresses for efficient "find ops where this
-- C-address authorized" queries (the primary smart-account use case).
CREATE INDEX IF NOT EXISTS idx_operations_soroban_auth_addresses
  ON operations_row_v2 USING GIN (soroban_auth_addresses)
  WHERE soroban_auth_addresses IS NOT NULL;

-- Verify columns exist.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'operations_row_v2'
        AND column_name = 'soroban_auth_credentials_types'
    ) THEN
        RAISE EXCEPTION 'Failed to add soroban_auth_credentials_types column';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'operations_row_v2'
        AND column_name = 'soroban_auth_addresses'
    ) THEN
        RAISE EXCEPTION 'Failed to add soroban_auth_addresses column';
    END IF;

    RAISE NOTICE 'Successfully added soroban auth credential columns to operations_row_v2';
END $$;

COMMIT;
