-- Migration 008: Carry Horizon transaction-resource XDR fields in bronze hot.
-- These fields are mandatory for Horizon-compatible transaction responses.

ALTER TABLE transactions_row_v2 ADD COLUMN IF NOT EXISTS tx_envelope TEXT;
ALTER TABLE transactions_row_v2 ADD COLUMN IF NOT EXISTS tx_result TEXT;
ALTER TABLE transactions_row_v2 ADD COLUMN IF NOT EXISTS tx_meta TEXT;
ALTER TABLE transactions_row_v2 ADD COLUMN IF NOT EXISTS tx_fee_meta TEXT;
ALTER TABLE transactions_row_v2 ADD COLUMN IF NOT EXISTS tx_signers TEXT;
