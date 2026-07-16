-- Migration 008: Carry Horizon transaction-resource XDR fields in bronze hot.
-- tx_envelope/tx_result/tx_fee_meta/tx_signers are mandatory for
-- Horizon-compatible transaction responses (the reader refuses rows missing
-- them); tx_meta is served when present.

ALTER TABLE transactions_row_v2 ADD COLUMN IF NOT EXISTS tx_envelope TEXT;
ALTER TABLE transactions_row_v2 ADD COLUMN IF NOT EXISTS tx_result TEXT;
ALTER TABLE transactions_row_v2 ADD COLUMN IF NOT EXISTS tx_meta TEXT;
ALTER TABLE transactions_row_v2 ADD COLUMN IF NOT EXISTS tx_fee_meta TEXT;
ALTER TABLE transactions_row_v2 ADD COLUMN IF NOT EXISTS tx_signers TEXT;
