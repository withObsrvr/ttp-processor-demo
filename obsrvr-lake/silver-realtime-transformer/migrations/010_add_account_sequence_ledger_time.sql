ALTER TABLE accounts_current ADD COLUMN IF NOT EXISTS sequence_ledger BIGINT;
ALTER TABLE accounts_current ADD COLUMN IF NOT EXISTS sequence_time BIGINT;

ALTER TABLE accounts_snapshot ADD COLUMN IF NOT EXISTS sequence_ledger BIGINT;
ALTER TABLE accounts_snapshot ADD COLUMN IF NOT EXISTS sequence_time BIGINT;
