ALTER TABLE accounts_snapshot_v1 ADD COLUMN IF NOT EXISTS sequence_ledger BIGINT;
ALTER TABLE accounts_snapshot_v1 ADD COLUMN IF NOT EXISTS sequence_time BIGINT;
