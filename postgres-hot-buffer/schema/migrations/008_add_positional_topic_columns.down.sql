-- Rollback: Remove positional topic columns from contract_events_stream_v1

DROP INDEX IF EXISTS idx_events_topic3_decoded;
DROP INDEX IF EXISTS idx_events_topic2_decoded;
DROP INDEX IF EXISTS idx_events_topic1_decoded;
DROP INDEX IF EXISTS idx_events_topic0_decoded;

ALTER TABLE contract_events_stream_v1
  DROP COLUMN IF EXISTS topic3_decoded,
  DROP COLUMN IF EXISTS topic2_decoded,
  DROP COLUMN IF EXISTS topic1_decoded,
  DROP COLUMN IF EXISTS topic0_decoded;
