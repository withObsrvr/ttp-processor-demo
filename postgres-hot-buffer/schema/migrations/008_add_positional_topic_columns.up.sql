-- Migration 008: Add positional topic columns to contract_events_stream_v1
-- Purpose: Enable indexed positional topic filtering (topic0=transfer&topic2=GABC...)
-- matching Stellar RPC v2 positional semantics.
-- Date: 2026-03-23

ALTER TABLE contract_events_stream_v1
  ADD COLUMN IF NOT EXISTS topic0_decoded TEXT,
  ADD COLUMN IF NOT EXISTS topic1_decoded TEXT,
  ADD COLUMN IF NOT EXISTS topic2_decoded TEXT,
  ADD COLUMN IF NOT EXISTS topic3_decoded TEXT;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_topic0_decoded
  ON contract_events_stream_v1 (topic0_decoded) WHERE topic0_decoded IS NOT NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_topic1_decoded
  ON contract_events_stream_v1 (topic1_decoded) WHERE topic1_decoded IS NOT NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_topic2_decoded
  ON contract_events_stream_v1 (topic2_decoded) WHERE topic2_decoded IS NOT NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_topic3_decoded
  ON contract_events_stream_v1 (topic3_decoded) WHERE topic3_decoded IS NOT NULL;
