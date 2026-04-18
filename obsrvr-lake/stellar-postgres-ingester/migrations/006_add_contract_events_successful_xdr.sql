-- Migration: Catch up contract_events_stream_v1 columns that PR #65 (9ead3b0)
-- added to the ingester INSERT path but never shipped a hot-PG migration for.
-- The columns already exist in the DuckLake cold schema
-- (postgres-ducklake-flusher/v3_bronze_schema.sql); this file brings hot PG
-- into parity so the ingester's COPY / INSERT succeeds.
-- Date: 2026-04-15

BEGIN;

ALTER TABLE contract_events_stream_v1
  ADD COLUMN IF NOT EXISTS successful BOOLEAN;

ALTER TABLE contract_events_stream_v1
  ADD COLUMN IF NOT EXISTS contract_event_xdr TEXT;

-- Backfill: historical rows predate the distinction, so default successful to
-- in_successful_contract_call (identical semantics for pre-diagnostic-event
-- contract events).
UPDATE contract_events_stream_v1
SET successful = in_successful_contract_call
WHERE successful IS NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'contract_events_stream_v1'
        AND column_name = 'successful'
    ) THEN
        RAISE EXCEPTION 'Failed to add successful column';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'contract_events_stream_v1'
        AND column_name = 'contract_event_xdr'
    ) THEN
        RAISE EXCEPTION 'Failed to add contract_event_xdr column';
    END IF;
    RAISE NOTICE 'Successfully synced contract_events_stream_v1 columns';
END $$;

COMMIT;
