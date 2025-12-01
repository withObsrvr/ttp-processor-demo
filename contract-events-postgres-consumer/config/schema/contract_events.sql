-- Contract Events Schema
-- Hybrid design: Indexed columns for hot paths + JSONB for flexibility

CREATE TABLE IF NOT EXISTS contract_events (
    -- Primary key
    id BIGSERIAL PRIMARY KEY,

    -- Ledger metadata (indexed for time-series queries)
    ledger_sequence INTEGER NOT NULL,
    ledger_closed_at TIMESTAMP WITH TIME ZONE NOT NULL,

    -- Transaction metadata (indexed for lookups)
    tx_hash TEXT NOT NULL,
    tx_successful BOOLEAN NOT NULL DEFAULT true,
    tx_index INTEGER NOT NULL,

    -- Event metadata (indexed for filtering)
    contract_id TEXT NOT NULL,
    event_type TEXT NOT NULL,  -- transfer, mint, burn, swap, etc.
    event_index INTEGER NOT NULL,
    operation_index INTEGER,   -- NULL for transaction-level events

    -- Flexible payload (JSONB enables querying any field)
    topics JSONB NOT NULL,          -- Array of decoded topics
    event_data JSONB,               -- Decoded event data
    topics_xdr TEXT[],              -- Raw XDR (base64) for topics
    data_xdr TEXT,                  -- Raw XDR (base64) for data

    -- System metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for common query patterns

-- Time-series queries (ledger range)
CREATE INDEX IF NOT EXISTS idx_contract_events_ledger_sequence
    ON contract_events(ledger_sequence DESC);

-- Closed time queries (chronological)
CREATE INDEX IF NOT EXISTS idx_contract_events_closed_at
    ON contract_events(ledger_closed_at DESC);

-- Transaction lookup
CREATE INDEX IF NOT EXISTS idx_contract_events_tx_hash
    ON contract_events(tx_hash);

-- Contract-specific queries (most common)
CREATE INDEX IF NOT EXISTS idx_contract_events_contract_id
    ON contract_events(contract_id);

-- Event type filtering
CREATE INDEX IF NOT EXISTS idx_contract_events_event_type
    ON contract_events(event_type);

-- Combined contract + event type (common DeFi queries)
CREATE INDEX IF NOT EXISTS idx_contract_events_contract_type
    ON contract_events(contract_id, event_type);

-- Successful transactions only (exclude failed)
CREATE INDEX IF NOT EXISTS idx_contract_events_successful
    ON contract_events(tx_successful) WHERE tx_successful = true;

-- GIN index for JSONB queries (enables querying any field in topics/data)
CREATE INDEX IF NOT EXISTS idx_contract_events_topics_gin
    ON contract_events USING GIN (topics);

CREATE INDEX IF NOT EXISTS idx_contract_events_data_gin
    ON contract_events USING GIN (event_data);

-- Composite index for common analytics queries
CREATE INDEX IF NOT EXISTS idx_contract_events_analytics
    ON contract_events(contract_id, event_type, ledger_closed_at DESC);

-- Comments for documentation
COMMENT ON TABLE contract_events IS 'Soroban contract events with hybrid schema';
COMMENT ON COLUMN contract_events.topics IS 'Decoded event topics as JSONB array';
COMMENT ON COLUMN contract_events.event_data IS 'Decoded event data as JSONB';
COMMENT ON COLUMN contract_events.topics_xdr IS 'Raw XDR topics (base64) for verification';
COMMENT ON COLUMN contract_events.data_xdr IS 'Raw XDR data (base64) for verification';
