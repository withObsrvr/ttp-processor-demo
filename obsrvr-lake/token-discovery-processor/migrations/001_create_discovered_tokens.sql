-- Token Discovery Processor Schema
-- Migration: 001_create_discovered_tokens.sql

-- Discovered tokens registry
CREATE TABLE IF NOT EXISTS discovered_tokens (
    contract_id TEXT PRIMARY KEY,

    -- Classification
    token_type TEXT NOT NULL,        -- "sep41", "lp", "sac", "unknown"
    detection_method TEXT,           -- "function_calls", "sac_deploy", "lp_pattern"

    -- SEP-41 metadata
    name TEXT,
    symbol TEXT,
    decimals INTEGER,

    -- For SAC tokens (wrapped classic assets)
    is_sac BOOLEAN DEFAULT FALSE,
    classic_asset_code TEXT,
    classic_asset_issuer TEXT,

    -- For LP tokens
    lp_pool_type TEXT,               -- "constant_product", "stable"
    lp_asset_a TEXT,
    lp_asset_b TEXT,
    lp_fee_bps INTEGER,

    -- Discovery tracking
    first_seen_ledger BIGINT NOT NULL,
    last_activity_ledger BIGINT NOT NULL,
    discovered_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    -- Cached stats (updated by processor)
    holder_count INTEGER DEFAULT 0,
    transfer_count BIGINT DEFAULT 0,
    total_supply NUMERIC,

    -- Observed functions (for SEP-41 detection)
    observed_functions TEXT[],
    sep41_score INTEGER DEFAULT 0    -- Count of SEP-41 functions observed
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_discovered_tokens_type ON discovered_tokens(token_type);
CREATE INDEX IF NOT EXISTS idx_discovered_tokens_symbol ON discovered_tokens(symbol);
CREATE INDEX IF NOT EXISTS idx_discovered_tokens_last_activity ON discovered_tokens(last_activity_ledger DESC);
CREATE INDEX IF NOT EXISTS idx_discovered_tokens_holder_count ON discovered_tokens(holder_count DESC);
CREATE INDEX IF NOT EXISTS idx_discovered_tokens_is_sac ON discovered_tokens(is_sac) WHERE is_sac = TRUE;

-- Checkpoint table for tracking processor progress
CREATE TABLE IF NOT EXISTS token_discovery_checkpoint (
    id INTEGER PRIMARY KEY DEFAULT 1,
    last_ledger_sequence BIGINT NOT NULL DEFAULT 0,
    last_processed_at TIMESTAMP,
    tokens_discovered INTEGER DEFAULT 0,
    tokens_updated INTEGER DEFAULT 0,
    CONSTRAINT single_checkpoint CHECK (id = 1)
);

-- Initialize checkpoint if not exists
INSERT INTO token_discovery_checkpoint (id, last_ledger_sequence)
VALUES (1, 0) ON CONFLICT (id) DO NOTHING;
