-- Event classification rules for the explorer events endpoint.
-- Rules are evaluated in priority order (highest first). First match wins.
-- This table is read by the stellar-query-api at startup and can be
-- hot-reloaded via POST /api/v1/explorer/events/rules/reload.

CREATE TABLE IF NOT EXISTS event_classification_rules (
    rule_id          SERIAL PRIMARY KEY,
    priority         INT NOT NULL DEFAULT 0,        -- higher = checked first
    event_type       TEXT NOT NULL,                  -- classified type: transfer, swap, mint, burn, approve, etc.
    protocol         TEXT,                           -- attribution: soroswap, aquarius, phoenix, sep41, etc.
    match_contracts  TEXT[],                         -- contract IDs to match (NULL = any contract)
    match_topic0     TEXT[],                         -- topic0 values to match (NULL = any)
    match_topic_sig  TEXT,                           -- regex on topics_decoded (NULL = skip)
    description      TEXT,                           -- human-readable description of what this rule detects
    enabled          BOOLEAN NOT NULL DEFAULT true,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    updated_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ecr_priority ON event_classification_rules (priority DESC) WHERE enabled = true;

-- Seed initial rules (priority order: protocol-specific first, generic fallbacks last)

-- Soroswap swap detection
INSERT INTO event_classification_rules (priority, event_type, protocol, match_contracts, match_topic0, description)
VALUES (100, 'swap', 'soroswap',
    ARRAY['CAG5LRYQ5JVEUI5TEID72EYOVX44TTUJT5BQR2J6J77FH65PCCFAJDDH'],
    ARRAY['swap'],
    'Soroswap router swap events')
ON CONFLICT DO NOTHING;

-- Generic SEP-41 token events (lower priority, catch-all for standard token ops)
INSERT INTO event_classification_rules (priority, event_type, protocol, match_topic0, description)
VALUES
    (10, 'transfer', 'sep41', ARRAY['transfer'], 'SEP-41 token transfer events'),
    (10, 'mint', 'sep41', ARRAY['mint'], 'SEP-41 token mint events'),
    (10, 'burn', 'sep41', ARRAY['burn'], 'SEP-41 token burn events'),
    (10, 'approve', 'sep41', ARRAY['approve'], 'SEP-41 token approve events')
ON CONFLICT DO NOTHING;

-- Default fallback (lowest priority)
INSERT INTO event_classification_rules (priority, event_type, protocol, description)
VALUES (0, 'contract_call', NULL, 'Default fallback for unclassified contract events')
ON CONFLICT DO NOTHING;
