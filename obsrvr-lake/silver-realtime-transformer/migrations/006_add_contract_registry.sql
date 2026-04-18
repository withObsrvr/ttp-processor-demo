-- Ensure pg_trgm extension for trigram search index
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Contract registry: single source of truth for contract display names.
-- Names any contract (tokens, DEXes, oracles, bridges, etc.), not just SEP-41 tokens.
-- Read by stellar-query-api for explorer events name resolution.

CREATE TABLE IF NOT EXISTS contract_registry (
    contract_id     TEXT PRIMARY KEY,
    display_name    TEXT NOT NULL,
    category        TEXT,             -- token, dex, oracle, bridge, dao, nft, game, utility
    project         TEXT,             -- redstone, soroswap, aquarius, blend, phoenix
    icon_url        TEXT,
    website         TEXT,
    verified        BOOLEAN NOT NULL DEFAULT false,
    source          TEXT NOT NULL,    -- token_registry, classification_rule, manual, community
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cr_category ON contract_registry (category);
CREATE INDEX IF NOT EXISTS idx_cr_project ON contract_registry (project);
CREATE INDEX IF NOT EXISTS idx_cr_display_name ON contract_registry USING gin (display_name gin_trgm_ops);

-- Auto-seed from token_registry
INSERT INTO contract_registry (contract_id, display_name, category, project, source)
SELECT contract_id,
       COALESCE(NULLIF(token_symbol, ''), NULLIF(token_name, ''), contract_id),
       'token',
       NULL,
       'token_registry'
FROM token_registry
ON CONFLICT (contract_id) DO NOTHING;

-- Seed known ecosystem contracts
INSERT INTO contract_registry (contract_id, display_name, category, project, verified, source) VALUES
    -- Native XLM SAC (testnet) — computed from Test SDF Network passphrase
    ('CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC', 'Stellar Lumens (XLM)', 'native_asset', 'stellar', true, 'manual'),
    -- Soroswap (mainnet router)
    ('CAG5LRYQ5JVEUI5TEID72EYOVX44TTUJT5BQR2J6J77FH65PCCFAJDDH', 'Soroswap Router', 'dex', 'soroswap', true, 'manual'),
    -- Soroswap (mainnet factory)
    ('CA4HEQTL2WPEUYKYKCDOHCDNIV4QHNJ7EL4J4NQ6VADP7SYHVRYZ7AW2', 'Soroswap Factory', 'dex', 'soroswap', true, 'manual'),
    -- RedStone oracle (testnet)
    ('CD4KFB23CQLJ47RIPESVBTQ444R75M6QVEZULWXHHFYYZT7SS2VGXOPW', 'RedStone Oracle', 'oracle', 'redstone', true, 'manual')
ON CONFLICT (contract_id) DO UPDATE SET
    display_name = EXCLUDED.display_name,
    category = EXCLUDED.category,
    project = EXCLUDED.project,
    verified = EXCLUDED.verified,
    source = EXCLUDED.source,
    updated_at = NOW();

-- Seed well-known tokens in token_registry (symbol/decimals for balance display).
-- The transformer auto-populates this from contract instance metadata, but SAC
-- contracts for native assets don't always have discoverable metadata — the
-- native XLM SAC in particular needs a manual seed.
INSERT INTO token_registry (contract_id, token_name, token_symbol, token_decimals, asset_code, token_type)
VALUES
    ('CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC', 'Stellar Lumens', 'XLM', 7, 'XLM', 'sac')
ON CONFLICT (contract_id) DO UPDATE SET
    token_name = EXCLUDED.token_name,
    token_symbol = EXCLUDED.token_symbol,
    token_decimals = EXCLUDED.token_decimals,
    asset_code = EXCLUDED.asset_code,
    token_type = EXCLUDED.token_type,
    updated_at = NOW();
