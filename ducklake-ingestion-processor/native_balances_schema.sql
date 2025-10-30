-- Native Balances Table - XLM Balance Tracking (Cycle 3)
-- Tracks native XLM balance changes over time
-- Similar structure to account_balances (trustlines) from Cycle 2

CREATE TABLE IF NOT EXISTS native_balances (
    -- ========================================
    -- ACCOUNT IDENTIFICATION
    -- ========================================
    account_id VARCHAR NOT NULL,                    -- Account address

    -- ========================================
    -- BALANCE
    -- ========================================
    balance BIGINT NOT NULL,                        -- XLM balance in stroops

    -- ========================================
    -- LIABILITIES (DEX)
    -- ========================================
    buying_liabilities BIGINT NOT NULL,             -- Outstanding buy obligations
    selling_liabilities BIGINT NOT NULL,            -- Outstanding sell obligations

    -- ========================================
    -- ACCOUNT METADATA
    -- ========================================
    num_subentries INT NOT NULL,                    -- Number of subentries (trustlines, offers, data, etc.)
    num_sponsoring INT NOT NULL,                    -- Number of entries this account sponsors
    num_sponsored INT NOT NULL,                     -- Number of entries sponsored for this account
    sequence_number BIGINT,                         -- Account sequence number

    -- ========================================
    -- LEDGER TRACKING
    -- ========================================
    last_modified_ledger BIGINT NOT NULL,           -- Last ledger where account was modified
    ledger_sequence BIGINT NOT NULL,                -- Ledger where this change occurred
    ledger_range BIGINT                             -- Partition key: (ledger_sequence / 10000) * 10000
);

-- ========================================
-- INDEXES
-- ========================================

-- Account lookups
CREATE INDEX IF NOT EXISTS idx_native_balances_account_id
    ON native_balances(account_id);

-- Ledger queries
CREATE INDEX IF NOT EXISTS idx_native_balances_ledger_sequence
    ON native_balances(ledger_sequence);

-- Time-series queries
CREATE INDEX IF NOT EXISTS idx_native_balances_account_ledger
    ON native_balances(account_id, ledger_sequence DESC);

-- Partition pruning
CREATE INDEX IF NOT EXISTS idx_native_balances_ledger_range
    ON native_balances(ledger_range);

-- ========================================
-- COMMENTS
-- ========================================

COMMENT ON TABLE native_balances IS 'Cycle 3: Native XLM balance tracking over time. Similar to account_balances (trustlines) from Cycle 2 but for native XLM.';

COMMENT ON COLUMN native_balances.balance IS 'XLM balance in stroops (1 XLM = 10,000,000 stroops)';
COMMENT ON COLUMN native_balances.buying_liabilities IS 'Amount reserved for outstanding buy offers on the DEX';
COMMENT ON COLUMN native_balances.selling_liabilities IS 'Amount reserved for outstanding sell offers on the DEX';
COMMENT ON COLUMN native_balances.num_subentries IS 'Count of trustlines, offers, data entries, and claimable balance entries. Affects minimum balance requirement: (2 + num_subentries) * base_reserve';
COMMENT ON COLUMN native_balances.num_sponsoring IS 'Number of ledger entries this account is sponsoring for other accounts';
COMMENT ON COLUMN native_balances.num_sponsored IS 'Number of ledger entries other accounts are sponsoring for this account';

-- ========================================
-- NOTES
-- ========================================

-- 1. Balance Calculation:
--    Available balance = balance - (buying_liabilities + selling_liabilities)
--    Minimum balance = (2 + num_subentries + num_sponsoring - num_sponsored) * base_reserve

-- 2. Differs from account_balances (Cycle 2):
--    - No asset_code, asset_issuer (native XLM only)
--    - No trust_line_limit (not applicable to native balances)
--    - No flags (not applicable to native balances)
--    - Adds num_subentries, num_sponsoring, num_sponsored, sequence_number

-- 3. Data Sources:
--    - AccountEntry from ledger state changes
--    - Tracks account creation, merges, payments, etc.

-- 4. Query Examples:
--    -- Track account balance over time
--    SELECT ledger_sequence, balance, buying_liabilities, selling_liabilities
--    FROM native_balances
--    WHERE account_id = 'GABC...'
--    ORDER BY ledger_sequence DESC;
--
--    -- Find accounts with low balances
--    SELECT account_id, balance, num_subentries
--    FROM native_balances
--    WHERE ledger_sequence = (SELECT MAX(ledger_sequence) FROM native_balances)
--      AND balance < (2 + num_subentries) * 5000000  -- Assuming 0.5 XLM base reserve
--    ORDER BY balance;
