-- Operations Table - Complete Schema (Cycle 3)
-- Schema-Complete, Implementation-Incremental approach
-- Natural keys (transaction_hash + operation_index) fix synthetic ID problems

CREATE TABLE IF NOT EXISTS operations (
    -- ========================================
    -- NATURAL KEYS (15 fields)
    -- ========================================
    transaction_hash VARCHAR NOT NULL,              -- FK to transactions table
    operation_index INT NOT NULL,                   -- 0-based index within transaction

    ledger_sequence BIGINT NOT NULL,                -- FK to ledgers table
    source_account VARCHAR NOT NULL,                -- Account initiating operation
    source_account_muxed VARCHAR,                   -- Muxed account if applicable
    type INT NOT NULL,                              -- Operation type enum (0-26)
    type_string VARCHAR NOT NULL,                   -- Human-readable type name
    created_at TIMESTAMP NOT NULL,                  -- Ledger close time
    transaction_successful BOOLEAN NOT NULL,        -- Parent transaction success
    operation_result_code VARCHAR,                  -- Result code (opSUCCESS, etc.)
    operation_trace_code VARCHAR,                   -- Detailed trace code
    ledger_range BIGINT,                           -- Partition key

    -- ========================================
    -- PAYMENT OPERATIONS (14 fields)
    -- Payment, PathPaymentStrictSend, PathPaymentStrictReceive
    -- ========================================
    payment_asset_type VARCHAR,
    payment_asset_code VARCHAR,
    payment_asset_issuer VARCHAR,
    payment_from VARCHAR,
    payment_from_muxed VARCHAR,
    payment_to VARCHAR,
    payment_to_muxed VARCHAR,
    payment_amount BIGINT,
    source_asset_type VARCHAR,
    source_asset_code VARCHAR,
    source_asset_issuer VARCHAR,
    source_amount BIGINT,
    source_max BIGINT,
    destination_min BIGINT,
    path TEXT,                                      -- JSON array

    -- ========================================
    -- DEX OPERATIONS (12 fields)
    -- ManageBuyOffer, ManageSellOffer, CreatePassiveSellOffer
    -- ========================================
    offer_id BIGINT,
    selling_asset_type VARCHAR,
    selling_asset_code VARCHAR,
    selling_asset_issuer VARCHAR,
    buying_asset_type VARCHAR,
    buying_asset_code VARCHAR,
    buying_asset_issuer VARCHAR,
    offer_amount BIGINT,
    offer_price_n INT,
    offer_price_d INT,
    offer_price VARCHAR,
    passive BOOLEAN,

    -- ========================================
    -- ACCOUNT OPERATIONS (7 fields)
    -- CreateAccount, AccountMerge
    -- ========================================
    starting_balance BIGINT,
    funder VARCHAR,
    funder_muxed VARCHAR,
    account VARCHAR,
    account_muxed VARCHAR,
    into VARCHAR,
    into_muxed VARCHAR,

    -- ========================================
    -- TRUST OPERATIONS (10 fields)
    -- ChangeTrust, AllowTrust, SetTrustLineFlags
    -- ========================================
    trust_asset_type VARCHAR,
    trust_asset_code VARCHAR,
    trust_asset_issuer VARCHAR,
    trustor VARCHAR,
    trustor_muxed VARCHAR,
    trustee VARCHAR,
    trustee_muxed VARCHAR,
    trust_limit BIGINT,
    authorize BOOLEAN,
    authorize_to_maintain_liabilities BOOLEAN,
    set_flags INT,
    set_flags_s TEXT,
    clear_flags INT,
    clear_flags_s TEXT,

    -- ========================================
    -- CLAIMABLE BALANCE OPERATIONS (8 fields)
    -- CreateClaimableBalance, ClaimClaimableBalance,
    -- ClawbackClaimableBalance, Clawback
    -- ========================================
    balance_id VARCHAR,
    cb_asset_type VARCHAR,
    cb_asset_code VARCHAR,
    cb_asset_issuer VARCHAR,
    cb_amount BIGINT,
    claimant VARCHAR,
    claimant_muxed VARCHAR,
    claimants TEXT,                                 -- JSON array

    -- ========================================
    -- SOROBAN OPERATIONS (12 fields)
    -- InvokeHostFunction, ExtendFootprintTtl, RestoreFootprint
    -- ========================================
    function VARCHAR,
    function_name VARCHAR,
    parameters TEXT,                                -- JSON array
    contract_id VARCHAR,
    contract_code_hash VARCHAR,
    contract_code VARCHAR,
    asset_balance_changes TEXT,                     -- JSON array
    extend_to INT,
    soroban_resources TEXT,                         -- JSON
    soroban_data TEXT,                              -- JSON
    soroban_auth TEXT,                              -- JSON

    -- ========================================
    -- SET OPTIONS (15 fields)
    -- SetOptions
    -- ========================================
    home_domain VARCHAR,
    inflation_dest VARCHAR,
    master_key_weight INT,
    low_threshold INT,
    med_threshold INT,
    high_threshold INT,
    signer_key VARCHAR,
    signer_weight INT,
    signer_type VARCHAR,

    -- ========================================
    -- DATA OPERATIONS (2 fields)
    -- ManageData
    -- ========================================
    data_name VARCHAR,
    data_value TEXT,

    -- ========================================
    -- SEQUENCE OPERATIONS (1 field)
    -- BumpSequence
    -- ========================================
    bump_to BIGINT,

    -- ========================================
    -- SPONSORSHIP OPERATIONS (5 fields)
    -- BeginSponsoringFutureReserves,
    -- EndSponsoringFutureReserves, RevokeSponsorship
    -- ========================================
    sponsored_id VARCHAR,
    begin_sponsor VARCHAR,
    begin_sponsor_muxed VARCHAR,
    revoke_sponsorship_type VARCHAR,
    revoke_sponsorship_data TEXT,                   -- JSON

    -- ========================================
    -- LIQUIDITY POOL OPERATIONS (13 fields)
    -- LiquidityPoolDeposit, LiquidityPoolWithdraw
    -- ========================================
    liquidity_pool_id VARCHAR,
    reserves_max TEXT,                              -- JSON array
    min_price VARCHAR,
    min_price_n INT,
    min_price_d INT,
    max_price VARCHAR,
    max_price_n INT,
    max_price_d INT,
    reserves_deposited TEXT,                        -- JSON array
    shares_received BIGINT,
    reserves_min TEXT,                              -- JSON array
    shares BIGINT,
    reserves_received TEXT                          -- JSON array
);

-- ========================================
-- INDEXES (Basic - optimize later)
-- ========================================

-- Primary key
-- Note: Will be added during table creation in Go code

-- Foreign keys
CREATE INDEX IF NOT EXISTS idx_operations_transaction_hash
    ON operations(transaction_hash);

CREATE INDEX IF NOT EXISTS idx_operations_ledger_sequence
    ON operations(ledger_sequence);

-- Query optimization
CREATE INDEX IF NOT EXISTS idx_operations_type
    ON operations(type_string);

CREATE INDEX IF NOT EXISTS idx_operations_source_account
    ON operations(source_account);

-- Payment queries
CREATE INDEX IF NOT EXISTS idx_operations_payment_to
    ON operations(payment_to) WHERE payment_to IS NOT NULL;

-- Partition pruning
CREATE INDEX IF NOT EXISTS idx_operations_ledger_range
    ON operations(ledger_range);

-- ========================================
-- COMMENTS
-- ========================================

COMMENT ON TABLE operations IS 'Cycle 3: Operations table with complete schema for all 24 operation types. Uses natural keys (transaction_hash + operation_index) to avoid synthetic ID problems. Schema is complete; extraction logic implemented incrementally.';

COMMENT ON COLUMN operations.transaction_hash IS 'Natural FK to transactions table. Fixes stellar-etl synthetic ID problem reported by esteblock.';
COMMENT ON COLUMN operations.operation_index IS 'Zero-based index of operation within transaction. Together with transaction_hash forms natural primary key.';
COMMENT ON COLUMN operations.type IS 'Operation type enum: 0=CreateAccount, 1=Payment, 2=PathPaymentStrictReceive, 3=ManageSellOffer, 4=CreatePassiveSellOffer, 5=SetOptions, 6=ChangeTrust, 7=AllowTrust, 8=AccountMerge, 9=Inflation, 10=ManageData, 11=BumpSequence, 12=ManageBuyOffer, 13=PathPaymentStrictSend, 14=CreateClaimableBalance, 15=ClaimClaimableBalance, 16=BeginSponsoringFutureReserves, 17=EndSponsoringFutureReserves, 18=RevokeSponsorship, 19=Clawback, 20=ClawbackClaimableBalance, 21=SetTrustLineFlags, 22=LiquidityPoolDeposit, 23=LiquidityPoolWithdraw, 24=InvokeHostFunction, 25=ExtendFootprintTtl, 26=RestoreFootprint';

-- ========================================
-- NOTES
-- ========================================

-- 1. Most fields will be NULL for any given operation
--    Only relevant fields are populated based on operation type

-- 2. JSON fields used for complex structures:
--    - path, claimants, parameters, asset_balance_changes
--    - reserves_max, reserves_deposited, reserves_min, reserves_received
--    - soroban_resources, soroban_data, soroban_auth
--    - revoke_sponsorship_data

-- 3. Implementation status:
--    Cycle 3 (Weeks 1-2):
--      [x] Schema complete
--      [ ] Payment operations (3 types)
--      [ ] CreateAccount, AccountMerge
--      [ ] ManageBuyOffer, ManageSellOffer
--      [ ] ChangeTrust, AllowTrust
--
--    Cycle 4+ (Future):
--      [ ] Remaining operation types

-- 4. Total fields: ~103 fields
--    - 12 common fields
--    - 91 type-specific fields
