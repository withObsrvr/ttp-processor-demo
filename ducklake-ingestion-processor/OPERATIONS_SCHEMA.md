# Operations Table - Complete Schema Definition

**Cycle 3 - Schema-Complete, Implementation-Incremental**

This document defines the complete operations table schema with ALL fields for ALL 24 operation types. Fields will be populated incrementally across Cycle 3 and future cycles.

## Overview

- **Total Fields**: ~85 fields
- **Common Fields**: 15 (all operations)
- **Type-Specific Fields**: ~70 (varies by operation type)
- **Operation Types**: 24

## Schema Design Principles

1. **Natural Keys**: Use `transaction_hash + operation_index` instead of synthetic IDs
   - Fixes stellar-etl's synthetic ID problem (Discord: esteblock)
   - Reliable JOINs to transactions table

2. **Schema-Complete**: Define ALL fields now to avoid reingestion
   - Parquet immutability requires complete schema upfront
   - NULL fields can be populated in future cycles

3. **Type-Specific Fields**: All operation types share same table
   - Most fields will be NULL for any given operation
   - Better than 24 separate tables (simpler queries, easier to maintain)

---

## Common Fields (15 fields - ALL operations)

```sql
-- Natural Keys (no synthetic IDs!)
transaction_hash VARCHAR NOT NULL,              -- FK to transactions table
operation_index INT NOT NULL,                   -- 0-based index within transaction
PRIMARY KEY (transaction_hash, operation_index),

-- Core Identification
ledger_sequence BIGINT NOT NULL,                -- FK to ledgers table
source_account VARCHAR NOT NULL,                -- Account initiating operation
source_account_muxed VARCHAR,                   -- Muxed account if applicable
type INT NOT NULL,                              -- Operation type enum (0-26)
type_string VARCHAR NOT NULL,                   -- Human-readable type name

-- Timing & Success
created_at TIMESTAMP NOT NULL,                  -- Ledger close time
transaction_successful BOOLEAN NOT NULL,        -- Parent transaction success

-- Result Tracking
operation_result_code VARCHAR,                  -- Result code (opSUCCESS, etc.)
operation_trace_code VARCHAR,                   -- Detailed trace code

-- Partitioning
ledger_range BIGINT,                           -- (ledger_sequence / 10000) * 10000
```

---

## Payment Operations (10 fields)

**Operation Types**: Payment, PathPaymentStrictSend, PathPaymentStrictReceive

```sql
-- Asset being transferred
payment_asset_type VARCHAR,                     -- native, credit_alphanum4, credit_alphanum12
payment_asset_code VARCHAR,                     -- Asset code (null for XLM)
payment_asset_issuer VARCHAR,                   -- Issuer address (null for XLM)

-- Parties
payment_from VARCHAR,                           -- Sender account
payment_from_muxed VARCHAR,                     -- Sender muxed address
payment_to VARCHAR,                             -- Recipient account
payment_to_muxed VARCHAR,                       -- Recipient muxed address

-- Amount
payment_amount BIGINT,                          -- Amount in stroops

-- Path payment specifics
source_asset_type VARCHAR,                      -- Source asset type
source_asset_code VARCHAR,                      -- Source asset code
source_asset_issuer VARCHAR,                    -- Source asset issuer
source_amount BIGINT,                           -- Source amount in stroops
source_max BIGINT,                              -- Max source amount (strict receive)
destination_min BIGINT,                         -- Min destination amount (strict send)
path TEXT,                                      -- JSON array of intermediate assets
```

---

## DEX Operations (12 fields)

**Operation Types**: ManageBuyOffer, ManageSellOffer, CreatePassiveSellOffer

```sql
-- Offer Identification
offer_id BIGINT,                                -- Offer ID (0 for new offers)

-- Selling Asset
selling_asset_type VARCHAR,
selling_asset_code VARCHAR,
selling_asset_issuer VARCHAR,

-- Buying Asset
buying_asset_type VARCHAR,
buying_asset_code VARCHAR,
buying_asset_issuer VARCHAR,

-- Offer Details
offer_amount BIGINT,                            -- Amount offered (stroops)
offer_price_n INT,                              -- Price numerator
offer_price_d INT,                              -- Price denominator
offer_price VARCHAR,                            -- Decimal price representation

-- Offer Type
passive BOOLEAN,                                -- True for passive offers
```

---

## Account Operations (7 fields)

**Operation Types**: CreateAccount, AccountMerge

```sql
-- CreateAccount
starting_balance BIGINT,                        -- Initial XLM balance
funder VARCHAR,                                 -- Account funding the creation
funder_muxed VARCHAR,                           -- Funder muxed address

-- AccountMerge
account VARCHAR,                                -- Account being merged
account_muxed VARCHAR,                          -- Account muxed address
into VARCHAR,                                   -- Destination account
into_muxed VARCHAR,                             -- Destination muxed address
```

---

## Trust Operations (10 fields)

**Operation Types**: ChangeTrust, AllowTrust, SetTrustLineFlags

```sql
-- Asset
trust_asset_type VARCHAR,
trust_asset_code VARCHAR,
trust_asset_issuer VARCHAR,
liquidity_pool_id VARCHAR,                      -- For liquidity pool trustlines

-- Parties
trustor VARCHAR,                                -- Account trusting
trustor_muxed VARCHAR,
trustee VARCHAR,                                -- Asset issuer
trustee_muxed VARCHAR,

-- Limits & Authorization
trust_limit BIGINT,                             -- Trust line limit
authorize BOOLEAN,                              -- Authorization status
authorize_to_maintain_liabilities BOOLEAN,      -- Partial authorization

-- Flags (SetTrustLineFlags)
set_flags INT,
set_flags_s TEXT,                               -- String representation
clear_flags INT,
clear_flags_s TEXT,
```

---

## Claimable Balance Operations (8 fields)

**Operation Types**: CreateClaimableBalance, ClaimClaimableBalance, ClawbackClaimableBalance, Clawback

```sql
-- Balance Identification
balance_id VARCHAR,                             -- Claimable balance ID

-- Asset
cb_asset_type VARCHAR,
cb_asset_code VARCHAR,
cb_asset_issuer VARCHAR,
cb_amount BIGINT,                               -- Amount

-- Claimants
claimant VARCHAR,                               -- Account claiming
claimant_muxed VARCHAR,
claimants TEXT,                                 -- JSON array of claimants (CreateClaimableBalance)
```

---

## Soroban Operations (12 fields)

**Operation Types**: InvokeHostFunction, ExtendFootprintTtl, RestoreFootprint

```sql
-- Contract Invocation
function VARCHAR,                               -- Function type (HostFunctionTypeInvokeContract, etc.)
function_name VARCHAR,                          -- Contract function name
parameters TEXT,                                -- JSON array of parameters
contract_id VARCHAR,                            -- Contract being invoked
contract_code_hash VARCHAR,                     -- Contract WASM hash
contract_code VARCHAR,                          -- Contract WASM code (for uploads)

-- Token transfers (extracted from events)
asset_balance_changes TEXT,                     -- JSON array of balance changes

-- Footprint Management
extend_to INT,                                  -- Extended TTL (ExtendFootprintTtl)
soroban_resources TEXT,                         -- JSON of resource usage

-- Additional fields for future use
soroban_data TEXT,                              -- JSON of additional Soroban data
soroban_auth TEXT,                              -- JSON of authorization entries
```

---

## Set Options (15 fields)

**Operation Type**: SetOptions

```sql
-- Domain & Inflation
home_domain VARCHAR,
inflation_dest VARCHAR,

-- Weights
master_key_weight INT,
low_threshold INT,
med_threshold INT,
high_threshold INT,

-- Signer
signer_key VARCHAR,
signer_weight INT,
signer_type VARCHAR,

-- Flags
set_flags INT,
set_flags_s TEXT,                               -- String representation
clear_flags INT,
clear_flags_s TEXT,
```

---

## Data Operations (3 fields)

**Operation Type**: ManageData

```sql
data_name VARCHAR,                              -- Data entry name
data_value TEXT,                                -- Data entry value (base64)
```

---

## Sequence Operations (2 fields)

**Operation Type**: BumpSequence

```sql
bump_to BIGINT,                                 -- New sequence number
```

---

## Sponsorship Operations (5 fields)

**Operation Types**: BeginSponsoringFutureReserves, EndSponsoringFutureReserves, RevokeSponsorship

```sql
-- BeginSponsoringFutureReserves
sponsored_id VARCHAR,                           -- Account being sponsored

-- EndSponsoringFutureReserves
begin_sponsor VARCHAR,                          -- Sponsoring account
begin_sponsor_muxed VARCHAR,

-- RevokeSponsorship (complex - many subtypes)
revoke_sponsorship_type VARCHAR,               -- Type of sponsorship being revoked
revoke_sponsorship_data TEXT,                   -- JSON of revocation details
```

---

## Liquidity Pool Operations (10 fields)

**Operation Types**: LiquidityPoolDeposit, LiquidityPoolWithdraw

```sql
liquidity_pool_id VARCHAR,                      -- Pool ID

-- Deposit
reserves_max TEXT,                              -- JSON array [amount_a, amount_b]
min_price VARCHAR,
min_price_n INT,
min_price_d INT,
max_price VARCHAR,
max_price_n INT,
max_price_d INT,
reserves_deposited TEXT,                        -- JSON array [deposited_a, deposited_b]
shares_received BIGINT,

-- Withdraw
reserves_min TEXT,                              -- JSON array [min_a, min_b]
shares BIGINT,                                  -- Shares to withdraw
reserves_received TEXT,                         -- JSON array [received_a, received_b]
```

---

## Deprecated Operations (1 field)

**Operation Type**: Inflation (deprecated but still in schema)

```sql
-- No additional fields beyond common fields
```

---

## Complete DDL

**Total Fields**: ~85 fields

See `operations_schema.sql` for the complete CREATE TABLE statement.

---

## Implementation Status

### Cycle 3 (Weeks 1-2)
- [x] Schema definition complete
- [ ] Payment operations (3 types)
- [ ] CreateAccount, AccountMerge
- [ ] ManageBuyOffer, ManageSellOffer
- [ ] ChangeTrust, AllowTrust
- [ ] Native balances extraction

### Cycle 4+ (Future)
- [ ] Claimable balance operations
- [ ] InvokeHostFunction (Soroban)
- [ ] SetOptions
- [ ] LiquidityPool operations
- [ ] Sponsorship operations
- [ ] ManageData, BumpSequence
- [ ] Remaining operation types

---

## Notes

1. **NULL Fields**: Most fields will be NULL for any given operation
   - Only relevant fields are populated based on operation type
   - This is by design - avoids 24 separate tables

2. **Natural Keys**:
   - `transaction_hash + operation_index` = natural primary key
   - No synthetic IDs = reliable JOINs
   - Fixes Discord user complaint about stellar-etl

3. **JSON Fields**:
   - Used for complex structures (arrays, nested objects)
   - Can be parsed/indexed in future cycles if needed
   - Keeps schema manageable

4. **Incremental Implementation**:
   - Schema is complete NOW
   - Extraction logic added incrementally
   - No reingestion needed for new operation types

---

## References

- Stellar Horizon API: https://developers.stellar.org/docs/data/apis/horizon/api-reference/resources/operations
- Stellar ETL: https://github.com/stellar/stellar-etl
- Stellar Go SDK: https://github.com/stellar/go
- Discord User Feedback: esteblock (Soroswap), Jake (SDF/Freighter)
