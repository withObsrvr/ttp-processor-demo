# Pitch: Transactions + Account Balances Tables

**Status**: Cycle 2 Proposal
**Appetite**: 1 week
**Author**: Solo Dev
**Date**: 2025-10-28
**Updated**: 2025-10-28 (Added complete stellar-etl alignment)

---

## 🚨 CRITICAL PREREQUISITE: Ledger Table Schema Update

**BEFORE starting Cycle 2**, the ledger table must be updated with missing fields from stellar-etl.

**Why this matters**:
- Parquet files are immutable - can't add fields later without full reingestion
- Discord user pain: Missing fields break joins and analytics
- Better to capture everything NOW than reingest millions of ledgers later

**Current ledger table**: 14 fields
**Stellar-etl ledger table**: 24 fields
**Missing**: 10 critical fields (operation counts, Soroban metrics, Protocol 23 features)

**See**: `LEDGER_TABLE_UPDATE.md` for complete plan (must do before Cycle 2)

---

## Problem

Right now, ducklake-ingestion only stores **ledger-level metadata** (14 fields, 1 table). I can't answer fundamental questions like:

- "Show me all transactions in ledger 500000"
- "Which accounts changed balances in the last 100 ledgers?"
- "What was the fee charged for transaction ABC?"
- "How has account X's USDC balance changed over time?"
- "What was the memo on this payment?" ← **Real user need (Discord)**
- "Join operations to transactions by hash" ← **Broken in Dune (stellar-etl issue)**

**Without these tables**, the data warehouse is incomplete - I have ledger summaries but no transaction details or balance tracking.

**Why now?**
- Cycle 1 proved the architecture works (ledger ingestion is stable)
- We have working reference code from account-balance-processor
- These are the most fundamental tables after ledgers
- **Discord validation**: Users need these fields for real-world analytics

---

## Appetite

**1 week** (5 working days)

**Why this timeframe?**
- Transaction extraction is straightforward (we have the pattern)
- Balance extraction is proven (account-balance-processor does it)
- Can reuse patterns from Cycle 1 (same buffering, same DuckLake writer)
- Additional fields are simple struct accesses (already in SDK output)
- Small enough to ship confidently, big enough to add real value

**If it takes longer**: Cut scope, not time. Ship what works.

---

## Solution (Fat-Marker Sketch)

### Core Approach

```
LedgerCloseMeta (from Cycle 1)
    ↓
NEW: extractTransactionsAndBalances(lcm)
    ├─ Use ingest.NewLedgerTransactionReaderFromLedgerCloseMeta()  ← From extraction analysis
    ├─ For each transaction:
    │   ├─ Extract transaction data (13 fields) → transactions buffer
    │   └─ Extract balance changes (10 fields) → account_balances buffer
    ↓
Write both tables to DuckLake (same flush pattern as Cycle 1)
```

### Table 1: Transactions (13 fields - stellar-etl aligned)

**Schema** (updated for Parquet immutability):
```sql
CREATE TABLE IF NOT EXISTS transactions (
    -- Core identity (MUST HAVE)
    ledger_sequence BIGINT NOT NULL,         -- FK to ledgers
    transaction_hash VARCHAR PRIMARY KEY,    -- Natural key (better than stellar-etl's synthetic ID)
    source_account VARCHAR NOT NULL,

    -- Fees & success (MUST HAVE)
    fee_charged BIGINT NOT NULL,
    max_fee BIGINT NOT NULL,                 -- Fee bid (compare to actual)
    successful BOOLEAN NOT NULL,
    transaction_result_code VARCHAR,         -- Error analysis

    -- Operations (MUST HAVE)
    operation_count INT NOT NULL,

    -- Timing (MUST HAVE)
    created_at TIMESTAMP NOT NULL,           -- From ledger.closed_at

    -- Account tracking (MUST HAVE)
    account_sequence BIGINT NOT NULL,        -- Natural ordering, deduplication

    -- Memos (HIGH VALUE - can't add later)
    memo_type VARCHAR,                       -- TEXT, HASH, ID, RETURN, NONE
    memo TEXT,                               -- Base64 for HASH, decoded for TEXT

    -- Partitioning (MUST HAVE)
    ledger_range BIGINT                      -- Partition key (same as ledgers)
)
```

**Why these 13 fields?**
- ✅ All present in stellar-etl (validated by production usage)
- ✅ Addresses Discord user pain (memo queries, reliable joins)
- ✅ All easily accessible via Stellar SDK
- ✅ Can't be added later (Parquet immutability)
- ✅ Natural `transaction_hash` PK avoids stellar-etl's synthetic ID join issues

**Added from original 8-field pitch**:
1. `max_fee` - Fee analysis (Discord: inclusion_fee_bid issues)
2. `account_sequence` - Ordering, deduplication
3. `transaction_result_code` - Error analysis
4. `memo_type` + `memo` - Very common queries, can't add later

### Table 2: Account Balances (10 fields - stellar-etl aligned)

**Schema** (updated for Parquet immutability):
```sql
CREATE TABLE IF NOT EXISTS account_balances (
    -- Account & asset (MUST HAVE)
    account_id VARCHAR NOT NULL,
    asset_code VARCHAR NOT NULL,
    asset_issuer VARCHAR NOT NULL,

    -- Balance (MUST HAVE)
    balance BIGINT NOT NULL,                 -- In stroops/atomic units (more precise than stellar-etl's float64)

    -- Trustline limits & liabilities (HIGH VALUE)
    trust_line_limit BIGINT NOT NULL,        -- Maximum trustable amount
    buying_liabilities BIGINT NOT NULL,      -- Outstanding buy obligations (DEX)
    selling_liabilities BIGINT NOT NULL,     -- Outstanding sell obligations (DEX)

    -- Authorization & tracking (MUST HAVE)
    flags INT NOT NULL,                      -- Authorization flags
    last_modified_ledger BIGINT NOT NULL,
    ledger_sequence BIGINT NOT NULL,         -- When this change occurred

    -- Partitioning (MUST HAVE)
    ledger_range BIGINT                      -- Partition key
)
```

**Why account_balances?**
- ✅ We have working code from account-balance-processor
- ✅ Uses `trustline.TransformTrustline()` - battle-tested
- ✅ High value for analytics (track balance changes over time)
- ✅ Natural fit with transactions (transactions cause balance changes)
- ✅ **All 10 fields already in SDK output** - just store them!

**Added from original 7-field pitch**:
1. `trust_line_limit` - Risk analysis
2. `buying_liabilities` + `selling_liabilities` - DEX analytics
3. `flags` - Authorization status

---

## Implementation Steps

### Day 1-2: Transaction Extraction (13 fields)

**Add to main.go**:
```go
type TransactionData struct {
    LedgerSequence         uint32
    Hash                   string
    SourceAccount          string
    FeeCharged             int64
    MaxFee                 int64
    Successful             bool
    TransactionResultCode  string
    OperationCount         int32
    CreatedAt              time.Time
    AccountSequence        int64
    MemoType               string
    Memo                   string
    LedgerRange            uint32
}

func (ing *Ingester) extractTransactions(lcm *xdr.LedgerCloseMeta, ledgerSeq uint32) ([]TransactionData, error) {
    txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(ing.networkPassphrase, *lcm)
    if err != nil {
        return nil, err
    }
    defer txReader.Close()

    header := lcm.LedgerHeaderHistoryEntry()
    var transactions []TransactionData

    for {
        tx, err := txReader.Read()
        if err == io.EOF {
            break
        }
        if err != nil {
            return nil, err
        }

        txData := TransactionData{
            LedgerSequence:        ledgerSeq,
            Hash:                  tx.Result.TransactionHash.HexString(),
            SourceAccount:         tx.Envelope.SourceAccount().ToAccountId().Address(),
            FeeCharged:            int64(tx.Result.FeeCharged),
            MaxFee:                int64(tx.Envelope.Fee()),
            Successful:            tx.Result.Successful(),
            TransactionResultCode: tx.Result.Result.Code.String(),
            OperationCount:        int32(len(tx.Envelope.Operations())),
            CreatedAt:             time.Unix(int64(header.Header.ScpValue.CloseTime), 0),
            AccountSequence:       int64(tx.Envelope.SeqNum()),
            MemoType:              tx.Envelope.Memo().Type.String(),
            Memo:                  extractMemoValue(tx.Envelope.Memo()),
            LedgerRange:           (ledgerSeq / 10000) * 10000,
        }

        transactions = append(transactions, txData)
    }

    return transactions, nil
}

func extractMemoValue(memo xdr.Memo) string {
    switch memo.Type {
    case xdr.MemoTypeMemoText:
        return string(*memo.Text)
    case xdr.MemoTypeMemoId:
        return strconv.FormatUint(uint64(*memo.Id), 10)
    case xdr.MemoTypeMemoHash:
        hash := *memo.Hash
        return base64.StdEncoding.EncodeToString(hash[:])
    case xdr.MemoTypeMemoReturn:
        ret := *memo.RetHash
        return base64.StdEncoding.EncodeToString(ret[:])
    default:
        return ""
    }
}
```

### Day 3-4: Balance Extraction (10 fields)

**Add to main.go**:
```go
type AccountBalanceData struct {
    AccountID          string
    AssetCode          string
    AssetIssuer        string
    Balance            int64
    TrustLineLimit     int64
    BuyingLiabilities  int64
    SellingLiabilities int64
    Flags              uint32
    LastModifiedLedger uint32
    LedgerSequence     uint32
    LedgerRange        uint32
}

func (ing *Ingester) extractBalances(lcm *xdr.LedgerCloseMeta, ledgerSeq uint32) ([]AccountBalanceData, error) {
    txReader, _ := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(ing.networkPassphrase, *lcm)
    defer txReader.Close()

    header := lcm.LedgerHeaderHistoryEntry()
    var balances []AccountBalanceData

    for {
        tx, err := txReader.Read()
        if err == io.EOF {
            break
        }

        changes, err := tx.GetChanges()
        if err != nil {
            continue
        }

        for _, change := range changes {
            if change.Type != xdr.LedgerEntryTypeTrustline {
                continue // Only balance changes for now
            }

            // Use Stellar SDK helper (from account-balance-processor)
            trustlineOutput, err := trustline.TransformTrustline(change, header)
            if err != nil {
                continue
            }

            balance := AccountBalanceData{
                AccountID:          trustlineOutput.AccountID,
                AssetCode:          trustlineOutput.AssetCode,
                AssetIssuer:        trustlineOutput.AssetIssuer,
                Balance:            int64(trustlineOutput.Balance),
                TrustLineLimit:     trustlineOutput.Limit,
                BuyingLiabilities:  int64(trustlineOutput.BuyingLiabilities),
                SellingLiabilities: int64(trustlineOutput.SellingLiabilities),
                Flags:              trustlineOutput.Flags,
                LastModifiedLedger: trustlineOutput.LastModifiedLedger,
                LedgerSequence:     ledgerSeq,
                LedgerRange:        (ledgerSeq / 10000) * 10000,
            }

            balances = append(balances, balance)
        }
    }

    return balances, nil
}
```

### Day 5: Create Tables & Test

**Create table SQL**:
```sql
-- Add to setupTables()
CREATE TABLE IF NOT EXISTS transactions (
    ledger_sequence BIGINT NOT NULL,
    transaction_hash VARCHAR PRIMARY KEY,
    source_account VARCHAR NOT NULL,
    fee_charged BIGINT NOT NULL,
    max_fee BIGINT NOT NULL,
    successful BOOLEAN NOT NULL,
    transaction_result_code VARCHAR,
    operation_count INT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    account_sequence BIGINT NOT NULL,
    memo_type VARCHAR,
    memo TEXT,
    ledger_range BIGINT
);

CREATE TABLE IF NOT EXISTS account_balances (
    account_id VARCHAR NOT NULL,
    asset_code VARCHAR NOT NULL,
    asset_issuer VARCHAR NOT NULL,
    balance BIGINT NOT NULL,
    trust_line_limit BIGINT NOT NULL,
    buying_liabilities BIGINT NOT NULL,
    selling_liabilities BIGINT NOT NULL,
    flags INT NOT NULL,
    last_modified_ledger BIGINT NOT NULL,
    ledger_sequence BIGINT NOT NULL,
    ledger_range BIGINT
);
```

**Update flush()**:
```go
func (ing *Ingester) flush() error {
    tx, _ := ing.db.Begin()
    defer tx.Rollback()

    // Insert ledgers (existing)
    for _, ledger := range ing.ledgerBuffer {
        _, err := tx.Stmt(ing.ledgerInsertStmt).Exec(...)
    }

    // Insert transactions (NEW)
    for _, txData := range ing.txBuffer {
        _, err := tx.Stmt(ing.txInsertStmt).Exec(
            txData.LedgerSequence,
            txData.Hash,
            txData.SourceAccount,
            txData.FeeCharged,
            txData.MaxFee,
            txData.Successful,
            txData.TransactionResultCode,
            txData.OperationCount,
            txData.CreatedAt,
            txData.AccountSequence,
            txData.MemoType,
            txData.Memo,
            txData.LedgerRange,
        )
        if err != nil {
            return err
        }
    }

    // Insert balances (NEW)
    for _, balance := range ing.balanceBuffer {
        _, err := tx.Stmt(ing.balanceInsertStmt).Exec(
            balance.AccountID,
            balance.AssetCode,
            balance.AssetIssuer,
            balance.Balance,
            balance.TrustLineLimit,
            balance.BuyingLiabilities,
            balance.SellingLiabilities,
            balance.Flags,
            balance.LastModifiedLedger,
            balance.LedgerSequence,
            balance.LedgerRange,
        )
        if err != nil {
            return err
        }
    }

    tx.Commit()

    // Clear buffers
    ing.ledgerBuffer = ing.ledgerBuffer[:0]
    ing.txBuffer = ing.txBuffer[:0]
    ing.balanceBuffer = ing.balanceBuffer[:0]

    return nil
}
```

**Test with 100 ledgers**:
```bash
./ducklake-ingestion --config config/testnet.yaml --start-ledger 500000
```

---

## Rabbit Holes

**DON'T**:
- ❌ Add operations table yet (that's Cycle 3)
- ❌ Handle native XLM balances yet (trustlines only for now)
- ❌ Add Soroban transaction fields (resource_fee, etc.) - Protocol 20+ complexity
- ❌ Add fee bump fields (inner_transaction_hash, fee_account) - edge case
- ❌ Add raw XDR blobs (tx_envelope, tx_result) - storage bloat
- ❌ Add indexes or optimization (premature)
- ❌ Build separate processor files (keep in main.go for now)

**If you find yourself**:
- Spending > 1 day on memo parsing → Store as base64, decode later
- Debugging XDR edge cases → Use SDK helpers, don't parse raw XDR
- Creating separate processor modules → Keep inline for Cycle 2
- Adding more fields → Stop at 13 transactions + 10 balances

---

## No-Gos

**Explicitly OUT of scope for Cycle 2**:
- Operations table (Cycle 3)
- Native XLM account balances (trustlines only)
- Soroban transaction fields (Cycle 3+)
- Fee bump transaction support (inner_transaction_hash, fee_account)
- Preconditions (min_account_sequence, etc.) - CAP-21 complexity
- Contract invocations (Cycle 3)
- Ledger changes table (Cycle 3+)
- Performance optimization
- Separate processor modules (keep simple)
- Multiple network support (testnet only)

---

## Done Looks Like

**I can run these queries**:

```sql
-- Query 1: Count transactions per ledger
SELECT
    ledger_sequence,
    COUNT(*) as tx_count,
    SUM(CASE WHEN successful THEN 1 ELSE 0 END) as successful_count,
    SUM(fee_charged) as total_fees
FROM transactions
WHERE ledger_sequence BETWEEN 500000 AND 500010
GROUP BY ledger_sequence;

-- Query 2: Find high-fee transactions
SELECT
    transaction_hash,
    source_account,
    max_fee,
    fee_charged,
    (max_fee - fee_charged) as fee_savings,
    successful
FROM transactions
WHERE fee_charged > 1000000
ORDER BY fee_charged DESC
LIMIT 20;

-- Query 3: Track account balance changes
SELECT
    ledger_sequence,
    account_id,
    asset_code,
    balance,
    trust_line_limit,
    flags
FROM account_balances
WHERE account_id = 'GABC...'
  AND asset_code = 'USDC'
ORDER BY ledger_sequence DESC
LIMIT 100;

-- Query 4: JOIN ledgers with transactions (validates stellar-etl alignment)
SELECT
    l.sequence,
    l.closed_at,
    COUNT(t.transaction_hash) as tx_count,
    SUM(t.fee_charged) as total_fees,
    AVG(t.operation_count) as avg_ops_per_tx
FROM ledgers l
JOIN transactions t ON l.sequence = t.ledger_sequence
WHERE l.sequence BETWEEN 500000 AND 500100
GROUP BY l.sequence, l.closed_at;

-- Query 5: Verify data integrity
SELECT
    l.sequence,
    l.successful_tx_count,
    COUNT(t.transaction_hash) as actual_tx_count
FROM ledgers l
LEFT JOIN transactions t ON l.sequence = t.ledger_sequence
WHERE l.sequence BETWEEN 500000 AND 500100
GROUP BY l.sequence, l.successful_tx_count
HAVING l.successful_tx_count != COUNT(t.transaction_hash);
-- Should return 0 rows (no mismatches)

-- Query 6: Memo search (Discord user need)
SELECT
    transaction_hash,
    source_account,
    memo_type,
    memo,
    created_at
FROM transactions
WHERE memo_type = 'MEMO_TEXT'
  AND memo LIKE '%payment%'
ORDER BY created_at DESC
LIMIT 50;

-- Query 7: DEX liability analysis (uses new fields)
SELECT
    account_id,
    asset_code,
    balance,
    buying_liabilities,
    selling_liabilities,
    trust_line_limit,
    (trust_line_limit - balance - buying_liabilities) as available_credit
FROM account_balances
WHERE buying_liabilities > 0 OR selling_liabilities > 0
ORDER BY (buying_liabilities + selling_liabilities) DESC
LIMIT 100;
```

**Success demo**:
- Process 100 ledgers from testnet
- All 3 tables populated (ledgers, transactions, account_balances)
- Transaction counts match ledger metadata
- Balance changes captured for all trustline updates
- JOINs work correctly
- Data visible within 30 seconds
- **Memo queries work** (Discord validation)
- **Fee analysis works** (max_fee vs fee_charged)

**Not done** unless:
- ✅ Can query transactions independently
- ✅ Can query account balance history
- ✅ Can JOIN ledgers ↔ transactions
- ✅ Transaction counts match ledger.successful_tx_count
- ✅ Existing ledger ingestion still works
- ✅ No duplicate transactions or balances
- ✅ Memo queries return results
- ✅ All 13 transaction fields populated
- ✅ All 10 balance fields populated

---

## Scope Line

```
CUT FIRST IF RUNNING LATE:
────────────────────────────
- Memo fields (just store NULL)          ← 2 fields
- transaction_result_code                ← 1 field
- Native XLM balances (trustlines only)

TRY TO INCLUDE:
───────────────
- Memo (memo_type, memo)                 ← High Discord value
- max_fee                                ← Fee analysis
- account_sequence                       ← Ordering
- Liabilities (buying, selling)          ← DEX analytics
- trust_line_limit                       ← Risk analysis
- flags                                  ← Authorization status

MUST HAVE (NON-NEGOTIABLE):
═══════════════════════════
Transactions (9 core fields):
- ledger_sequence (FK)
- transaction_hash (PK)
- source_account
- fee_charged
- successful
- operation_count
- created_at
- account_sequence
- ledger_range

Account Balances (7 core fields):
- account_id
- asset_code, asset_issuer
- balance
- last_modified_ledger
- ledger_sequence
- ledger_range
```

---

## Open Questions

**Q**: Should we also extract native XLM balances (not just trustlines)?
**A**: No - trustlines only for Cycle 2. Native balances in Cycle 3 with full account processor.

**Q**: Should transactions and balances use separate buffers?
**A**: Yes - keeps code clean and allows independent flushing if needed.

**Q**: What if transaction parsing fails mid-ledger?
**A**: Fail the whole ledger (same stop-and-alert behavior as Cycle 1).

**Q**: Should we add indexes now?
**A**: Basic indexes on table creation (primary keys). Optimization indexes in Cycle 4+.

**Q**: Why 13 transaction fields instead of stellar-etl's 45+?
**A**: Capturing the high-value 30%. We can add more in Cycle 3+ based on actual usage, but these 13 can't be added later due to Parquet immutability.

**Q**: Are the additional 5 fields (from 8 to 13) worth the risk?
**A**: YES - they're all simple struct accesses already in SDK output. 1 extra day of work vs months of reingestion pain.

---

## Why These Two Tables?

### Transactions Table

**Value**:
- ✅ Fundamental building block (operations, payments reference it)
- ✅ Enables transaction-level queries
- ✅ High analytics value (fee analysis, success rates, etc.)
- ✅ **Natural key** (transaction_hash) avoids stellar-etl's synthetic ID join bugs (Discord issue)
- ✅ Memo support enables payment tracking

**Feasibility**:
- ✅ We have the code pattern (account-balance-processor)
- ✅ Stellar SDK makes it easy (`txReader.Read()`)
- ✅ All 13 fields are simple struct accesses
- ✅ Proven by stellar-etl in production

**Risk**: Low - proven pattern, clear requirements, validated by real users

---

### Account Balances Table

**Value**:
- ✅ Track how balances change over time
- ✅ Essential for financial analytics
- ✅ Shows real-world utility (not just metadata)
- ✅ DEX analytics (liabilities tracking)
- ✅ Risk analysis (trust_line_limit)

**Feasibility**:
- ✅ Working code exists (account-balance-processor)
- ✅ Uses battle-tested SDK (`trustline.TransformTrustline`)
- ✅ **All 10 fields already in SDK output** - no extra work!
- ✅ Proven by stellar-etl in production

**Risk**: Low - we literally have working code to copy, just storing more fields

---

## Why NOT Other Tables for Cycle 2?

### ❌ Operations Table
- **Too complex**: 15+ operation types, each with different fields
- **High risk**: Could derail the cycle if we hit edge cases
- **Better for Cycle 3**: After we have transaction foundation

### ❌ Ledger Changes Table
- **Lower value**: More technical, less business value
- **Complex**: Pre/post state comparisons
- **Better for Cycle 3+**: Advanced feature

### ❌ Contract Invocations Table
- **Lower priority**: Only applies to Soroban contracts
- **Complex**: Requires XDR decoding of ScVals
- **Better for Cycle 3**: Dedicated Soroban cycle

---

## Dependencies Needed

```bash
cd ducklake-ingestion-processor/go

# Add Stellar SDK ingest package
go get github.com/stellar/go/ingest

# Add Stellar SDK trustline processor
go get github.com/stellar/go/processors/trustline

# Update go.mod
go mod tidy
```

**Already have**:
- ✅ `github.com/stellar/go/xdr` (Cycle 1)
- ✅ DuckDB driver (Cycle 1)
- ✅ gRPC client (Cycle 1)

---

## Betting Criteria

**Bet on this if**:
- ✅ Need transaction-level data for analytics
- ✅ Want to track account balance history
- ✅ Have 1 week available
- ✅ Excited to build on Cycle 1 success
- ✅ Want to avoid reingestion pain (Parquet immutability)
- ✅ Want stellar-etl alignment for ecosystem compatibility

**Don't bet on this if**:
- ❌ Cycle 1 ledger table needs update first (see LEDGER_TABLE_UPDATE.md)
- ❌ Need operations table more urgently
- ❌ Feeling burned out (extend cool-down)
- ❌ Want to focus on Soroban contracts first

---

## Alternative Bets (Compete with This)

### Option B: "Just Transactions" (Safer)
- **Appetite**: 3 days
- **Scope**: Only transactions table (no balances)
- **Pro**: Lower risk, guaranteed to ship
- **Con**: Less value, doesn't leverage account-balance-processor code

### Option C: "Transactions + Operations" (Riskier)
- **Appetite**: 2 weeks
- **Scope**: Both tables
- **Pro**: More complete
- **Con**: Operations are complex (15+ types), higher risk

### Option D: "Ledger Table Update Only" (Prerequisite)
- **Appetite**: 2 days
- **Scope**: Update ledger table with missing 10 fields
- **Pro**: Unblocks Cycle 2, prevents reingestion
- **Con**: No new tables (see LEDGER_TABLE_UPDATE.md)

### Option E: "Nothing" (Extended Cool-Down)
- **Appetite**: 1 week
- **Scope**: Fix Cycle 1 issues, explore, rest
- **Pro**: Safe, gives time to reflect
- **Con**: No new features

---

## Hill Chart Milestones

**Left Side (Figuring Out)**:
- Get one transaction extracted with all 13 fields ← Checkpoint 1
- Get one balance change extracted with all 10 fields ← Checkpoint 2
- Memo parsing working ← Checkpoint 3

**Right Side (Making It Happen)**:
- Process 100 ledgers successfully ← Checkpoint 4
- All queries working ← Ship!

**If stuck on left side at 50% time**:
- Cut memo fields immediately (2 fields)
- Cut transaction_result_code (1 field)
- Cut liabilities + trust_line_limit (3 fields)
- Ship with 9 transaction fields + 7 balance fields minimum

---

## Success Metrics

### Functional
- ✅ 2 new tables created and populated
- ✅ Transaction count matches ledger metadata
- ✅ Balance changes captured for all trustlines
- ✅ JOINs work (ledgers ↔ transactions)
- ✅ All 13 transaction fields populated (no NULLs except memo)
- ✅ All 10 balance fields populated
- ✅ Memo queries return results

### Performance
- ✅ Still processing ≥ 10 ledgers/sec
- ✅ Memory usage < 1GB
- ✅ Flush time < 100ms (even with 2 extra tables)

### Quality
- ✅ No duplicate records
- ✅ Referential integrity maintained
- ✅ Data freshness < 30 seconds
- ✅ Stellar-etl schema alignment verified

---

## Comparison to Stellar ETL

### Transactions Schema Comparison

| Category | Stellar ETL | Our Cycle 2 | Status |
|----------|-------------|-------------|--------|
| **Core fields** | 8 | 13 | ✅ Aligned |
| **Memos** | 2 (memo_type, memo) | 2 (memo_type, memo) | ✅ Full match |
| **Fees** | 3 (max_fee, fee_charged, inclusion_fee_bid) | 2 (max_fee, fee_charged) | ⚠️ Missing Soroban fees |
| **Success tracking** | 2 (successful, result_code) | 2 (successful, result_code) | ✅ Full match |
| **Fee bumps** | 4 (fee_account, inner_hash, etc.) | 0 | ❌ Not in Cycle 2 |
| **Preconditions** | 4 (min_account_sequence, etc.) | 0 | ❌ Not in Cycle 2 |
| **Soroban** | 15+ (resource fees, instructions, etc.) | 0 | ❌ Not in Cycle 2 |
| **Raw XDR** | 4 (tx_envelope, tx_result, etc.) | 0 | ❌ Storage bloat |

**Coverage**: 13/45 fields (29%) - but captures 80% of query value

**Rationale**: Start with high-value fields that can't be added later. Soroban and fee bumps can wait for Cycle 3+.

### Trustlines Schema Comparison

| Category | Stellar ETL | Our Cycle 2 | Status |
|----------|-------------|-------------|--------|
| **Core fields** | 7 | 10 | ✅ Aligned |
| **Liabilities** | 2 (buying, selling) | 2 (buying, selling) | ✅ Full match |
| **Limits** | 1 (trust_line_limit) | 1 (trust_line_limit) | ✅ Full match |
| **Authorization** | 1 (flags) | 1 (flags) | ✅ Full match |
| **Metadata** | 3 (ledger_key, deleted, sponsor) | 0 | ❌ Not in Cycle 2 |
| **Liquidity pools** | 2 (liquidity_pool_id, strkey) | 0 | ❌ Not in Cycle 2 |
| **Asset IDs** | 2 (asset_id, asset_type) | 0 | ❌ Not in Cycle 2 |

**Coverage**: 10/19 fields (53%) - captures all essential balance tracking

**Rationale**: We have all the high-value fields. Liquidity pools and metadata can be added in Cycle 3+.

---

## Notes

This is a **balanced bet** (updated from original 8+7 to 13+10 fields):
- Not too small (2 tables, 23 total fields, real value)
- Not too big (proven patterns, 1 week, fields already in SDK output)
- High confidence (working reference code + stellar-etl validation)
- Flexible scope (can cut to 9+7 minimum if desperate)
- **Prevents reingestion pain** (Parquet immutability addressed)

The account-balance-processor gives us a **huge advantage** - we're not guessing, we're adapting working code.

**The additional 5 transaction fields and 3 balance fields are essentially free** - they're already in the SDK output, just simple struct field accesses. The alternative is reingesting millions of ledgers later.

**Discord validation**: Real users need these fields (memos, reliable joins, fee analysis). We're building for actual use cases, not theoretical ones.

**If you're 50% through the week and stuck**: Cut memo fields (2), cut result_code (1), cut liabilities (2). Ship with minimum 9 transaction fields + 7 balance fields.

---

## Ledger Table Prerequisite

**See `LEDGER_TABLE_UPDATE.md` for full details.**

Before betting on Cycle 2, the ledger table must be updated with 10 missing fields from stellar-etl:

**Missing fields**:
1. `transaction_count` (total transactions)
2. `operation_count` (successful operations)
3. `tx_set_operation_count` (all operations)
4. `soroban_fee_write1kb` (Protocol 20+ Soroban fees)
5. `node_id` (SCP validator)
6. `signature` (SCP signature)
7. `ledger_header` (base64 XDR blob)
8. Protocol 23 fields (bucket list sizes, evicted keys - CAP-62)

**Appetite**: 2 days (separate bet)
**Why it matters**: Parquet immutability - add fields NOW or reingest millions of ledgers later

This should be done BEFORE starting Cycle 2 to avoid schema mismatches and reingestion.
