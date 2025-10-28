# Pitch: Query Transactions from DuckLake

**Status**: Option (not committed)
**Appetite**: 1 week
**Author**: Solo Dev
**Date**: 2025-10-26

---

## Problem

Right now I can only see ledger-level data in DuckLake. I want to answer questions like:
- "Which transactions happened in ledger 500000?"
- "How many transactions failed due to insufficient balance?"
- "What was the average fee paid in the last 1000 ledgers?"

**Without this**, I need to:
- Re-process raw XDR manually every time
- Use external tools like stellar-etl
- Can't build transaction-based analytics

**Why now?**
- Ledger ingestion is stable (Cycle 1 complete)
- Need transaction data for payment analysis
- Cycle 1 proved the architecture works

---

## Appetite

**1 week** (5 working days)

This is a small cycle because:
- Transaction extraction is simpler than Soroban
- Can reuse patterns from ledger processor
- Want to validate multi-table approach before going deeper

**If it takes longer**: Cut scope, not time.

---

## Solution (Fat-Marker Sketch)

### Core Approach

```
LedgerCloseMeta (existing)
    ↓
New: TransactionProcessor
    ↓
Extract ~8 core fields (not all 20)
    ↓
Write to: public_archives_test.ledger_testing.transactions
    ↓
Query: JOIN with ledgers table
```

### Minimal Transaction Fields

**MUST HAVE**:
- ledger_sequence (FK to ledgers)
- transaction_hash (PK)
- source_account
- fee_charged
- successful (boolean)
- operation_count

**NICE TO HAVE**:
- memo_type, memo_value
- created_at (copy from ledger.closed_at)

**COULD HAVE** (cut first if needed):
- max_fee (fee_bid)
- time_bounds
- signatures

### Table Schema (Rough)

```sql
CREATE TABLE transactions (
    ledger_sequence BIGINT,
    transaction_hash VARCHAR PRIMARY KEY,
    source_account VARCHAR,
    fee_charged BIGINT,
    successful BOOLEAN,
    operation_count INT,
    created_at TIMESTAMP,
    -- MAYBE: memo fields
)
```

### Implementation Steps

1. Copy `extractLedgerData()` pattern → `extractTransactionData()`
2. Add transaction buffer (parallel to ledger buffer)
3. Flush both buffers in same transaction
4. Test with 100 ledgers locally
5. Ship to production

---

## Rabbit Holes

**DON'T**:
- ❌ Build operation processor yet (that's a separate bet)
- ❌ Parse memos beyond storing raw values (complex, low value)
- ❌ Decode Soroban resource usage (Cycle 3 territory)
- ❌ Handle transaction meta XDR (just the result)
- ❌ Add indexes or optimization (premature)
- ❌ Build a query API (just SQL for now)

**If you find yourself**:
- Writing XDR decoders → Stop, use SDK helpers
- Adding > 10 fields → Cut to core fields
- Spending > 1 day on schema design → Ship simple schema

---

## No-Gos

**Explicitly OUT of scope**:
- Operation-level data (that's a future pitch)
- Soroban-specific transaction fields
- Transaction history API
- Performance optimization (< 5s lag is fine)
- Backfilling historical data (manual is OK)
- Multiple network support (testnet only)

---

## Done Looks Like

**I can run these queries**:

```sql
-- Count successful vs failed transactions
SELECT
  successful,
  COUNT(*)
FROM transactions
GROUP BY successful;

-- Average fee per ledger
SELECT
  l.sequence,
  AVG(t.fee_charged) as avg_fee
FROM ledgers l
JOIN transactions t ON l.sequence = t.ledger_sequence
GROUP BY l.sequence
ORDER BY l.sequence DESC
LIMIT 10;

-- Find high-fee transactions
SELECT * FROM transactions
WHERE fee_charged > 1000000
ORDER BY fee_charged DESC
LIMIT 20;
```

**Success demo**:
- Process 1000 ledgers
- Both `ledgers` and `transactions` tables populated
- No duplicate transaction hashes
- JOIN queries work
- Data visible within 30 seconds of ingestion

**Not done** unless:
- ✅ Can query transactions independently
- ✅ Can JOIN with ledgers table
- ✅ Existing ledger ingestion still works
- ✅ Basic SQL queries run in < 1 second

---

## Scope Line

```
CUT FIRST IF RUNNING LATE:
────────────────────────────
- Memo fields
- Time bounds
- Additional metadata

TRY TO INCLUDE:
───────────────
- Basic memo_type
- created_at timestamp
- operation_count

MUST HAVE (NON-NEGOTIABLE):
═══════════════════════════
- ledger_sequence (FK)
- transaction_hash (PK)
- source_account
- fee_charged
- successful flag
```

---

## Open Questions

**Q**: Should transactions share same partition key as ledgers?
**A**: Yes - partition by ledger_range for co-location

**Q**: What if transaction parsing fails mid-ledger?
**A**: Fail the whole ledger (stop-and-alert, same as Cycle 1)

**Q**: UPSERT or INSERT?
**A**: INSERT only (transactions are immutable)

---

## Betting Criteria

**Bet on this if**:
- ✅ Still need transaction-level analytics
- ✅ No higher priority problems emerged from Cycle 1
- ✅ Have 1 week available (not rushed)
- ✅ Excited to work on this

**Don't bet on this if**:
- ❌ Ledger data alone is sufficient
- ❌ Other problems are more urgent
- ❌ Feeling burned out (extend cool-down)
- ❌ Architecture needs refactoring first

---

## Alternative Pitches (Compete with This)

Instead of betting on transactions, could bet on:

**Option B**: "Add Operation Counts to Ledgers"
- Appetite: 2 days
- Adds operation stats to existing ledger table
- Matches stellar-etl parity
- Smaller, safer bet

**Option C**: "Query Soroban Contract Events"
- Appetite: 2 weeks
- More valuable for contract developers
- Riskier (unknown complexity)
- Bigger bet

**Option D**: "Nothing" (Extended Cool-Down)
- Appetite: 1 week
- Focus on docs, cleanup, exploration
- Safe bet
- Good if unsure what's next

---

## Notes

This is a **pitch, not a plan**. It might not get built. After cool-down, decide if this is still the most valuable bet.

**Hill Chart Milestone**:
- **Left side (figuring out)**: Get one transaction extracted and written
- **Right side (making it happen)**: Process 1000 ledgers successfully

If stuck on left side at 50% time → **cut memos and time_bounds immediately**.

