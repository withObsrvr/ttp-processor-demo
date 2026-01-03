# Shape Up: Token Transfers Analytics Table

**Cycle**: 2 weeks
**Team**: Solo
**Status**: Future - requires Cycles 1 & 2 completion
**Prerequisite**: Cool-down period after Cycle 2

---

## Problem

Analytics users and block explorers need a **unified view of all token movements** across the Stellar network - both classic assets (payments, path payments) and Soroban tokens (SAC tokens, custom contracts). Without this:

- Must understand 3+ different operation types (payment, path_payment_strict_send, path_payment_strict_receive)
- Must parse contract events to find token transfers (SEP-41 standard)
- Must normalize between classic and Soroban token formats
- Can't answer simple questions like "show me all transfers of asset X" without complex logic

**User pain point:**
"I want to track USDC movements, but USDC exists as both a classic asset and a Soroban token. I have to query operations for classic payments AND parse contract events for Soroban transfers, then merge the results. It's too complicated."

---

## Appetite

**2 weeks** (10 working days)

This is a large feature because:
- Complex business logic spanning multiple sources
- SEP-41 contract event parsing (new territory)
- Schema design for unified token representation
- Both classic and Soroban token handling

**Why 2 weeks?**
- Week 1: Research, schema design, classic token extraction
- Week 2: Soroban event parsing, unification, testing

**Not 1 week:** Too complex. Event parsing alone could take 3-4 days.
**Not 3+ weeks:** Diminishing returns. Ship v1, iterate later if needed.

---

## Solution

### Fat Marker Sketch

```
Bronze Layer                               Token Transfers Table
├─ operations_row_v2                       ┌────────────────────────┐
│  └─ payment (type 1)          ────────>  │  Unified Token Transfer│
│  └─ path_payment_* (type 2,13) ───────>  │  ├─ from_account       │
│                                           │  ├─ to_account         │
├─ contract_events_stream_v1              │  ├─ asset (unified)    │
│  └─ SEP-41 transfer events   ────────>   │  ├─ amount            │
│     (topic: "transfer")                   │  ├─ tx_hash           │
│                                           │  ├─ ledger_sequence   │
└─ ledgers_row_v2 (context)   ────────>    │  └─ transfer_type     │
                                            └────────────────────────┘
```

### Implementation Approach

**Week 1: Foundation & Classic Tokens**

**Day 1-2: Research & Schema Design**
- Study SEP-41 token interface standard
- Analyze Hubble's token_transfers_raw schema
- Design unified asset representation (classic vs Soroban)
- Prototype extraction logic with curl + DuckDB

**Day 3-4: Classic Token Extraction**
```go
// Extract from operations_row_v2
func extractClassicTransfers(operations []BronzeOperation) []TokenTransfer {
    transfers := []TokenTransfer{}

    for _, op := range operations {
        switch op.Type {
        case 1: // payment
            transfers = append(transfers, TokenTransfer{
                FromAccount:    op.SourceAccount,
                ToAccount:      op.Details.To,
                Asset:          normalizeAsset(op.Details.Asset),
                Amount:         op.Details.Amount,
                TransferType:   "payment",
                LedgerSequence: op.LedgerSequence,
            })
        case 2: // path_payment_strict_receive
            // Extract send and receive transfers
        case 13: // path_payment_strict_send
            // Extract send and receive transfers
        }
    }

    return transfers
}
```

**Day 5: Testing & Refinement**
- Test with 10,000 ledgers of testnet data
- Verify payment operation extraction correctness
- Add metrics for classic transfers

**Week 2: Soroban Tokens & Unification**

**Day 6-7: SEP-41 Event Parsing**
```go
// Extract from contract_events_stream_v1
func extractSorobanTransfers(events []BronzeContractEvent) []TokenTransfer {
    transfers := []TokenTransfer{}

    for _, event := range events {
        // Check if this is a transfer event
        if !isTransferEvent(event) {
            continue
        }

        // Parse SEP-41 format:
        // Topic: ["transfer", from, to]
        // Data: [amount]
        transfer := TokenTransfer{
            FromAccount:    parseAddress(event.Topics[1]),
            ToAccount:      parseAddress(event.Topics[2]),
            Asset:          normalizeSorobanAsset(event.ContractID),
            Amount:         parseAmount(event.Data),
            TransferType:   "soroban_transfer",
            LedgerSequence: event.LedgerSequence,
            ContractID:     event.ContractID,
        }

        transfers = append(transfers, transfer)
    }

    return transfers
}
```

**Day 8: Unified Asset Normalization**
```go
type Asset struct {
    AssetType  string // "native", "credit_alphanum4", "credit_alphanum12", "soroban"
    AssetCode  string // "XLM", "USDC", etc.
    AssetIssuer string // Classic issuer or Soroban contract ID
}

func normalizeAsset(classicAsset ClassicAsset) Asset { ... }
func normalizeSorobanAsset(contractID string) Asset { ... }
```

**Day 9: Integration & Testing**
- Combine classic and Soroban extractors
- Insert into `token_transfers_raw` table
- End-to-end test with mixed classic/Soroban transfers
- Verify no duplicates, no missing transfers

**Day 10: Polish & Documentation**
- Add comprehensive metrics
- Document SEP-41 parsing decisions
- Add query examples for common use cases
- Performance check (should handle 1000s of transfers/sec)

### Files to Create/Modify

1. **silver-transformer/token_transfers.go** (NEW) - Extraction logic
2. **silver-transformer/sep41.go** (NEW) - SEP-41 event parsing
3. **silver-transformer/schema.go** - Add token_transfers_raw table
4. **silver-transformer/transformer.go** - Wire up token extraction
5. **silver-transformer/assets.go** (NEW) - Asset normalization

---

## Rabbit Holes

**DON'T:**

1. **Don't try to identify all possible tokens**
   Extract transfers only. Token metadata (name, symbol) is out of scope.

2. **Don't implement token balance tracking**
   Just record transfers. Balance calculation is a different table (gold layer).

3. **Don't handle non-standard contract events**
   Only SEP-41 compliant events. Custom formats are out of scope.

4. **Don't add price/value calculations**
   Transfer amounts only. USD values require external price feeds (future work).

5. **Don't implement liquidity pool events**
   Just direct transfers. Pool deposits/withdrawals are different semantics.

6. **Don't parse memo fields**
   Transfer basics only. Memo interpretation is out of scope.

7. **Don't deduplicate based on operation IDs**
   Trust bronze layer. Dedup logic adds complexity without clear benefit.

8. **Don't build a token registry**
   Discover tokens dynamically from transfers. Central registry is future work.

---

## No-Gos

Explicitly **OUT OF SCOPE** for this cycle:

❌ Token metadata (name, symbol, decimals)
❌ Token balance calculations (that's gold layer)
❌ Price/valuation data
❌ Liquidity pool events
❌ Non-SEP-41 custom contract events
❌ Memo field parsing
❌ Token categorization (stablecoin vs governance vs NFT)
❌ Cross-chain bridge tracking
❌ Historical balance reconstruction
❌ Transfer aggregations (daily volumes, etc.)

---

## Done

**Demo:**

```bash
# Start full pipeline
cd ducklake-ingestion-obsrvr-v3
./bin/ducklake-ingestion-obsrvr-v3 -config config/query-api-test.yaml \
  --multi-network --query-port :8081

cd ../silver-transformer
./silver-transformer

# Wait for processing
sleep 60

# Query unified transfers
echo "SELECT
    transfer_type,
    asset_code,
    from_account,
    to_account,
    amount,
    ledger_sequence
FROM token_transfers_raw
WHERE asset_code = 'USDC'
ORDER BY ledger_sequence DESC
LIMIT 10" | duckdb catalogs/silver.duckdb

# Expected output:
# transfer_type     | asset_code | from_account | to_account  | amount | ledger_sequence
# ------------------|------------|--------------|-------------|--------|----------------
# payment           | USDC       | GABC...      | GDEF...     | 100.0  | 9523
# soroban_transfer  | USDC       | GHIJ...      | GKLM...     | 50.0   | 9487
# path_payment      | USDC       | GNOP...      | GQRS...     | 25.5   | 9401

# Verify counts match sources
echo "SELECT
    (SELECT COUNT(*) FROM operations_row_v2 WHERE type IN (1,2,13)) as classic_ops,
    (SELECT COUNT(*) FROM contract_events_stream_v1 WHERE topic[1] = 'transfer') as soroban_events,
    (SELECT COUNT(*) FROM token_transfers_raw) as total_transfers" \
  | duckdb catalogs/bronze.duckdb

# Verify no duplicates
echo "SELECT tx_hash, from_account, to_account, amount, COUNT(*)
FROM token_transfers_raw
GROUP BY tx_hash, from_account, to_account, amount
HAVING COUNT(*) > 1" | duckdb catalogs/silver.duckdb
# Should return 0 rows

# Performance check
curl http://localhost:9092/metrics | grep token_transfers
# token_transfers_extracted_total{type="classic"} 12450
# token_transfers_extracted_total{type="soroban"} 3201
# token_transfers_extraction_latency_seconds{quantile="0.95"} 0.234
```

**Success Criteria:**

✅ token_transfers_raw table created and populated
✅ Classic payment operations extracted correctly
✅ Path payment operations extracted (both send and receive)
✅ SEP-41 Soroban transfer events parsed
✅ Unified asset representation across classic/Soroban
✅ No duplicate transfers
✅ Count matches: classic ops + soroban events = total transfers
✅ Query examples work (filter by asset, account, date range)
✅ Extraction latency < 1 second per batch
✅ Service stable for 2 hours processing mixed transfers
✅ Documentation explains SEP-41 parsing decisions

---

## Scope Line

```
COULD HAVE ─────────────────────────────────────────
│ • Token metadata extraction (name, symbol)
│ • Asset discovery service
│ • Transfer aggregation views
│ • Memo field parsing
│
NICE TO HAVE ───────────────────────────────────────
│ • Non-SEP-41 custom event handling
│ • Transfer categorization (payment vs swap vs pool)
│ • Performance optimization for high-volume tokens
│ • Integration tests with known USDC contract
│
MUST HAVE ══════════════════════════════════════════
│ • Classic payment operation extraction
│ • SEP-41 Soroban transfer event parsing
│ • Unified asset representation
│ • token_transfers_raw table populated
│ • No duplicates, correct counts
│ • Query examples functional
│ • Documentation for parsing logic
└─────────────────────────────────────────────────────
```

**Cutting Scope Strategy:**

If behind schedule at 50% mark (end of Week 1):

1. **Cut COULD HAVE immediately**
2. **Simplify asset normalization** - Just use raw strings, not structured Asset type
3. **Skip path payments** - Only implement basic payment operation (type 1)
4. **Reduce Soroban parsing** - Just extract topics, don't validate SEP-41 compliance
5. **Last resort: Ship classic only** - Defer Soroban to next cycle

**CORE MUST SHIP:** Basic payment operations + basic Soroban transfer events in one table

---

## Hill Chart

Track progress through the 2-week cycle:

```
Figuring it out          |          Making it happen
    (0-50%)              |              (50-100%)
                         |
   [Week 1]              |           [Week 2]
   Days 1-5              |           Days 6-10
```

**Left Side (Figuring Out):**
- Research SEP-41 standard
- Understand contract event format
- Design unified schema
- Prototype with sample data
- Extract classic transfers

**Right Side (Making Happen):**
- Implement Soroban parser
- Build asset normalization
- Wire up full pipeline
- Test end-to-end
- Polish and document

**Red Flag:** If still on left side by end of Day 6 → Cut Soroban parsing, ship classic only

---

## Cool-Down After This Cycle

**Mandatory 2-3 days** (20% of 2-week cycle).

This cycle is complex, so proper cool-down is critical:

Cool-down activities:
- Review and refactor token extraction code
- Add comprehensive integration tests
- Profile actual extraction performance
- Document SEP-41 edge cases discovered
- Write tutorial for querying token transfers
- Fix any bugs discovered during testing
- **REST** - This was a hard cycle!

---

## Dependencies

**Required:**
- ✅ Cycle 1 & 2 completed (enriched operations working)
- ✅ Bronze has contract_events_stream_v1 populated
- ✅ Bronze has operations_row_v2 with payment operations
- ✅ Query API supports contract events (may need extension)

**Blocked By:**
- ⚠️ May need to extend Query API for contract events (check early)

**Not Required:**
- ❌ Token metadata service
- ❌ External price feeds
- ❌ Gold layer aggregations

---

## Risk Assessment

**High Risk** 🔴

This cycle has significant risk because:
- **Unknown complexity** - SEP-41 parsing is new territory
- **Data variety** - Many edge cases in contract events
- **Multiple sources** - Classic ops + Soroban events must align
- **Schema design** - Unified asset model is tricky

**Mitigation Strategies:**
1. **SEP-41 risk** - Research thoroughly in Days 1-2, prototype before coding
2. **Complexity risk** - Cut scope aggressively if behind by Day 6
3. **Data quality risk** - Start with testnet (cleaner data), validate before mainnet
4. **Schema risk** - Validate with known transfers (USDC, USDT) early

**Kill Criteria:**
- If SEP-41 parsing not working by Day 7 → Ship classic-only version
- If unified schema doesn't work by Day 8 → Ship separate classic/soroban tables
- If extraction latency > 5 seconds by Day 9 → Ship simplified version

**Escape Hatch:**
Ship separate tables: `token_transfers_classic` and `token_transfers_soroban`. Unification can be a future cycle.

---

## Success Metrics

Track these twice per week (Days 3, 5, 8, 10):

| Metric | Target | Red Flag | Kill If |
|--------|--------|----------|---------|
| Classic extraction working | Day 4 | Not by Day 5 | - |
| SEP-41 parser functional | Day 7 | Not by Day 8 | Not by Day 9 |
| Unified schema validated | Day 8 | Not by Day 9 | Not by Day 10 |
| End-to-end test passing | Day 9 | Not by Day 10 | - |
| Extraction latency | < 1s | > 3s | > 5s |
| Data accuracy | 100% match | < 98% | < 95% |

---

## Research Questions (Answer in Days 1-2)

Before coding, answer these:

1. **What is the exact SEP-41 event format?**
   - Topic structure: `["transfer", from: Address, to: Address]`
   - Data structure: `[amount: i128]`
   - Edge cases: mints (from=0), burns (to=0)

2. **How do we identify transfer events vs other events?**
   - Topic[0] == "transfer"
   - Is there a type field we can filter on?

3. **What's the contract ID → asset code mapping?**
   - Use contract ID directly? Or lookup asset metadata?
   - Decision: Use contract ID, defer asset metadata

4. **How do path payments work?**
   - Do they create multiple transfer records?
   - Decision: TBD after research

5. **What's the expected volume?**
   - How many transfers per ledger on testnet? Mainnet?
   - Will we hit performance issues?

**Document answers in RESEARCH_NOTES.md before Day 3**

---

## References

- SEP-41 Token Interface: https://stellar.org/protocol/sep-41
- Hubble token_transfers_raw: https://developers.stellar.org/docs/data/analytics/hubble/data-catalog/data-dictionary/silver#token_transfers_raw
- Soroban contract events: https://developers.stellar.org/docs/smart-contracts/guides/events
- Silver mapping doc: [SILVER_LAYER_MAPPING.md](../SILVER_LAYER_MAPPING.md)

---

## Notes on Flexibility

This 2-week cycle has built-in flexibility:

**If ahead of schedule:**
- Add path payment support (nice to have)
- Implement basic token categorization
- Add memo field extraction

**If behind schedule:**
- Ship classic-only (Week 1 deliverable)
- Defer Soroban to Cycle 4
- Use separate tables instead of unified

**The goal:** Ship something useful at 2-week mark, even if not perfect. Perfect is the enemy of done.
