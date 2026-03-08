# Soroban-First Polish — Remaining 10%

Work completed in Cycles 1–4 covers ~90% of the Soroban-first API surface. This document outlines the remaining polish items, organized by effort and impact.

---

## 1. Structured Swap Fields on TxSummary

**Current state:** `tx_summary.go` detects swap patterns (two counter-directional transfers involving the same address) and produces a human-readable string like `"Swapped 100.0000000 USDC for 48.5000000 XLM on contract CDEF..."`.

**Gap:** The summary is a flat string. If Prism needs to render swap details (token icons, amounts, price) it has to parse the string.

**Fix:** Add structured fields to `TxSummary`:

```go
type TxSummary struct {
    Description        string   `json:"description"`
    Type               string   `json:"type"`
    InvolvedContracts  []string `json:"involved_contracts"`
    // New structured fields (populated when Type == "swap")
    Swap *SwapDetail `json:"swap,omitempty"`
}

type SwapDetail struct {
    SoldAsset    string `json:"sold_asset"`
    SoldAmount   string `json:"sold_amount"`
    BoughtAsset  string `json:"bought_asset"`
    BoughtAmount string `json:"bought_amount"`
    Router       string `json:"router,omitempty"`   // contract that orchestrated
    Trader       string `json:"trader"`             // address that initiated
}
```

**Effort:** Small (1–2 hours). The swap detection logic already extracts both transfers — just needs to populate the struct instead of only formatting the string.

**Files:** `tx_summary.go`, `silver_reader.go`

---

## 2. Composite Explorer Transaction Endpoint

**Current state:** Transaction data is split across multiple endpoints:
- `/silver/explorer/transaction?tx_hash=` — basic ops + tx metadata
- `/silver/tx/{hash}/decoded` — human-readable summary + decoded ops + events
- `/silver/tx/{hash}/events` — CAP-67 events
- `/silver/tx/{hash}/contracts-involved` — contract list
- `/silver/tx/{hash}/call-graph` — call tree

**Gap:** A Prism transaction page needs 3–5 calls to render fully. The RFP implies a single endpoint should power the transaction detail view.

**Fix:** Extend `/silver/explorer/transaction` (or create `/silver/tx/{hash}/full`) to return a composite response:

```json
{
  "transaction": { /* existing tx metadata */ },
  "summary": { /* from decoded endpoint */ },
  "operations": [ /* decoded operations with contract/function fields */ ],
  "events": [ /* CAP-67 unified events */ ],
  "contracts_involved": ["CABC...", "CDEF..."],
  "call_graph": [ /* call tree if Soroban */ ]
}
```

**Effort:** Medium (half day). All the data fetching methods exist — this is composition and a new handler method in `handlers_decode.go` or `handlers_silver.go`.

**Files:** `handlers_decode.go` or `handlers_silver.go`, `main_silver.go`

---

## 3. Promote `/calls` as First-Class Resource

**Current state:** Contract call filtering lives at `/silver/operations/soroban/by-function`. It works but is buried under "operations" — conceptually these are "calls," not "operations."

**Gap:** The LLM response and RFP both frame contract calls as a top-level entity. A `/calls` resource would make the API more intuitive for Soroban-native developers.

**Options:**

| Approach | Effort | Tradeoff |
|----------|--------|----------|
| A. Add `/silver/calls` as alias routing to the same handler | Trivial | Two routes, same code |
| B. Create dedicated `handlers_calls.go` with richer response shape | Small | Can add call-specific fields (depth, sub-invocations) later |
| C. Leave as-is, document clearly | Zero | Functional but less discoverable |

**Recommendation:** Option A for now. Register `/silver/calls` pointing to `HandleSorobanOpsByFunction`. Rename later if the response shape diverges.

**Files:** `main_silver.go` (1 line)

---

## 4. Structured Mint/Burn Detail on TxSummary

**Current state:** Mint and burn summaries are strings: `"Minted 100 USDC to GXYZ..."`, `"Burned 50 USDC from GABC..."`.

**Gap:** Same as swaps — Prism may need structured data for rendering.

**Fix:** Add optional structured fields:

```go
type MintBurnDetail struct {
    Asset   string `json:"asset"`
    Amount  string `json:"amount"`
    Account string `json:"account"` // minted to / burned from
}

// On TxSummary:
Mint *MintBurnDetail `json:"mint,omitempty"`
Burn *MintBurnDetail `json:"burn,omitempty"`
```

**Effort:** Small (1 hour). Detection logic already exists.

**Files:** `tx_summary.go`, `silver_reader.go`

---

## 5. Token Decimals Awareness

**Current state:** SEP-41 balances and amounts are returned in raw stroops (7 decimals assumed). The `token_transfers_raw` table stores amounts as integers.

**Gap:** Soroban tokens can have arbitrary decimals (not always 7). A USDC token with 6 decimals would display incorrectly if formatted with 7.

**Fix:** When `contract_data_current` gets decoded balance/metadata columns (or when a decimals lookup is available), add a `decimals` field to `SEP41TokenMetadata` and use it to format display amounts. For now, document the assumption.

**Interim approach:** Add `decimals` field to metadata response defaulting to `7`, with a comment that this will be contract-derived once available.

**Effort:** Small now (add field + default), medium later (actual contract state lookup).

**Files:** `silver_reader.go`, `unified_duckdb_reader.go` (GetSEP41TokenMetadata)

---

## 6. SEP-50 Placeholder

**Current state:** No SEP-50 (NFT) endpoints exist.

**Gap:** RFP mentions SEP-50 support. The standard is still draft but the API should be ready to add it without breaking changes.

**Fix:** No code needed now. Reserve the route namespace:

```
/silver/nfts/{contract_id}                  — collection metadata
/silver/nfts/{contract_id}/tokens           — token list
/silver/nfts/{contract_id}/tokens/{token_id} — single token
/silver/nfts/{contract_id}/transfers        — transfer history
```

Document these as "coming when SEP-50 is finalized" in the analyst guide.

**Effort:** Zero (documentation only for now).

---

## Priority Order

| # | Item | Effort | Impact | Priority |
|---|------|--------|--------|----------|
| 1 | Structured swap fields | Small | High — Prism rendering | Do first |
| 2 | Composite tx endpoint | Medium | High — reduces frontend calls | Do first |
| 4 | Structured mint/burn fields | Small | Medium — consistency with swap | Bundle with #1 |
| 3 | `/calls` alias | Trivial | Medium — API discoverability | Quick win |
| 5 | Token decimals | Small | Medium — correctness for non-7-decimal tokens | Next cycle |
| 6 | SEP-50 placeholder | Zero | Low — future-proofing | Document only |

**Total estimated effort:** ~1 day for items 1–4, half day for item 5, zero for item 6.
