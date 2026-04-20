# Ledger Summary Endpoint

Purpose: define a structured ledger-level summary endpoint that can power ledger-first explorer surfaces such as Prism home page hero cards, dashboards, and analytics views.

This endpoint is intended to provide:
- ledger identity
- transaction and operation totals
- semantic composition counts
- Soroban utilization metrics
- provenance / partial-data indicators

It should remain:
- deterministic
- structured
- reusable by non-Prism consumers
- free of product-specific narration

---

## Recommended endpoint

```http
GET /api/v1/silver/ledgers/{seq}/summary
```

### Why this route
This route keeps the payload tied to a canonical ledger resource rather than a Prism-specific homepage concept.

It is suitable for:
- explorer home pages
- ledger cards
- dashboard widgets
- API consumers who want a compact ledger summary without multiple fan-out requests

---

## Design goals

The endpoint should let clients render UI like:
- `ledger #52,844,201`
- `245 transactions`
- chips like `68 swaps`, `86 calls`, `39 agents`
- utilization bars like `instructions 64%`, `read / write 60%`

without requiring multiple follow-up requests.

---

## Response shape

### Example response

```json
{
  "ledger": {
    "sequence": 52844201,
    "closed_at": "2026-04-17T18:46:53Z",
    "protocol_version": 23,
    "hash": "abc123...",
    "previous_hash": "def456..."
  },
  "totals": {
    "transaction_count": 245,
    "successful_tx_count": 231,
    "failed_tx_count": 14,
    "operation_count": 612,
    "contract_event_count": 98,
    "soroban_op_count": 141,
    "total_fee_charged": 128400
  },
  "classification_counts": {
    "swap_tx_count": 68,
    "contract_call_tx_count": 86,
    "agent_tx_count": 39,
    "payment_tx_count": 22,
    "wallet_tx_count": 14,
    "deployment_tx_count": 3,
    "classic_tx_count": 104,
    "soroban_tx_count": 141
  },
  "soroban_utilization": {
    "instructions_used": 384000000,
    "instructions_limit": 600000000,
    "instructions_pct": 64,
    "read_bytes_used": 210000,
    "write_bytes_used": 150000,
    "read_write_bytes_used": 360000,
    "read_write_bytes_limit": 600000,
    "read_write_pct": 60,
    "rent_burned": 24.8
  },
  "composition": {
    "dominant_tx_type": "contract_call",
    "dominant_tx_type_count": 86,
    "soroban_share_pct": 58,
    "failed_share_pct": 6
  },
  "provenance": {
    "classification_source": "semantic_tx_materialized_v1",
    "utilization_source": "ledger_soroban_aggregates_v1",
    "partial": false
  }
}
```

---

## Field definitions

## `ledger`
Canonical ledger identity fields.

```json
"ledger": {
  "sequence": 52844201,
  "closed_at": "2026-04-17T18:46:53Z",
  "protocol_version": 23,
  "hash": "...",
  "previous_hash": "..."
}
```

### Required
- `sequence`
- `closed_at`

### Optional but recommended
- `protocol_version`
- `hash`
- `previous_hash`

---

## `totals`
Base ledger metrics.

```json
"totals": {
  "transaction_count": 245,
  "successful_tx_count": 231,
  "failed_tx_count": 14,
  "operation_count": 612,
  "contract_event_count": 98,
  "soroban_op_count": 141,
  "total_fee_charged": 128400
}
```

### Required
- `transaction_count`

### Strongly recommended
- `successful_tx_count`
- `failed_tx_count`
- `operation_count`
- `soroban_op_count`

### Optional
- `contract_event_count`
- `total_fee_charged`

---

## `classification_counts`
Per-ledger transaction composition counts. These power UI chips and analytics summaries.

```json
"classification_counts": {
  "swap_tx_count": 68,
  "contract_call_tx_count": 86,
  "agent_tx_count": 39,
  "payment_tx_count": 22,
  "wallet_tx_count": 14,
  "deployment_tx_count": 3,
  "classic_tx_count": 104,
  "soroban_tx_count": 141
}
```

### Required for ledger-first explorer mock
- `swap_tx_count`
- `contract_call_tx_count`
- `agent_tx_count`

### Strongly recommended
- `payment_tx_count`
- `wallet_tx_count`
- `deployment_tx_count`
- `classic_tx_count`
- `soroban_tx_count`

### Important note on `agent_tx_count`
This field must have a documented deterministic meaning.

Recommended definition:

> `agent_tx_count` = number of transactions in the ledger where at least one actor is classified as `agent` or `automated_actor` by the semantic transaction classifier.

If the term `agent` is not considered stable enough yet, consider using:
- `automated_actor_tx_count`

and letting downstream applications label it as “agents” if desired.

---

## `soroban_utilization`
Resource usage and capacity metrics for the ledger.

```json
"soroban_utilization": {
  "instructions_used": 384000000,
  "instructions_limit": 600000000,
  "instructions_pct": 64,
  "read_bytes_used": 210000,
  "write_bytes_used": 150000,
  "read_write_bytes_used": 360000,
  "read_write_bytes_limit": 600000,
  "read_write_pct": 60,
  "rent_burned": 24.8
}
```

### Required for ledger-first explorer mock
- `instructions_pct`
- `read_write_pct`

### Strongly recommended
- `instructions_used`
- `instructions_limit`
- `read_bytes_used`
- `write_bytes_used`
- `read_write_bytes_used`
- `read_write_bytes_limit`

### Optional
- `rent_burned`

### Design note
The API should provide:
- raw usage
- raw limits
- percentages

This allows clients to render:
- bars
- text labels
- tooltips
- alternate normalizations

without having to guess denominators.

---

## `composition`
Optional helper metrics derived from the main totals and classification counts.

```json
"composition": {
  "dominant_tx_type": "contract_call",
  "dominant_tx_type_count": 86,
  "soroban_share_pct": 58,
  "failed_share_pct": 6
}
```

These are not required for the mock but can support richer ledger-first explorer experiences.

### Optional fields
- `dominant_tx_type`
- `dominant_tx_type_count`
- `soroban_share_pct`
- `failed_share_pct`

---

## `provenance`
Metadata describing where summary values came from and whether the payload is partial.

```json
"provenance": {
  "classification_source": "semantic_tx_materialized_v1",
  "utilization_source": "ledger_soroban_aggregates_v1",
  "partial": false
}
```

### Recommended fields
- `classification_source`
- `utilization_source`
- `partial`

### Why this matters
This helps preserve trust when:
- some ledgers are only partially indexed
- some counts are approximated or lagging
- some semantic materializations are unavailable

---

## Minimal viable response

If implementing a minimal version first, this shape is sufficient to render the current ledger-first inspiration mock:

```json
{
  "ledger": {
    "sequence": 52844201,
    "closed_at": "2026-04-17T18:46:53Z"
  },
  "totals": {
    "transaction_count": 245
  },
  "classification_counts": {
    "swap_tx_count": 68,
    "contract_call_tx_count": 86,
    "agent_tx_count": 39
  },
  "soroban_utilization": {
    "instructions_pct": 64,
    "read_write_pct": 60
  },
  "provenance": {
    "partial": false
  }
}
```

However, the richer version is recommended for long-term reuse.

---

## Counting semantics

## Core recommendation
Classification counts should be based on **unique transactions per ledger**, not operation counts or event counts.

### Why
The home/hero ledger summary is intended to answer:
- how many transactions in this ledger were swaps?
- how many were calls?
- how many involved agents?

This is more intuitive and stable than counting raw operations or events.

---

## Counting model

### Recommended model: non-exclusive counters
A single transaction may increment multiple counters.

Example:
- a smart wallet swap transaction may count toward:
  - `swap_tx_count`
  - `contract_call_tx_count`
  - `agent_tx_count`

This model is best for ledger composition chips.

### Not recommended for this use case: exclusive buckets
Exclusive classification would force one tx into exactly one bucket, which makes the ledger less informative for multi-dimensional homepage summaries.

---

## Suggested classification definitions

These are illustrative and should be aligned with actual semantic-layer definitions.

- `swap_tx_count`
  - count txs whose semantic classifier yields a swap-like tx type
- `contract_call_tx_count`
  - count txs with Soroban contract invocation / contract-call semantic classification
- `agent_tx_count`
  - count txs with at least one actor classified as `agent` or `automated_actor`
- `wallet_tx_count`
  - count txs where `wallet_involved = true`
- `deployment_tx_count`
  - count txs classified as deployment-like activity
- `classic_tx_count`
  - count txs with no Soroban call involvement or explicitly classified classic-only activity
- `soroban_tx_count`
  - count txs with Soroban operation involvement

---

## Implementation sources

This endpoint should ideally be composed from three existing or near-existing sources:

### 1. Ledger totals source
Used for:
- transaction counts
- operation counts
- success/fail counts
- fee totals
- contract event totals

### 2. Semantic transaction classifications grouped by ledger
Used for:
- semantic composition counts
- agent/activity counts
- dominant tx type

Materialized or pre-aggregated sources are preferred when possible.

### 3. Ledger Soroban aggregate source
Used for:
- instructions usage
- read/write usage
- rent burned
- utilization percentages

Existing Soroban ledger aggregate work should be reused where possible.

---

## Suggested Go types

```go
type LedgerSummaryResponse struct {
    Ledger               LedgerSummaryLedger              `json:"ledger"`
    Totals               LedgerSummaryTotals              `json:"totals"`
    ClassificationCounts LedgerSummaryClassifications     `json:"classification_counts"`
    SorobanUtilization   *LedgerSummarySorobanUtilization `json:"soroban_utilization,omitempty"`
    Composition          *LedgerSummaryComposition        `json:"composition,omitempty"`
    Provenance           LedgerSummaryProvenance          `json:"provenance"`
}

type LedgerSummaryLedger struct {
    Sequence        int64  `json:"sequence"`
    ClosedAt        string `json:"closed_at"`
    ProtocolVersion int    `json:"protocol_version,omitempty"`
    Hash            string `json:"hash,omitempty"`
    PreviousHash    string `json:"previous_hash,omitempty"`
}

type LedgerSummaryTotals struct {
    TransactionCount   int64 `json:"transaction_count"`
    SuccessfulTxCount  int64 `json:"successful_tx_count,omitempty"`
    FailedTxCount      int64 `json:"failed_tx_count,omitempty"`
    OperationCount     int64 `json:"operation_count,omitempty"`
    ContractEventCount int64 `json:"contract_event_count,omitempty"`
    SorobanOpCount     int64 `json:"soroban_op_count,omitempty"`
    TotalFeeCharged    int64 `json:"total_fee_charged,omitempty"`
}

type LedgerSummaryClassifications struct {
    SwapTxCount         int64 `json:"swap_tx_count,omitempty"`
    ContractCallTxCount int64 `json:"contract_call_tx_count,omitempty"`
    AgentTxCount        int64 `json:"agent_tx_count,omitempty"`
    PaymentTxCount      int64 `json:"payment_tx_count,omitempty"`
    WalletTxCount       int64 `json:"wallet_tx_count,omitempty"`
    DeploymentTxCount   int64 `json:"deployment_tx_count,omitempty"`
    ClassicTxCount      int64 `json:"classic_tx_count,omitempty"`
    SorobanTxCount      int64 `json:"soroban_tx_count,omitempty"`
}

type LedgerSummarySorobanUtilization struct {
    InstructionsUsed    int64   `json:"instructions_used,omitempty"`
    InstructionsLimit   int64   `json:"instructions_limit,omitempty"`
    InstructionsPct     float64 `json:"instructions_pct,omitempty"`
    ReadBytesUsed       int64   `json:"read_bytes_used,omitempty"`
    WriteBytesUsed      int64   `json:"write_bytes_used,omitempty"`
    ReadWriteBytesUsed  int64   `json:"read_write_bytes_used,omitempty"`
    ReadWriteBytesLimit int64   `json:"read_write_bytes_limit,omitempty"`
    ReadWritePct        float64 `json:"read_write_pct,omitempty"`
    RentBurned          float64 `json:"rent_burned,omitempty"`
}

type LedgerSummaryComposition struct {
    DominantTxType      string  `json:"dominant_tx_type,omitempty"`
    DominantTxTypeCount int64   `json:"dominant_tx_type_count,omitempty"`
    SorobanSharePct     float64 `json:"soroban_share_pct,omitempty"`
    FailedSharePct      float64 `json:"failed_share_pct,omitempty"`
}

type LedgerSummaryProvenance struct {
    ClassificationSource string `json:"classification_source,omitempty"`
    UtilizationSource    string `json:"utilization_source,omitempty"`
    Partial              bool   `json:"partial"`
}
```

---

## Documentation guidance

### Summary
Returns a compact, structured ledger-level summary suitable for explorer homepages and analytics dashboards.

### Description
Provides:
- ledger identity
- transaction and operation totals
- semantic transaction composition counts
- Soroban resource utilization
- provenance metadata

Designed for deterministic rendering without client-side fan-out.

---

## What this endpoint should not do

This endpoint should **not** include:
- long prose descriptions
- UI-specific labels
- Prism-only wording
- subjective framing such as “interesting” or “suspicious”
- page-specific presentation fields

It should remain a structured data product suitable for any consumer.

---

## Future extensions

Possible future fields that still fit this contract:
- top protocol in ledger
- dominant asset moved
- fee pressure indicators
- anomaly counts
- signer/admin action counts
- wallet-family composition

These should only be added if they remain deterministic and broadly reusable.
