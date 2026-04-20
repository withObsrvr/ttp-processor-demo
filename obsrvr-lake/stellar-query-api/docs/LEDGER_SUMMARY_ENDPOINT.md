# Ledger Summary Endpoint

Purpose: define a structured ledger-level summary endpoint that can power ledger-first explorer surfaces such as Prism home page hero cards, dashboards, and analytics views.

This endpoint is intended to provide:
- ledger identity
- transaction and operation totals
- semantic composition counts
- Soroban utilization metrics
- representative transaction samples
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
- an expanded panel with representative transactions like:
  - one dominant contract call
  - one dominant swap
  - one dominant classic payment

without requiring multiple follow-up requests.

---

## Response shape

### Example response

```json
{
  "ledger": {
    "sequence": 52844212,
    "closed_at": "2026-04-17T18:46:53Z",
    "close_time_seconds": 4.9,
    "closed_by": "sdf-validator-1",
    "protocol_version": 23,
    "hash": "abc123...",
    "previous_hash": "def456..."
  },
  "totals": {
    "transaction_count": 254,
    "successful_tx_count": 240,
    "failed_tx_count": 14,
    "operation_count": 762,
    "contract_event_count": 98,
    "soroban_op_count": 141,
    "total_fee_charged": 128400
  },
  "classification_counts": {
    "swap_tx_count": 68,
    "contract_call_tx_count": 96,
    "agent_payment_tx_count": 38,
    "classic_tx_count": 50,
    "confidential_tx_count": 2,
    "payment_tx_count": 22,
    "wallet_tx_count": 14,
    "deployment_tx_count": 3,
    "soroban_tx_count": 141
  },
  "soroban_utilization": {
    "instructions_used": 384000000,
    "instructions_limit": 600000000,
    "instructions_pct": 60,
    "read_bytes_used": 210000,
    "write_bytes_used": 150000,
    "read_write_bytes_used": 360000,
    "read_write_bytes_limit": 514285,
    "read_write_pct": 70,
    "rent_burned": 24.8
  },
  "sampling": {
    "strategy": "one_per_dominant_kind",
    "sample_count": 3,
    "represented_transaction_count": 214,
    "total_transaction_count": 254
  },
  "representative_transactions": [
    {
      "tx_hash": "e20b2e306d...",
      "category": "contract_call",
      "category_label": "Contract Call",
      "coverage_count": 96,
      "classification": {
        "tx_type": "contract_call",
        "subtype": "loan_repayment",
        "confidence": "high"
      },
      "summary": {
        "description": "Blend's pool settled a loan repayment",
        "function_name": "submit",
        "protocol_label": "Blend V2"
      },
      "actors": {
        "primary_label": "Blend's pool",
        "primary_type": "protocol_pool",
        "secondary_label": "Blend V2"
      }
    },
    {
      "tx_hash": "9424170774...",
      "category": "swap",
      "category_label": "Swap",
      "coverage_count": 68,
      "classification": {
        "tx_type": "swap",
        "confidence": "high"
      },
      "summary": {
        "description": "Swapped 2642 XLM for 142.34 USDC",
        "sold_amount": "2642",
        "sold_asset": "XLM",
        "bought_amount": "142.34",
        "bought_asset": "USDC"
      },
      "actors": {
        "primary_label": "anonymous trader",
        "primary_type": "classic_account"
      }
    },
    {
      "tx_hash": "70520c9592...",
      "category": "classic_payment",
      "category_label": "Classic",
      "coverage_count": 50,
      "classification": {
        "tx_type": "simple_payment",
        "confidence": "high"
      },
      "summary": {
        "description": "Sent 714 XLM to another wallet",
        "amount": "714",
        "asset": "XLM"
      },
      "actors": {
        "primary_label": "classic payment",
        "primary_type": "classic_account"
      }
    }
  ],
  "composition": {
    "dominant_tx_type": "contract_call",
    "dominant_tx_type_count": 96,
    "soroban_share_pct": 56,
    "failed_share_pct": 6
  },
  "provenance": {
    "classification_source": "semantic_tx_materialized_v1",
    "utilization_source": "ledger_soroban_aggregates_v1",
    "sampling_source": "ledger_semantic_sampling_v1",
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
  "sequence": 52844212,
  "closed_at": "2026-04-17T18:46:53Z",
  "close_time_seconds": 4.9,
  "closed_by": "sdf-validator-1",
  "protocol_version": 23,
  "hash": "...",
  "previous_hash": "..."
}
```

### Required
- `sequence`
- `closed_at`

### Strongly recommended
- `close_time_seconds`
- `closed_by`

### Optional
- `protocol_version`
- `hash`
- `previous_hash`

---

## `totals`
Base ledger metrics.

```json
"totals": {
  "transaction_count": 254,
  "successful_tx_count": 240,
  "failed_tx_count": 14,
  "operation_count": 762,
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
Per-ledger transaction composition counts. These power chips and ledger composition summaries.

```json
"classification_counts": {
  "swap_tx_count": 68,
  "contract_call_tx_count": 96,
  "agent_payment_tx_count": 38,
  "classic_tx_count": 50,
  "confidential_tx_count": 2,
  "payment_tx_count": 22,
  "wallet_tx_count": 14,
  "deployment_tx_count": 3,
  "soroban_tx_count": 141
}
```

### Required for the expanded ledger-first inspiration mock
- `swap_tx_count`
- `contract_call_tx_count`
- `agent_payment_tx_count`
- `classic_tx_count`
- `confidential_tx_count`

### Strongly recommended
- `payment_tx_count`
- `wallet_tx_count`
- `deployment_tx_count`
- `soroban_tx_count`

### Important note on `agent_payment_tx_count`
This field must have a documented deterministic meaning.

Recommended definition:

> `agent_payment_tx_count` = number of transactions in the ledger where the semantic classifier identifies an agent-mediated payment or at least one actor classified as `agent` or `automated_actor` participates in a payment-like transfer.

If the term `agent payment` is not stable enough yet, consider using a more neutral field like:
- `automated_actor_payment_tx_count`

and letting downstream applications label it as “agent payments” if desired.

### Important note on `confidential_tx_count`
This field must also have a deterministic definition.
For example, it could represent transactions whose semantic classification or involved contracts indicate confidentiality-preserving transfer or settlement behavior.

---

## `soroban_utilization`
Resource usage and capacity metrics for the ledger.

```json
"soroban_utilization": {
  "instructions_used": 384000000,
  "instructions_limit": 600000000,
  "instructions_pct": 60,
  "read_bytes_used": 210000,
  "write_bytes_used": 150000,
  "read_write_bytes_used": 360000,
  "read_write_bytes_limit": 514285,
  "read_write_pct": 70,
  "rent_burned": 24.8
}
```

### Required for the inspiration mock
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

## `sampling`
Metadata describing how representative examples were selected and how much of the ledger they represent.

```json
"sampling": {
  "strategy": "one_per_dominant_kind",
  "sample_count": 3,
  "represented_transaction_count": 214,
  "total_transaction_count": 254
}
```

### Required for the expanded ledger row
- `strategy`
- `sample_count`
- `represented_transaction_count`
- `total_transaction_count`

### Why this matters
This supports UI text like:
- “One of each dominant kind”
- “the 3 samples below represent 214 of the 254 transactions in this ledger”

### Recommended `strategy` values
- `one_per_dominant_kind`
- `top_n_by_category`
- `highest_confidence_examples`

Initial recommendation:
- `one_per_dominant_kind`

---

## `representative_transactions`
A small set of example transactions chosen deterministically to represent the dominant transaction kinds in the ledger.

```json
"representative_transactions": [
  {
    "tx_hash": "e20b2e306d...",
    "category": "contract_call",
    "category_label": "Contract Call",
    "coverage_count": 96,
    "classification": {
      "tx_type": "contract_call",
      "subtype": "loan_repayment",
      "confidence": "high"
    },
    "summary": {
      "description": "Blend's pool settled a loan repayment",
      "function_name": "submit",
      "protocol_label": "Blend V2"
    },
    "actors": {
      "primary_label": "Blend's pool",
      "primary_type": "protocol_pool",
      "secondary_label": "Blend V2"
    }
  }
]
```

### Required fields per sample
- `tx_hash`
- `category`
- `category_label`
- `coverage_count`
- `classification`
- `summary.description`

### Strongly recommended fields per sample
- `classification.confidence`
- `summary.function_name`
- `summary.protocol_label`
- structured swap/payment fields
- actor labels/types

### Sample categories
Examples:
- `contract_call`
- `swap`
- `classic_payment`
- `agent_payment`
- `deployment`
- `confidential_transfer`

### Important design note
This field should remain structured enough that clients can:
- render a sample row directly
- rephrase it in their own tone
- link to the underlying tx

without needing to parse prose.

---

## `composition`
Optional helper metrics derived from the main totals and classification counts.

```json
"composition": {
  "dominant_tx_type": "contract_call",
  "dominant_tx_type_count": 96,
  "soroban_share_pct": 56,
  "failed_share_pct": 6
}
```

These are not required for the mock but can support richer ledger-first explorer experiences.

---

## `provenance`
Metadata describing where summary values came from and whether the payload is partial.

```json
"provenance": {
  "classification_source": "semantic_tx_materialized_v1",
  "utilization_source": "ledger_soroban_aggregates_v1",
  "sampling_source": "ledger_semantic_sampling_v1",
  "partial": false
}
```

### Recommended fields
- `classification_source`
- `utilization_source`
- `sampling_source`
- `partial`

### Why this matters
This helps preserve trust when:
- some ledgers are only partially indexed
- some counts are approximated or lagging
- some semantic materializations are unavailable
- sample selection had to degrade gracefully

---

## Minimal viable response

If implementing a minimal version first, this shape is sufficient to render a basic ledger-first mock and expanded sample area:

```json
{
  "ledger": {
    "sequence": 52844212,
    "closed_at": "2026-04-17T18:46:53Z"
  },
  "totals": {
    "transaction_count": 254,
    "operation_count": 762
  },
  "classification_counts": {
    "swap_tx_count": 68,
    "contract_call_tx_count": 96,
    "classic_tx_count": 50
  },
  "soroban_utilization": {
    "instructions_pct": 60,
    "read_write_pct": 70
  },
  "sampling": {
    "strategy": "one_per_dominant_kind",
    "sample_count": 3,
    "represented_transaction_count": 214,
    "total_transaction_count": 254
  },
  "representative_transactions": [
    {
      "tx_hash": "e20b2e306d...",
      "category": "contract_call",
      "category_label": "Contract Call",
      "coverage_count": 96,
      "summary": {
        "description": "Blend's pool settled a loan repayment"
      }
    }
  ],
  "provenance": {
    "partial": false
  }
}
```

However, the richer version is recommended.

---

## Counting semantics

## Core recommendation
Classification counts should be based on **unique transactions per ledger**, not operation counts or event counts.

### Why
The ledger summary is intended to answer:
- how many transactions in this ledger were swaps?
- how many were calls?
- how many were agent payments?
- how many were classic?

This is more intuitive and stable than counting raw operations or events.

---

## Counting model

### Recommended model: non-exclusive counters
A single transaction may increment multiple counters.

Example:
- a smart wallet swap transaction may count toward:
  - `swap_tx_count`
  - `contract_call_tx_count`
  - `agent_payment_tx_count`

This model is best for homepage chips and ledger composition summaries.

### Not recommended for this use case: exclusive buckets
Exclusive classification would force one tx into exactly one bucket, which makes the ledger less informative for this style of summary.

---

## Representative sample selection semantics

Representative transactions should be selected deterministically.

### Recommended selection policy
For each dominant ledger category:
1. prefer successful transactions
2. prefer highest-confidence semantic classifications
3. prefer transactions with the richest structured summary
4. avoid duplicate category coverage
5. prefer examples with recognizable actors/protocol labels when available

### Recommended result model
- one representative transaction per dominant category
- selected using deterministic ranking, not randomness

### Why
This keeps the endpoint:
- stable across refreshes
- explainable to downstream consumers
- suitable for analytics and explorer UIs

---

## Suggested classification definitions

These are illustrative and should be aligned with actual semantic-layer definitions.

- `swap_tx_count`
  - count txs whose semantic classifier yields a swap-like tx type
- `contract_call_tx_count`
  - count txs with Soroban contract invocation / contract-call semantic classification
- `agent_payment_tx_count`
  - count txs where the semantic classifier identifies payment-like activity involving an agent or automated actor
- `classic_tx_count`
  - count txs with classic-only activity and no Soroban invocation
- `confidential_tx_count`
  - count txs classified as confidentiality-preserving settlement/transfer activity
- `payment_tx_count`
  - count txs with simple payment/account-funding/payment-like semantics
- `wallet_tx_count`
  - count txs where `wallet_involved = true`
- `deployment_tx_count`
  - count txs classified as deployment-like activity
- `soroban_tx_count`
  - count txs with Soroban operation involvement

---

## Implementation sources

This endpoint should ideally be composed from four sources:

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
- dominant tx type
- category membership for representative sampling

Materialized or pre-aggregated sources are preferred when possible.

### 3. Ledger Soroban aggregate source
Used for:
- instructions usage
- read/write usage
- rent burned
- utilization percentages

### 4. Transaction semantic/receipt summary source
Used for:
- representative sample transaction summaries
- actor labels
- protocol/function labels
- structured sample details

---

## Suggested Go types

```go
type LedgerSummaryResponse struct {
    Ledger                   LedgerSummaryLedger               `json:"ledger"`
    Totals                   LedgerSummaryTotals               `json:"totals"`
    ClassificationCounts     LedgerSummaryClassifications      `json:"classification_counts"`
    SorobanUtilization       *LedgerSummarySorobanUtilization  `json:"soroban_utilization,omitempty"`
    Sampling                 *LedgerSummarySampling            `json:"sampling,omitempty"`
    RepresentativeTransactions []LedgerRepresentativeTx        `json:"representative_transactions,omitempty"`
    Composition              *LedgerSummaryComposition         `json:"composition,omitempty"`
    Provenance               LedgerSummaryProvenance           `json:"provenance"`
}

type LedgerSummaryLedger struct {
    Sequence         int64   `json:"sequence"`
    ClosedAt         string  `json:"closed_at"`
    CloseTimeSeconds float64 `json:"close_time_seconds,omitempty"`
    ClosedBy         string  `json:"closed_by,omitempty"`
    ProtocolVersion  int     `json:"protocol_version,omitempty"`
    Hash             string  `json:"hash,omitempty"`
    PreviousHash     string  `json:"previous_hash,omitempty"`
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
    AgentPaymentTxCount int64 `json:"agent_payment_tx_count,omitempty"`
    ClassicTxCount      int64 `json:"classic_tx_count,omitempty"`
    ConfidentialTxCount int64 `json:"confidential_tx_count,omitempty"`
    PaymentTxCount      int64 `json:"payment_tx_count,omitempty"`
    WalletTxCount       int64 `json:"wallet_tx_count,omitempty"`
    DeploymentTxCount   int64 `json:"deployment_tx_count,omitempty"`
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

type LedgerSummarySampling struct {
    Strategy                   string `json:"strategy"`
    SampleCount                int    `json:"sample_count"`
    RepresentedTransactionCount int64 `json:"represented_transaction_count"`
    TotalTransactionCount      int64  `json:"total_transaction_count"`
}

type LedgerRepresentativeTx struct {
    TxHash        string                           `json:"tx_hash"`
    Category      string                           `json:"category"`
    CategoryLabel string                           `json:"category_label,omitempty"`
    CoverageCount int64                            `json:"coverage_count,omitempty"`
    Classification LedgerRepresentativeClassification `json:"classification"`
    Summary       LedgerRepresentativeSummary      `json:"summary"`
    Actors        *LedgerRepresentativeActors      `json:"actors,omitempty"`
}

type LedgerRepresentativeClassification struct {
    TxType     string `json:"tx_type,omitempty"`
    Subtype    string `json:"subtype,omitempty"`
    Confidence string `json:"confidence,omitempty"`
}

type LedgerRepresentativeSummary struct {
    Description   string `json:"description"`
    FunctionName  string `json:"function_name,omitempty"`
    ProtocolLabel string `json:"protocol_label,omitempty"`
    SoldAmount    string `json:"sold_amount,omitempty"`
    SoldAsset     string `json:"sold_asset,omitempty"`
    BoughtAmount  string `json:"bought_amount,omitempty"`
    BoughtAsset   string `json:"bought_asset,omitempty"`
    Amount        string `json:"amount,omitempty"`
    Asset         string `json:"asset,omitempty"`
}

type LedgerRepresentativeActors struct {
    PrimaryLabel   string `json:"primary_label,omitempty"`
    PrimaryType    string `json:"primary_type,omitempty"`
    SecondaryLabel string `json:"secondary_label,omitempty"`
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
    SamplingSource       string `json:"sampling_source,omitempty"`
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
- representative transactions for dominant ledger categories
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
