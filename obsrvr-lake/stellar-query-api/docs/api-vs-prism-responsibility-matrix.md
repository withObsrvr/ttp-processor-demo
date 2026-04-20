# API vs Prism Responsibility Matrix

Purpose: define a durable boundary between the Obsrvr gateway/query API as a flagship developer product and Prism as an opinionated explorer application.

This document is intended to help answer questions like:

- What belongs in the API vs in Prism?
- How far should semantic enrichment go before it becomes product-specific narration?
- How do we keep the API broadly useful to builders while still making Prism highly human-readable?

---

## Core principle

The recommended boundary is:

- **Gateway/API provides structured, deterministic, composable intelligence**
- **Prism provides narrative, presentation, and opinionated UX**

In other words:

- the API should be rich
- the API should be semantic
- the API should be explorer-friendly
- the API should **not** become a Prism-specific prose engine

---

## Mental model

The current semantic-layer design already points in the right direction:

### Layer 1 — deterministic classification
Machine-usable labels derived from rules, templates, heuristics, and indexed facts.

Examples:
- `tx_type`
- `subtype`
- `wallet_involved`
- `effective_actor_type`
- `contract_type`
- `wallet_type`

### Layer 2 — structured context enrichment
Additional structured context that makes Layer 1 useful in real applications.

Examples:
- actors with roles
- asset movement structure
- call graph
- token metadata
- confidence
- evidence
- smart wallet provenance
- signed diffs

### Layer 3 — narration and presentation
The final human-facing interpretation layer.

Examples:
- headlines
- explanatory paragraphs
- badges
- “what stands out”
- narrative summaries
- product tone and voice

**Recommendation:**
- Layers 1 and 2 belong in the API
- Layer 3 belongs in Prism

---

## Responsibility matrix

| Concern | Bronze | Silver | Semantic | Prism |
|---|---|---|---|---|
| Raw chain facts | Yes | No | No | No |
| Normalized chain facts | Limited | Yes | Yes | No |
| Deterministic summaries | No | Yes | Yes | No |
| Classifications | No | Light | Yes | Light consumption |
| Confidence/evidence | No | Light | Yes | Yes, presentation layer |
| Actor roles | No | Limited | Yes | Presented |
| Wallet detection | No | Light/basic | Yes | Presented |
| Natural-language narration | No | Minimal only | No | Yes |
| Visual emphasis / badges / explorer copy | No | No | No | Yes |
| Subjective framing / editorial tone | No | No | No | Yes |

---

## What the API should own

### 1. Raw and canonical facts
These are the non-negotiable building blocks that should remain first-class API data.

Examples:
- ledger sequence
- tx hash
- operation type
- event type
- effect type
- source and destination accounts
- contract id
- asset code / issuer / token contract id
- amounts
- balances
- signer state
- trustline state

### 2. Deterministic normalization
These are facts that have been cleaned up, merged, or reshaped for downstream consumers.

Examples:
- unified transfers
- signed diffs
- decoded operations
- receipt payloads
- event unification
- effect aggregation
- wallet balance normalization
- account activity normalization

### 3. Deterministic semantic classification
These fields add real platform value and are broadly reusable across apps.

Examples:
- `tx_type`
- `subtype`
- `operation_types`
- `wallet_involved`
- `effective_actor_type`
- `contract_type`
- `wallet_type`
- `implementation`
- `protocol hints`
- `call_graph`

### 4. Structured context enrichment
These make the API meaning-oriented without crossing into product narration.

Examples:
- actors with roles
- asset movement context
- confidence
- evidence arrays
- involved contracts
- observed functions
- token metadata
- provenance / source-of-truth fields
- `partial` / `degraded` indicators

### 5. Compact factual summaries
Short, deterministic summaries are acceptable in the API when they are factual and mechanically derived.

Examples:
- `"Sent 0.29 DSD to GDQF...2X4D"`
- `"Added trustline for VNM"`
- `"Created account ... with 10,000 XLM"`

These are acceptable because they are:
- concise
- factual
- low-opinion
- reusable by multiple consumers

---

## What Prism should own

### 1. Human-readable presentation
Prism should decide how structured facts are surfaced to end users.

Examples:
- page titles
- section labels
- hero cards
- “Overview / Security / Activity” tabs
- badge colors and iconography
- row grouping and ordering

### 2. Narrative prose
Prism should turn structured facts into end-user-friendly narration.

Examples:
- “This transaction funded a new account...”
- “This looks like a routine wallet interaction...”
- “Delegated execution occurred through a smart wallet...”

### 3. Subjective framing
These are product-level decisions and should not live in the API.

Examples:
- “High confidence” label wording
- “routine”
- “important”
- “suspicious”
- “safe”
- “likely intended”

### 4. Product opinion
Prism should decide what matters most to the user.

Examples:
- which actors are prominent
- whether a warning should be shown
- whether multiple transfers should collapse into one transaction row
- whether to call something “payment” vs “transfer” in UI copy

---

## Endpoint-by-endpoint guidance

## `/api/v1/silver/tx/{hash}/receipt`

### API should own
- tx metadata
- decoded operations
- events
- effects
- signed diffs
- structured summary
- semantic classification
- actors
- assets
- evidence
- source/provenance metadata
- partial/materialized/source-version metadata

### API should avoid
- long human narratives
- UI-oriented labels
- Prism-specific badge text
- editorial sections like “what stands out”

### Prism should own
- hero title
- explanation cards
- actor pills
- event grouping/collapsing
- UX copy and tone
- fallback messaging

### Recommendation
Treat `/receipt` as the **single-call canonical structured transaction object**.
It should be rich in facts, not rich in prose.

---

## `/api/v1/silver/tx/{hash}/semantic`

### API should own
- `classification`
- `confidence`
- actor roles
- wallet involvement
- effective actor type
- assets
- operations
- events
- call graph
- optional diffs
- legacy summary for compatibility

### API should avoid
- product voice
- sentence-level storytelling
- app-specific narrative framing

### Prism should own
- turning semantic fields into:
  - narratives
  - evidence cards
  - actor chips
  - “why Prism thinks this” sections

### Recommendation
This endpoint should be the flagship **structured semantic intelligence** endpoint.
Lean hard into structure, not prose.

---

## `/api/v1/silver/tx/{hash}/full`

### API should own
- assembled decoded tx facts
- structured summary
- decoded operations and events
- resources
- contracts involved
- call graph if included

### API should avoid
- becoming a Prism page JSON blob

### Prism should own
- deciding which fields matter most in UI
- rendering details progressively

---

## `/api/v1/silver/smart-wallets/{contract_id}`

### API should own
- detection status
- provenance
- implementation
- wallet type
- signers if decoded
- approval model facts
- policy/admin activity facts
- balance facts
- timeline facts
- partial/degraded flags

### API should avoid
- pretending unknown thresholds/policies are known
- heavy narrative like “this wallet is commonly used for...”
- UI-specific section names such as “Overview” or “Security”

### Prism should own
- section naming
- degraded-mode messaging
- humanized policy/admin activity text
- visual confidence/provenance presentation

### Recommendation
Smart wallet detail should be a **structured wallet profile**, not a pre-rendered explorer experience.

---

## `/api/v1/silver/smart-wallets/{contract_id}/balances`

### API should own
- canonical wallet balances
- symbol / decimals
- balance source
- freshness / provenance
- partial/confidence indicators

### Prism should own
- portfolio presentation
- sorting prominence
- iconography
- UI grouping

---

## `/api/v1/silver/accounts/{id}/activity`

### API should own
- normalized activity items
- type/subtype
- actor references
- tx hash
- timestamp / ledger
- amount / asset
- source-of-classification
- confidence if useful

### API should avoid
- final UI semantics like “Security” vs “Activity” unless they are truly machine-meaningful categories

### Prism should own
- tab model
- whether to merge account activity with wallet activity
- human phrases like “Sent”, “Received”, “Moved within wallet”

---

## `/api/v1/silver/transfers`

### API should own
- transfer facts
- directionality
- token metadata
- canonical identities
- strong filtering and pagination

### API should avoid
- trying to explain user intent

### Prism should own
- grouping multiple transfer events into one human transaction row
- feed copy
- “transaction vs transfer event” semantics in UI

---

## `/api/v1/semantic/contracts`

### API should own
- `contract_type`
- `wallet_type`
- `observed_functions`
- usage metrics
- deployer/activity
- token metadata

### API should avoid
- qualitative labels such as “popular”, “trusted”, or “important”

### Prism should own
- directory presentation
- tags/badges
- “Top contracts” copy

---

## `/api/v1/semantic/activities`

### API should own
- normalized high-level activity facts
- actor references
- tx type
- assets
- timestamp
- evidence/confidence if available

### API should avoid
- final end-user narration

### Prism should own
- feed wording
- grouping/collapsing
- home page presentation

---

## `/api/v1/semantic/flows`

### API should own
- value movement facts
- flow type
- source/target identities
- asset identities
- amount and direction

### API should avoid
- trader/investor interpretation
- protocol success narratives

### Prism should own
- flow visualizations
- summaries
- storytelling

---

## Field-level rule of thumb

### Put a field in the API if it is:
- deterministic
- reproducible
- inspectable
- broadly useful to non-Prism consumers
- stable under rewording
- easier to compute near the data than in clients

Examples:
- `tx_type`
- `subtype`
- `confidence`
- `actors[].roles`
- `wallet_involved`
- `effective_actor_type`
- `summary.transfer`
- `summary.swap`
- `evidence[]`
- `partial`
- `source_of_truth`

### Put a field in Prism if it is:
- prose
- UI-oriented
- product-voice-heavy
- audience-specific
- likely to vary by application

Examples:
- `human_title`
- `human_narrative`
- `signals`
- `what_stands_out`
- `severity_label`
- `hero_copy`
- `badge_text`
- `trust_message`

---

## Good additions to gateway

These are examples of fields and concepts that strengthen the flagship API without making it too subjective:

- `evidence`
- `confidence`
- `actors`
- `assets`
- `call_graph`
- `wallet_type`
- `implementation`
- `partial`
- `source_version`
- `source_of_truth`
- structured `summary.transfer`
- structured `summary.swap`
- structured `summary.mint`
- structured `summary.burn`
- signed `diffs`
- normalized effects

---

## Risky additions to gateway

These are examples of fields that likely belong in Prism instead:

- `human_title`
- `human_narrative`
- `signals`
- `what_stands_out`
- `risk_label`
- `routine_activity`
- `suspicious`
- `important`
- long friendly descriptions with product voice

---

## Product positioning recommendation

The flagship API should aim to provide:

- **canonical facts**
- **normalized entities**
- **deterministic classifications**
- **structured semantic context**
- **portable evidence**

Prism should aim to provide:

- **human explanation**
- **opinionated simplification**
- **narrative**
- **UI semantics**
- **editorial emphasis**

This keeps the API broadly useful to builders while allowing Prism to be the most human-readable explorer possible.

---

## Acceptance checklist for new API fields

When adding a new field to the gateway/query API, check whether it satisfies at least one of these:

1. Useful to non-Prism consumers
2. Deterministic and reproducible
3. Easier to compute near the data than in clients
4. Structured rather than editorial
5. Improves correctness, not just friendliness

If a proposed field does **not** satisfy any of these, it probably belongs in Prism instead.

---

## Short-form summary

| Category | Gateway/API | Prism |
|---|---|---|
| Facts | Owns | Consumes |
| Normalization | Owns | Consumes |
| Classification | Owns | Consumes/interprets |
| Evidence/confidence | Owns | Displays |
| Narration | Avoid | Owns |
| UX labels/badges | Avoid | Owns |
| Subjective framing | Avoid | Owns |

---

## Final guidance

A premium API does **not** have to be raw-only.

The right target is:
- rich semantics
- strong normalization
- deterministic interpretation
- minimal editorial tone

That lets Obsrvr ship a flagship developer product that many builders can trust and compose, while Prism remains free to be highly humanized, expressive, and opinionated.
