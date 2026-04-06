# Explorer Events API

The Explorer Events endpoint provides a unified, enriched stream of all Soroban contract events — transfers, swaps, mints, burns, approves, oracle updates, and more — with contract name resolution, protocol attribution, and an extensible classification system.

Designed for block explorer UIs (like Prism), real-time dashboards, and protocol analytics.

---

## Quick Start

Get the latest events across all types:

```bash
curl -H "Authorization: Api-Key YOUR_API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/explorer/events?limit=10"
```

Get only swap events:

```bash
curl -H "Authorization: Api-Key YOUR_API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/explorer/events?type=swap&limit=10"
```

Get transfers and mints for a specific token by name:

```bash
curl -H "Authorization: Api-Key YOUR_API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/explorer/events?type=transfer,mint&contract_name=USDC&limit=10"
```

---

## Endpoint Reference

### GET /api/v1/explorer/events

Returns paginated contract events enriched with contract names, semantic type classification, and protocol attribution.

#### Query Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `type` | string | _(all)_ | Comma-separated event types. See [Event Types](#event-types) below. |
| `tab` | string | _(all)_ | UI shortcut: `transfers`, `swaps`, `mints_burns`, `contract_calls`. Ignored if `type` is set. |
| `contract_id` | string | | Filter by contract ID (C... address or hex hash) |
| `contract_name` | string | | Search by token/contract name (case-insensitive substring match) |
| `tx_hash` | string | | Filter by transaction hash |
| `topic_match` | string | | Substring match across all decoded topics (case-insensitive) |
| `topic0` | string | | Exact match on topic position 0 |
| `topic1` | string | | Exact match on topic position 1 |
| `topic2` | string | | Exact match on topic position 2 |
| `topic3` | string | | Exact match on topic position 3 |
| `start_ledger` | int | | Only events at or after this ledger |
| `end_ledger` | int | | Only events at or before this ledger |
| `limit` | int | 20 | Max results per page (max: 200) |
| `cursor` | string | | Pagination cursor from previous response |
| `order` | string | `desc` | Sort order: `asc` or `desc` |

#### Response

```json
{
  "meta": {
    "matched_count": 2847,
    "count_capped": false,
    "ledger_range": { "min": 5104892, "max": 5104938 },
    "events_per_second": null
  },
  "events": [
    {
      "event_id": "bf2a...1b:0:0",
      "type": "transfer",
      "protocol": "sep41",
      "contract_id": "CCW6...7YMK",
      "contract_name": "USDC:GBBD...LA5",
      "contract_symbol": "USDC",
      "ledger_sequence": 5104938,
      "transaction_hash": "bf2a...1b",
      "closed_at": "2026-04-06T12:00:00Z",
      "successful": true,
      "topic0": "transfer",
      "topic1": "GDEF...9R",
      "topic2": "GHU...2M",
      "topic3": "native",
      "topics_decoded": "[\"transfer\", ...]",
      "data": "{\"value\":\"2500000000\",\"type\":\"i128\"}",
      "data_decoded": "{\"value\":\"2500000000\",\"type\":\"i128\"}",
      "event_index": 0,
      "operation_index": 0
    }
  ],
  "count": 1,
  "has_more": true,
  "next_cursor": "5104938:0"
}
```

**Key fields:**
- `type` — Classified event type (from rules, not raw topic0)
- `protocol` — Which protocol/standard emitted this event (e.g., `sep41`, `soroswap`)
- `contract_name` / `contract_symbol` — Resolved from token_registry (null if unknown)
- `topic0`–`topic3` — Raw decoded topic values for frontend rendering
- `data` / `data_decoded` — Raw event data payload

**Meta block:**
- `matched_count` — Total events matching filters (capped at 10,000 for performance)
- `count_capped` — True if the real count exceeds 10,000
- `ledger_range` — Min/max ledger sequence in the result set

---

## Event Types

Event types are **not hardcoded** — they come from classification rules stored in the database. The currently loaded types can be inspected via the rules endpoint.

Default types shipped with the system:

| Type | Protocol | Description |
|------|----------|-------------|
| `transfer` | sep41 | SEP-41 token transfers |
| `mint` | sep41 | SEP-41 token mints |
| `burn` | sep41 | SEP-41 token burns |
| `approve` | sep41 | SEP-41 token approvals |
| `swap` | soroswap | Soroswap DEX swap events |
| `contract_call` | _(none)_ | Catch-all for unclassified events |

New types can be added at any time without code changes. See [Classification Rules](#classification-rules) below.

---

## Polling Pattern

The endpoint is designed for polling-based real-time updates:

1. **Initial load**: `GET /api/v1/explorer/events?order=desc&limit=20`
2. **Load newer events**: Poll with `start_ledger` set to the highest ledger from your current data
3. **Load older events**: Use `next_cursor` from the previous response
4. **Filtered views**: Add `type`, `contract_id`, `tab`, or topic filters

Recommended poll interval: 1–2 seconds for a real-time feel.

---

## Classification Rules

### How Classification Works

Every contract event flowing through the explorer endpoint is classified by a set of **priority-ordered rules** stored in the `event_classification_rules` table in silver_hot.

```
Event arrives → Check rules in priority order (highest first) → First match wins
```

Each rule can match on:
- **Contract IDs** — Which contract emitted the event (C... address)
- **topic0 values** — The first topic of the event (e.g., `transfer`, `swap`, `REDSTONE`)
- **Topic signature regex** — A regex pattern matched against the full decoded topics string

If no rule matches, the event falls through to the lowest-priority catch-all rule (typically `contract_call`).

### Viewing Current Rules

```bash
curl -H "Authorization: Api-Key YOUR_API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/explorer/events/rules"
```

```json
{
  "count": 6,
  "rules": [
    {
      "rule_id": 1,
      "priority": 100,
      "event_type": "swap",
      "protocol": "soroswap",
      "match_contracts": ["CAG5LRYQ5JVEUI5TEID72EYOVX44TTUJT5BQR2J6J77FH65PCCFAJDDH"],
      "match_topic0": ["swap"],
      "description": "Soroswap router swap events"
    },
    {
      "rule_id": 2,
      "priority": 10,
      "event_type": "transfer",
      "protocol": "sep41",
      "match_topic0": ["transfer"],
      "description": "SEP-41 token transfer events"
    }
  ]
}
```

### Adding a New Rule

Adding support for a new protocol requires **no code changes and no redeployment**. Just insert a row and reload.

**Example: Classify Aquarius DEX trades**

```sql
INSERT INTO event_classification_rules
  (priority, event_type, protocol, match_contracts, match_topic0, description)
VALUES
  (100, 'swap', 'aquarius',
   ARRAY['CAQARIUS_ROUTER_CONTRACT_ID_HERE'],
   ARRAY['trade'],
   'Aquarius DEX trade events');
```

**Example: Classify RedStone oracle price feeds**

```sql
INSERT INTO event_classification_rules
  (priority, event_type, protocol, match_topic0, description)
VALUES
  (20, 'oracle_update', 'redstone',
   ARRAY['REDSTONE'],
   'RedStone oracle price feed updates');
```

**Example: Classify events using a regex on topic signature**

```sql
INSERT INTO event_classification_rules
  (priority, event_type, protocol, match_topic_sig, description)
VALUES
  (50, 'governance_vote', 'dao_protocol',
   '"vote".*"proposal_id"',
   'DAO governance voting events');
```

After inserting, reload the rules:

```bash
curl -X POST -H "Authorization: Api-Key YOUR_API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/explorer/events/rules/reload"
```

The new type is immediately available as a filter:

```bash
curl -H "Authorization: Api-Key YOUR_API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/explorer/events?type=oracle_update&limit=5"
```

### Rule Schema Reference

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `priority` | int | yes | Higher = checked first. Use 100+ for protocol-specific, 10 for generic standards, 0 for catch-all. |
| `event_type` | text | yes | The classified type name. Becomes a valid `type` filter value. |
| `protocol` | text | no | Protocol/standard attribution (e.g., `soroswap`, `sep41`, `aquarius`). Returned in the `protocol` response field. |
| `match_contracts` | text[] | no | Array of contract IDs (C... format) to match. NULL = match any contract. |
| `match_topic0` | text[] | no | Array of topic0 values to match. NULL = match any topic0. |
| `match_topic_sig` | text | no | Regex pattern applied to `topics_decoded`. NULL = skip regex check. |
| `description` | text | no | Human-readable description of what this rule detects. |
| `enabled` | bool | yes | Set to `false` to disable without deleting. Default: `true`. |

### Rule Matching Logic

Rules are evaluated in `priority DESC, rule_id ASC` order. A rule matches if **all** of its non-NULL conditions match:

- If `match_contracts` is set: the event's contract_id must be in the array
- If `match_topic0` is set: the event's topic0 must be in the array
- If `match_topic_sig` is set: the event's decoded topics string must match the regex
- If all conditions are NULL: the rule matches everything (use for catch-all)

**Priority guidelines:**

| Priority | Use For |
|----------|---------|
| 100+ | Protocol-specific rules (Soroswap, Aquarius, etc.) |
| 50–99 | Domain-specific patterns (oracle feeds, governance) |
| 10–49 | Generic standards (SEP-41 token events) |
| 0 | Default catch-all (`contract_call`) |

Higher-priority rules are checked first. If a Soroswap contract emits a `transfer` event, the Soroswap rule (priority 100) would match before the generic SEP-41 transfer rule (priority 10) — but only if the Soroswap rule is configured to match `transfer` in its `match_topic0`.

### Disabling or Removing Rules

Disable a rule (keeps it for re-enabling later):

```sql
UPDATE event_classification_rules SET enabled = false WHERE rule_id = 7;
```

Delete a rule permanently:

```sql
DELETE FROM event_classification_rules WHERE rule_id = 7;
```

Always reload after changes:

```bash
curl -X POST -H "Authorization: Api-Key YOUR_API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/explorer/events/rules/reload"
```

---

## Contract Registry

The contract registry gives every contract a human-readable name — tokens, DEXes, oracles, bridges, anything. Without it, Prism shows raw C... addresses or "Unknown".

### How it works

The `contract_registry` table is the single source of truth for contract identity:

| Field | Description |
|-------|-------------|
| `contract_id` | C... address (primary key) |
| `display_name` | Human-readable name shown in UIs |
| `category` | Contract type: `token`, `dex`, `oracle`, `bridge`, `dao`, `nft`, `game`, `utility` |
| `project` | Project name: `soroswap`, `redstone`, `aquarius`, `blend`, etc. |
| `verified` | Manually verified by operators |
| `source` | How the entry was created: `token_registry`, `manual`, `community` |

The explorer events endpoint uses `contract_registry` to populate `contract_name`, `contract_symbol`, and `contract_category` in every event.

**Relationship to classification rules**: Classification rules answer "what happened?" (event type + protocol). The contract registry answers "who is this?" (display name + category). They're separate concerns used together by the explorer endpoint.

### Viewing registry entries

Look up a single contract:

```bash
curl -H "Authorization: Api-Key YOUR_API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/explorer/contracts/CD4KFB23CQLJ47RIPESVBTQ444R75M6QVEZULWXHHFYYZT7SS2VGXOPW"
```

```json
{
  "contract_id": "CD4KFB23CQLJ47RIPESVBTQ444R75M6QVEZULWXHHFYYZT7SS2VGXOPW",
  "display_name": "RedStone Oracle",
  "category": "oracle",
  "project": "redstone",
  "verified": true,
  "source": "manual",
  "created_at": "2026-04-06T19:05:36Z",
  "updated_at": "2026-04-06T19:05:36Z"
}
```

List contracts by category:

```bash
curl -H "Authorization: Api-Key YOUR_API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/explorer/contracts?category=dex"
```

Search by name or project:

```bash
curl -H "Authorization: Api-Key YOUR_API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/explorer/contracts/search?q=soroswap"
```

### Adding a contract to the registry

**Via API** (no database access needed):

```bash
curl -X POST -H "Authorization: Api-Key YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "contract_id": "CABC...XYZ",
    "display_name": "Aquarius AMM",
    "category": "dex",
    "project": "aquarius"
  }' \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/explorer/contracts"
```

**Via SQL** (for bulk operations):

```sql
INSERT INTO contract_registry (contract_id, display_name, category, project, verified, source)
VALUES ('CABC...XYZ', 'Aquarius AMM', 'dex', 'aquarius', true, 'manual')
ON CONFLICT (contract_id) DO UPDATE SET
    display_name = EXCLUDED.display_name,
    category = EXCLUDED.category,
    project = EXCLUDED.project,
    updated_at = NOW();
```

Changes take effect immediately — the explorer endpoint reads the registry on every request. No reload needed (unlike classification rules which are cached in memory).

### Updating an existing entry

POST the same `contract_id` with updated fields. Existing fields not provided in the request are preserved:

```bash
curl -X POST -H "Authorization: Api-Key YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "contract_id": "CD4KFB23CQLJ47RIPESVBTQ444R75M6QVEZULWXHHFYYZT7SS2VGXOPW",
    "display_name": "RedStone Price Feed",
    "verified": true
  }' \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/explorer/contracts"
```

### Removing an entry

```bash
curl -X DELETE -H "Authorization: Api-Key YOUR_API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/explorer/contracts/CABC...XYZ"
```

### Auto-seeding from token registry

New tokens added by the pipeline are not automatically added to the contract registry. To sync, call the seed endpoint:

```bash
curl -X POST -H "Authorization: Api-Key YOUR_API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/explorer/contracts/seed"
```

```json
{
  "status": "seeded",
  "new_entries": 3
}
```

This inserts any tokens from `token_registry` that don't already exist in `contract_registry`. It never overwrites existing entries.

### Contract Registry API Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/explorer/contracts/{id}` | GET | Look up a single contract |
| `/api/v1/explorer/contracts` | GET | List entries. Filters: `?category=`, `?project=`, `?source=`, `?limit=` |
| `/api/v1/explorer/contracts/search?q=` | GET | Search by display name or project |
| `/api/v1/explorer/contracts` | POST | Add or update an entry (JSON body) |
| `/api/v1/explorer/contracts/{id}` | DELETE | Remove an entry |
| `/api/v1/explorer/contracts/seed` | POST | Re-seed from token_registry |

---

## Examples

### Monitor all token activity for USDC

```bash
curl -H "Authorization: Api-Key YOUR_API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/explorer/events?type=transfer,mint,burn&contract_name=USDC&limit=20"
```

### Get events for a specific transaction

```bash
curl -H "Authorization: Api-Key YOUR_API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/explorer/events?tx_hash=YOUR_TX_HASH&limit=50"
```

### Filter events by sender address (topic1)

```bash
curl -H "Authorization: Api-Key YOUR_API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/explorer/events?type=transfer&topic1=GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3QJZNSR&limit=10"
```

### Browse events in a ledger range

```bash
curl -H "Authorization: Api-Key YOUR_API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/explorer/events?start_ledger=290000&end_ledger=295000&limit=20"
```

### Paginate through older events

```bash
# First page
curl -H "Authorization: Api-Key YOUR_API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/explorer/events?limit=20"
# Response includes: "next_cursor": "295698:0"

# Next page
curl -H "Authorization: Api-Key YOUR_API_KEY" \
  "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/explorer/events?limit=20&cursor=295698:0"
```
