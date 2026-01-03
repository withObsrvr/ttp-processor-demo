# Contract Event Index API

The Contract Event Index provides fast lookups for which ledgers contain events from specific smart contracts on the Stellar network. This is useful for continuous auditing, event monitoring, and contract activity tracking.

## Base URL

```
http://localhost:8092/api/v1/index/contracts
```

## Authentication

Public read-only access (no authentication required for alpha release).

## Endpoints

### 1. Get Ledgers for Contract

Returns a list of ledgers containing events from a specific contract.

**Endpoint:** `GET /api/v1/index/contracts/{contract_id}/ledgers`

**URL Parameters:**
- `contract_id` (required): The contract address (e.g., `CABC123...`)

**Query Parameters:**
- `start_ledger` (optional): Minimum ledger sequence to include
- `end_ledger` (optional): Maximum ledger sequence to include
- `limit` (optional): Maximum number of results to return (default: 1000, max: 10000)

**Example Request:**
```bash
curl "http://localhost:8092/api/v1/index/contracts/CABC123.../ledgers?start_ledger=280000&limit=100"
```

**Example Response:**
```json
{
  "contract_id": "CABC123...",
  "ledgers": [280015, 280234, 280567, 281003, 281245],
  "total": 5
}
```

**Use Case - Cursor-Based Continuous Auditing:**
```bash
# Start from ledger 0, get first batch
curl "http://localhost:8092/api/v1/index/contracts/STELLARCARBON.../ledgers?start_ledger=0&limit=100" > batch1.json

# Get last ledger from batch1
LAST_LEDGER=$(jq -r '.ledgers[-1]' batch1.json)

# Get next batch starting from last ledger
curl "http://localhost:8092/api/v1/index/contracts/STELLARCARBON.../ledgers?start_ledger=$LAST_LEDGER&limit=100" > batch2.json
```

### 2. Get Contract Event Summary

Returns detailed event information including event counts for each ledger.

**Endpoint:** `GET /api/v1/index/contracts/{contract_id}/summary`

**URL Parameters:**
- `contract_id` (required): The contract address

**Query Parameters:**
- `start_ledger` (optional): Minimum ledger sequence to include
- `end_ledger` (optional): Maximum ledger sequence to include

**Example Request:**
```bash
curl "http://localhost:8092/api/v1/index/contracts/CABC123.../summary?start_ledger=280000&end_ledger=281000"
```

**Example Response:**
```json
{
  "contract_id": "CABC123...",
  "summary": [
    {
      "contract_id": "CABC123...",
      "ledger_sequence": 280015,
      "event_count": 3,
      "first_seen_at": "2025-01-02T10:30:00Z",
      "ledger_range": 2
    },
    {
      "contract_id": "CABC123...",
      "ledger_sequence": 280234,
      "event_count": 1,
      "first_seen_at": "2025-01-02T10:30:00Z",
      "ledger_range": 2
    }
  ],
  "total": 2
}
```

### 3. Batch Contract Lookup

Performs batch lookup for multiple contracts in a single request.

**Endpoint:** `POST /api/v1/index/contracts/lookup`

**Request Body:**
```json
{
  "contract_ids": ["CABC123...", "CDEF456...", "CXYZ789..."]
}
```

**Limits:**
- Maximum 100 contract_ids per request

**Example Request:**
```bash
curl -X POST "http://localhost:8092/api/v1/index/contracts/lookup" \
  -H "Content-Type: application/json" \
  -d '{
    "contract_ids": ["CABC123...", "CDEF456..."]
  }'
```

**Example Response:**
```json
{
  "results": [
    {
      "contract_id": "CABC123...",
      "ledgers": [280015, 280234, 280567],
      "total": 3
    },
    {
      "contract_id": "CDEF456...",
      "ledgers": [281003, 281245],
      "total": 2
    }
  ],
  "total": 2
}
```

### 4. Contract Index Health

Returns statistics about the Contract Event Index coverage and status.

**Endpoint:** `GET /api/v1/index/contracts/health`

**Example Request:**
```bash
curl "http://localhost:8092/api/v1/index/contracts/health"
```

**Example Response:**
```json
{
  "status": "healthy",
  "index": {
    "total_contract_ledger_pairs": 12543,
    "unique_contracts": 87,
    "min_ledger": 276592,
    "max_ledger": 283820,
    "ledger_coverage": 7228,
    "last_updated": "2025-01-02T10:40:00Z"
  }
}
```

## Data Model

### ContractLedgerInfo

Represents a ledger containing events from a contract.

```typescript
interface ContractLedgerInfo {
  contract_id: string;       // Contract address
  ledger_sequence: number;   // Ledger number
  event_count: number;       // Number of events in this ledger
  first_seen_at: string;     // ISO 8601 timestamp
  ledger_range: number;      // Partition key (ledger_sequence / 100000)
}
```

## Error Responses

All endpoints return standard HTTP error codes:

**400 Bad Request:**
```json
{
  "error": "invalid start_ledger"
}
```

**404 Not Found:**
```json
{
  "error": "transaction not found in index"
}
```

**500 Internal Server Error:**
```json
{
  "error": "failed to query contract index: ..."
}
```

## Use Cases

### 1. Continuous Contract Auditing

Monitor all events from a specific contract for compliance or auditing:

```bash
#!/bin/bash
CONTRACT_ID="STELLARCARBON..."
START_LEDGER=0

while true; do
  # Get next batch of ledgers
  RESPONSE=$(curl -s "http://localhost:8092/api/v1/index/contracts/${CONTRACT_ID}/ledgers?start_ledger=${START_LEDGER}&limit=100")

  LEDGERS=$(echo $RESPONSE | jq -r '.ledgers[]')

  if [ -z "$LEDGERS" ]; then
    echo "No more ledgers, waiting for new data..."
    sleep 30
    continue
  fi

  # Process each ledger
  for LEDGER in $LEDGERS; do
    echo "Processing ledger $LEDGER"
    # Fetch full events from ledger...
  done

  # Update cursor
  START_LEDGER=$(echo $RESPONSE | jq -r '.ledgers[-1]')
done
```

### 2. Contract Activity Dashboard

Build a dashboard showing contract activity over time:

```bash
# Get all ledgers for a contract
curl "http://localhost:8092/api/v1/index/contracts/MYCONTRACT.../summary" | \
  jq '.summary[] | {ledger: .ledger_sequence, events: .event_count}'
```

### 3. Multi-Contract Monitoring

Monitor multiple contracts simultaneously:

```bash
curl -X POST "http://localhost:8092/api/v1/index/contracts/lookup" \
  -H "Content-Type: application/json" \
  -d '{
    "contract_ids": ["CONTRACT1...", "CONTRACT2...", "CONTRACT3..."]
  }' | jq '.results[] | {contract: .contract_id, activity: .total}'
```

## Performance

- **Lookup Time:** O(1) for single contract lookups
- **Dataset Size:** ~MB per year per contract
- **Partitioning:** By ledger_range (ledger_sequence / 100000)
- **Storage:** Parquet files on B2/S3 via DuckLake

## Limitations

- Index is eventually consistent (30 second polling interval)
- Returns only ledger numbers, not full event details
- Event details must be fetched separately from Bronze/Silver layer
- No event filtering by type, topic, or data (returns all events)

## Integration with Bronze/Silver

After finding ledgers with the Contract Event Index, fetch full event details:

```bash
# Step 1: Find ledgers containing contract events
LEDGERS=$(curl -s "http://localhost:8092/api/v1/index/contracts/MYCONTRACT.../ledgers" | jq -r '.ledgers[]')

# Step 2: Fetch full event details from Bronze layer
for LEDGER in $LEDGERS; do
  curl "http://localhost:8092/contract_events?ledger_sequence=${LEDGER}&contract_id=MYCONTRACT..."
done
```

## Configuration

Enable Contract Event Index in `config.yaml`:

```yaml
contract_index:
  enabled: true
  catalog_host: "catalog.example.com"
  catalog_port: 25060
  catalog_database: "obsrvr_lake_catalog"
  catalog_user: "ducklake_user"
  catalog_password: "${CATALOG_PASSWORD}"
  s3_endpoint: "s3.us-west-004.backblazeb2.com"
  s3_region: "us-west-004"
  s3_bucket: "obsrvr-prod-testnet"
  s3_access_key_id: "${B2_KEY_ID}"
  s3_secret_access_key: "${B2_KEY_SECRET}"
```

## Architecture

```
Bronze Hot (PostgreSQL)          DuckLake (B2/Parquet)
contract_events_stream_v1  →  index.contract_events_index
       ↓ poll every 30s              ↓ query via DuckDB
[contract-event-index-transformer]  [stellar-query-api]
                                          ↓
                                   HTTP API Endpoints
```

## Support

For issues or questions:
- GitHub: https://github.com/withobsrvr/obsrvr-lake/issues
- Documentation: https://docs.obsrvr.com

## Changelog

### v1.0.0 (2025-01-03)
- Initial release with core endpoints
- GET `/contracts/{contract_id}/ledgers`
- GET `/contracts/{contract_id}/summary`
- POST `/contracts/lookup`
- GET `/contracts/health`
