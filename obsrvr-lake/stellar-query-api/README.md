# Stellar Query API

Unified query API for Stellar blockchain data across Bronze (raw), Silver (analytics), Semantic (meaning-oriented), and Gold (compliance) data layers. Transparently combines hot (PostgreSQL) and cold (DuckLake/Parquet) storage.

## Documentation

- **[Analyst Guide](./analyst-guide/index.md)** - Quick start, common queries, and architecture overview
- **[Data Model](./analyst-guide/data-model.md)** - Bronze/Silver/Semantic layers and Hot/Cold storage explained
- **[API Reference](./analyst-guide/common-queries.md)** - Complete endpoint documentation with examples
- **[Horizon Migration](./analyst-guide/horizon-migration.md)** - Side-by-side Horizon vs Query API mappings
- **[nebu for Analysts](./analyst-guide/nebu-for-analysts.md)** - Power user CLI guide
- **[vs Hubble](./analyst-guide/vs-hubble.md)** - When to use which tool

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│              Stellar Query API (Port 8092)                    │
├──────────────────────────────────────────────────────────────┤
│  Bronze │ Silver │ Semantic │ Gold │ Index Plane             │
├─────────┴────────┴──────────┴──────┴─────────────────────────┤
│  Hot/Cold Query Merging + Unified Silver Reader              │
├──────────────────────┬───────────────────────────────────────┤
│   Hot (PostgreSQL)   │   Cold (DuckLake on S3/B2)           │
│   Last ~7 days       │   Full history (Parquet)             │
└──────────────────────┴───────────────────────────────────────┘
```

The API automatically determines which storage layer(s) to query and merges results seamlessly. You don't need to think about where data lives.

## API Endpoints

All endpoints return JSON. See the [full API reference](./analyst-guide/common-queries.md) for parameters and examples.

### Bronze Layer (Raw Data)

| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/bronze/ledgers` | Raw ledger data by sequence range |
| `GET /api/v1/bronze/transactions` | Raw transaction data |
| `GET /api/v1/bronze/operations` | Raw operation data |
| `GET /api/v1/bronze/effects` | Raw effect data |
| `GET /api/v1/bronze/trades` | Raw DEX trade data |
| `GET /api/v1/bronze/accounts` | Raw account snapshots |
| `GET /api/v1/bronze/trustlines` | Raw trustline snapshots |
| `GET /api/v1/bronze/offers` | Raw DEX offer snapshots |
| `GET /api/v1/bronze/contract_events` | Raw Soroban contract events |
| `GET /api/v1/bronze/stats` | Network statistics (freshest data) |

### Silver Layer (Analytics-Ready)

**Accounts:**
| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/silver/accounts` | List/search accounts with sorting |
| `GET /api/v1/silver/accounts/current` | Current state for a specific account |
| `GET /api/v1/silver/accounts/history` | Historical account snapshots |
| `GET /api/v1/silver/accounts/top` | Top accounts by balance |
| `GET /api/v1/silver/accounts/signers` | Account signer configurations |
| `GET /api/v1/silver/accounts/{id}/balances` | All balances (XLM + trustlines) |
| `GET /api/v1/silver/accounts/{id}/offers` | Account DEX offers |
| `GET /api/v1/silver/accounts/{id}/activity` | Account activity feed |
| `GET /api/v1/silver/accounts/{id}/contracts` | Account contract interactions with call counts |
| `POST /api/v1/silver/accounts/batch` | Batch account lookup |

**Operations & Payments:**
| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/silver/operations/enriched` | Operations with full context |
| `GET /api/v1/silver/operations/soroban` | Soroban operations only |
| `GET /api/v1/silver/operations/soroban/by-function` | Filter by contract/function |
| `GET /api/v1/silver/payments` | Payment operations |
| `GET /api/v1/silver/transfers` | Unified token transfers (classic + SAC) |
| `GET /api/v1/silver/calls` | Soroban contract calls (alias) |

**Assets & Tokens:**
| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/silver/assets` | List all tracked assets |
| `GET /api/v1/silver/assets/{code}:{issuer}/holders` | Asset holder list |
| `GET /api/v1/silver/assets/{code}:{issuer}/stats` | Asset statistics |
| `GET /api/v1/silver/tokens/{contract_id}` | SEP-41 token metadata |
| `GET /api/v1/silver/tokens/{contract_id}/balances` | Token holder balances |
| `GET /api/v1/silver/tokens/{contract_id}/balance/{addr}` | Single holder balance |
| `GET /api/v1/silver/tokens/{contract_id}/transfers` | Token transfer history |
| `GET /api/v1/silver/address/{addr}/token-balances` | Address token portfolio |

**DEX & Prices:**
| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/silver/prices/pairs` | Available trading pairs |
| `GET /api/v1/silver/prices/{base}/{counter}/latest` | Latest trade price |
| `GET /api/v1/silver/prices/{base}/{counter}/ohlc` | OHLC candlestick data |

**Contracts:**
| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/silver/contracts/top` | Most active contracts |
| `GET /api/v1/silver/contracts/{id}/analytics` | Per-contract analytics |
| `GET /api/v1/silver/contracts/{id}/interface` | Detected contract ABI |
| `GET /api/v1/silver/smart-wallet/{id}` | SEP-50 smart wallet detection |

**Ledger Analysis:**
| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/silver/ledgers/{seq}/soroban` | Per-ledger Soroban resource aggregates (CPU, I/O, rent) |
| `GET /api/v1/silver/ledgers/{seq}/fees` | Per-ledger fee distribution and histogram |

**Events:**
| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/silver/events` | CAP-67 unified event stream |
| `GET /api/v1/silver/events/generic` | Raw contract events (all types) |
| `GET /api/v1/silver/events/by-contract` | Events for a specific contract |

**Transactions:**
| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/silver/tx/{hash}/decoded` | Human-readable transaction summary |
| `GET /api/v1/silver/tx/{hash}/diffs` | Balance and state diffs |
| `GET /api/v1/silver/tx/{hash}/events` | Transaction events |
| `GET /api/v1/silver/tx/{hash}/full` | Full transaction analysis |
| `GET /api/v1/silver/tx/{hash}/hierarchy` | Contract call hierarchy |
| `POST /api/v1/silver/tx/batch` | Batch transaction summaries |

**Statistics:**
| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/silver/stats/network` | Network-wide statistics |
| `GET /api/v1/silver/stats/soroban` | Soroban runtime statistics |
| `GET /api/v1/silver/fees/stats` | Fee percentiles and surge detection |
| `GET /api/v1/silver/fees/distribution` | Per-ledger fee histogram |

**Search:**
| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/silver/search` | Universal search across all data types |

### Semantic Layer (Meaning-Oriented)

Human-readable analytics that answer high-level questions without requiring Stellar internals knowledge.

| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/semantic/activities` | Unified on-chain activity feed (payments, contract calls, etc.) |
| `GET /api/v1/semantic/contracts` | Contract registry with type classification and usage stats |
| `GET /api/v1/semantic/contracts/functions` | Per-function call stats with optional time period filter |
| `GET /api/v1/semantic/contracts/functions/leaderboard` | Top functions across all contracts |
| `GET /api/v1/semantic/accounts/summary` | Account activity summary (operations, payments, contracts) |
| `GET /api/v1/semantic/flows` | Normalized value transfers (transfers, mints, burns) |

**Quick examples:**

```bash
# What happened on-chain recently?
curl -H "Authorization: Api-Key $API_KEY" \
  "$BASE/api/v1/semantic/activities?limit=10"

# What contracts exist and how active are they?
curl -H "Authorization: Api-Key $API_KEY" \
  "$BASE/api/v1/semantic/contracts?limit=10"

# What functions has a contract exposed, and how often are they called in the last 7 days?
curl -H "Authorization: Api-Key $API_KEY" \
  "$BASE/api/v1/semantic/contracts/functions?contract_id=CABCD...&period=7d"

# What's an account's activity summary?
curl -H "Authorization: Api-Key $API_KEY" \
  "$BASE/api/v1/semantic/accounts/summary?account_id=GABCD..."

# What value moved on-chain?
curl -H "Authorization: Api-Key $API_KEY" \
  "$BASE/api/v1/semantic/flows?asset_code=XLM&limit=10"
```

### Gold Layer (Compliance & Auditing)

| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/gold/compliance/balances` | Point-in-time balance snapshots |
| `GET /api/v1/gold/compliance/supply` | Supply timeline with audit trail |
| `GET /api/v1/gold/compliance/archives` | Compliance archive index |
| `GET /api/v1/gold/compliance/archives/{id}` | Archive details and artifacts |
| `GET /api/v1/gold/compliance/archives/{id}/download` | Download archive artifacts |
| `GET /api/v1/gold/compliance/lineage` | Audit lineage and checksums |

### Index Plane (Fast Lookups)

| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/index/tx/{hash}` | Transaction lookup by hash |
| `POST /api/v1/index/tx/lookup` | Batch transaction lookup |
| `GET /api/v1/index/health` | Index coverage statistics |
| `GET /api/v1/index/contracts/{id}/ledgers` | Ledgers containing contract events |
| `GET /api/v1/index/contracts/{id}/summary` | Contract event summary |
| `POST /api/v1/index/contracts/lookup` | Batch contract lookup |
| `GET /api/v1/index/contracts/health` | Contract index health |

### Other

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Service health check |

## Configuration

Edit `config.yaml` (or `config.silver.yaml` for Silver mode):

```yaml
service:
  name: stellar-query-api
  port: 8092
  read_timeout_seconds: 30
  write_timeout_seconds: 30

postgres:
  host: localhost
  port: 5434
  database: stellar_hot
  user: stellar
  password: stellar_dev_password
  sslmode: disable
  max_connections: 10

ducklake:
  catalog_path: "ducklake:postgres:postgresql://..."
  data_path: "s3://obsrvr-test-bucket-1/testnet_bronze/"
  catalog_name: testnet_catalog
  schema_name: bronze
  metadata_schema: bronze_meta
  aws_access_key_id: "..."
  aws_secret_access_key: "..."
  aws_region: "us-west-004"
  aws_endpoint: "https://s3.us-west-004.backblazeb2.com"

silver:
  hot:
    host: localhost
    port: 25060
    database: silver_hot
    user: doadmin
    password: "..."
    sslmode: require

query:
  default_limit: 100
  max_limit: 10000
  cache_ttl_seconds: 60
```

## Building

```bash
cd go
go mod tidy
go build -o ../bin/stellar-query-api
```

## Running

```bash
./bin/stellar-query-api -config config.yaml
```

## Dependencies

- Go 1.21+
- PostgreSQL connection (`github.com/lib/pq`)
- DuckDB Go bindings (`github.com/duckdb/duckdb-go/v2`)
- YAML config (`gopkg.in/yaml.v3`)
- Gorilla Mux (`github.com/gorilla/mux`)
