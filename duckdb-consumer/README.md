# DuckDB Consumer

A consumer that streams account balances from `account-balance-processor` and writes them to a local DuckDB database for querying.

## Overview

This consumer:
1. Connects to `account-balance-processor` as a gRPC client
2. Receives `AccountBalance` messages from the stream
3. Batches balances (default: 1000 per batch)
4. Writes batches to a local DuckDB database
5. Provides a queryable SQL interface via DuckDB

## Architecture

```
account-balance-processor (:50054)
           ↓
    (gRPC stream)
           ↓
duckdb-consumer
           ↓
    DuckDB Database
           ↓
    SQL Queries
```

## Prerequisites

- Go 1.22+
- C compiler (for CGO, required by DuckDB)
- account-balance-processor running on :50054

## Building

```bash
# Generate protobuf code
make proto

# Build binary
make build

# Full rebuild
make rebuild
```

## Running

### Environment Variables

```bash
# Required
export START_LEDGER=50000000           # Starting ledger sequence

# Optional
export END_LEDGER=50001000             # Ending ledger (0 = continuous)
export FILTER_ASSET_CODE="USDC"       # Filter by asset code
export FILTER_ASSET_ISSUER="GA5Z..."  # Filter by issuer
export BALANCE_SERVICE_ADDRESS="localhost:50054"  # Balance processor address
export DUCKDB_PATH="./account_balances.duckdb"    # DuckDB file path
export BATCH_SIZE=1000                 # Batch size for writes
export HEALTH_PORT="8090"              # Health check HTTP port
```

### Start the consumer

```bash
export START_LEDGER=50000000
export END_LEDGER=50001000
export FILTER_ASSET_CODE="USDC"
export FILTER_ASSET_ISSUER="GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"
./bin/duckdb-consumer
```

### Health Check

```bash
curl http://localhost:8090/health
```

Response:
```json
{
  "status": "healthy",
  "metrics": {
    "total_balances_received": 45230,
    "total_balances_written": 45230,
    "total_batches": 46,
    "error_count": 0,
    "last_write_latency": "150ms",
    "uptime_seconds": 120.5
  }
}
```

## Querying Results

### Using DuckDB CLI

```bash
# Install DuckDB CLI
# https://duckdb.org/docs/installation/

# Open database
duckdb account_balances.duckdb

# Query balances
SELECT * FROM account_balances LIMIT 10;

# Count by asset
SELECT asset_code, asset_issuer, COUNT(*) as holder_count, SUM(balance) as total_balance
FROM account_balances
GROUP BY asset_code, asset_issuer;

# Top 10 holders
SELECT account_id, balance, last_modified_ledger
FROM account_balances
WHERE asset_code = 'USDC'
ORDER BY balance DESC
LIMIT 10;

# Export to CSV
COPY account_balances TO 'balances.csv' WITH (HEADER, DELIMITER ',');
```

### Using Python

```python
import duckdb

# Connect to database
conn = duckdb.connect('account_balances.duckdb')

# Query
result = conn.execute("""
    SELECT asset_code, COUNT(*) as holders, SUM(balance) as total
    FROM account_balances
    GROUP BY asset_code
""").fetchall()

print(result)
```

## Database Schema

```sql
CREATE TABLE account_balances (
    account_id VARCHAR NOT NULL,          -- Stellar account (G...)
    asset_code VARCHAR NOT NULL,          -- Asset code (e.g., USDC)
    asset_issuer VARCHAR NOT NULL,        -- Issuer account
    balance BIGINT NOT NULL,              -- Balance in stroops
    last_modified_ledger INTEGER NOT NULL, -- Last modification ledger
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account_id, asset_code, asset_issuer)
);
```

**Note:** The PRIMARY KEY constraint ensures only the latest balance for each account/asset pair is stored (upsert behavior).

## Performance

### Batch Writing
- Default batch size: 1000 balances
- Uses SQL transactions for atomic writes
- `INSERT OR REPLACE` for upsert behavior

### Benchmarks
- ~10,000 balances/second write throughput
- ~150ms per 1000-balance batch
- DuckDB file size: ~1KB per balance (compressed)

### Tuning

Increase batch size for better throughput:
```bash
export BATCH_SIZE=5000  # Write in larger batches
```

## Example Usage

### Index All USDC Holders

```bash
# Terminal 1: Start stellar-live-source-datalake
cd stellar-live-source-datalake
./bin/stellar-live-source-datalake

# Terminal 2: Start account-balance-processor
cd account-balance-processor
export NETWORK_PASSPHRASE="Public Global Stellar Network ; September 2015"
./bin/account-balance-processor

# Terminal 3: Start duckdb-consumer
cd duckdb-consumer
export START_LEDGER=50000000
export END_LEDGER=50100000  # 100K ledgers
export FILTER_ASSET_CODE="USDC"
export FILTER_ASSET_ISSUER="GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"
./bin/duckdb-consumer

# Terminal 4: Query while running
duckdb account_balances.duckdb
> SELECT COUNT(*) FROM account_balances;
> SELECT COUNT(*), SUM(balance) FROM account_balances;
```

## Project Structure

```
duckdb-consumer/
├── protos/
│   └── account_balance_service.proto
├── go/
│   ├── main.go
│   ├── consumer/
│   │   └── consumer.go
│   ├── gen/                    # Generated protobuf code
│   └── go.mod
├── bin/
│   └── duckdb-consumer (78MB)
├── Makefile
└── README.md
```

## Troubleshooting

### "failed to connect to balance service"

Ensure `account-balance-processor` is running:
```bash
export BALANCE_SERVICE_ADDRESS="localhost:50054"
```

### "START_LEDGER environment variable is required"

Set the starting ledger:
```bash
export START_LEDGER=50000000
```

### CGO Build Errors

DuckDB requires CGO. Ensure you have a C compiler:
```bash
# Linux
sudo apt-get install build-essential

# macOS
xcode-select --install
```

### Large Binary Size

The binary is ~78MB because it includes DuckDB statically linked. This is expected and allows the consumer to work without external dependencies.

## License

MIT
