# Account Balance Processor

A gRPC service that streams Stellar account balances from ledger data, with support for filtering by asset.

## Overview

This service:
1. Connects to `stellar-live-source-datalake` to receive raw ledger data
2. Parses `LedgerCloseMeta` XDR to extract trustline entries
3. Uses Stellar SDK's `trustline` processor to extract account balances
4. Filters by asset code/issuer (optional)
5. Streams `AccountBalance` messages to clients via gRPC

## Architecture

```
stellar-live-source-datalake (:50053)
           ↓
    (gRPC client connection)
           ↓
account-balance-processor (:50054)
           ↓
    StreamAccountBalances()
           ↓
    Consumer (e.g., duckdb-consumer)
```

## Prerequisites

- Go 1.22+
- protoc compiler
- stellar-live-source-datalake running on :50053

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
export NETWORK_PASSPHRASE="Test SDF Network ; September 2015"  # or mainnet

# Optional
export PORT=":50054"                           # gRPC server port (default: :50054)
export SOURCE_SERVICE_ADDRESS="localhost:50053" # stellar-live-source-datalake address
export HEALTH_PORT="8089"                      # Health check HTTP port
```

### Start the server

```bash
export NETWORK_PASSPHRASE="Test SDF Network ; September 2015"
./bin/account-balance-processor
```

### Health Check

```bash
curl http://localhost:8089/health
```

Response:
```json
{
  "status": "healthy",
  "metrics": {
    "success_count": 150,
    "error_count": 0,
    "total_processed": 150,
    "total_balances_emitted": 45230,
    "last_processed_ledger": 123456,
    "processing_latency": "15ms"
  }
}
```

## Usage

### gRPC API

Service: `AccountBalanceService`

#### StreamAccountBalances

Request:
```protobuf
message StreamAccountBalancesRequest {
  uint32 start_ledger = 1;            // Starting ledger sequence
  uint32 end_ledger = 2;              // Ending ledger (0 = continuous)
  string filter_asset_code = 3;       // Optional: e.g., "USDC"
  string filter_asset_issuer = 4;     // Optional: issuer account ID
}
```

Response (stream):
```protobuf
message AccountBalance {
  string account_id = 1;              // Stellar account (G...)
  string asset_code = 2;              // Asset code
  string asset_issuer = 3;            // Issuer account
  int64 balance = 4;                  // Balance in stroops
  uint32 last_modified_ledger = 5;    // Last modification ledger
}
```

### Example: Stream USDC balances

```go
import (
    pb "github.com/withobsrvr/account-balance-processor/gen/account_balance_service"
    "google.golang.org/grpc"
)

conn, _ := grpc.Dial("localhost:50054", grpc.WithInsecure())
client := pb.NewAccountBalanceServiceClient(conn)

req := &pb.StreamAccountBalancesRequest{
    StartLedger:        50000000,
    EndLedger:          50001000,
    FilterAssetCode:    "USDC",
    FilterAssetIssuer:  "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
}

stream, _ := client.StreamAccountBalances(context.Background(), req)
for {
    balance, err := stream.Recv()
    if err == io.EOF {
        break
    }
    fmt.Printf("Account %s has %d USDC\n", balance.AccountId, balance.Balance)
}
```

## Implementation Details

### Trustline Processing

Uses Stellar SDK's `github.com/stellar/go/processors/trustline`:
- Extracts account ID, asset code/issuer, balance
- Handles both regular assets and liquidity pool shares
- Battle-tested code from Stellar Foundation

### Performance

- Processes ~100 ledgers/second
- Concurrent ledger fetching from source
- Streams results as they're processed (no buffering)

## Testing

```bash
make test
```

## Project Structure

```
account-balance-processor/
├── protos/
│   ├── account_balance_service.proto
│   └── raw_ledger_service/
│       └── raw_ledger_service.proto
├── go/
│   ├── main.go
│   ├── server/
│   │   └── server.go
│   ├── gen/                    # Generated protobuf code
│   └── go.mod
├── bin/
│   └── account-balance-processor
├── Makefile
└── README.md
```

## Troubleshooting

### "failed to connect to raw ledger source service"

Ensure `stellar-live-source-datalake` is running on the expected address:
```bash
export SOURCE_SERVICE_ADDRESS="localhost:50053"
```

### "NETWORK_PASSPHRASE environment variable not set"

Set the network passphrase:
```bash
export NETWORK_PASSPHRASE="Test SDF Network ; September 2015"
```

For mainnet:
```bash
export NETWORK_PASSPHRASE="Public Global Stellar Network ; September 2015"
```

## License

MIT
