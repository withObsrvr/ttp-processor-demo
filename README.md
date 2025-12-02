# TTP Processor Demo

This project demonstrates a microservices architecture for processing Token Transfer Protocol (TTP) events from the Stellar blockchain. The system processes ledger data from either a Stellar RPC endpoint or a data lake, extracts TTP events, and makes them available to consumer applications via gRPC.

## Quick Start - Contract Events Pipeline

**Process Stellar contract events to PostgreSQL:**

```bash
# Start the pipeline
./run-pipeline.sh

# Monitor processing
docker logs -f flowctl-pipeline-postgres-consumer

# Stop the pipeline
./stop-pipeline.sh
```

See [RUNNING_PIPELINE.md](RUNNING_PIPELINE.md) for complete documentation.

---

## Acknowledgments

This project is based on the original work from the Stellar Development Foundation's TTP as a Service PoC:
- [TTP as a Service PoC Pull Request](https://github.com/stellar/go/pull/5637)
- [Stellar Discord Discussion](https://discord.com/channels/897514728459468821/1275530054444781730/1355067897873170593)

The core architecture and initial implementation were inspired by these sources, with additional features and improvements added to support multiple data sources and enhanced processing capabilities.

## Architecture

The system is split into several components that work together:

```plaintext
+--------------------------+        +--------------------------+        +-----------------------------+
| Stellar Data Source      |        |   Service A:             |        |   Service B:                |
| (RPC or Data Lake)       |        | stellar-live-source      |        |   ttp-processor (Go)        |
+--------------------------+        +--------------------------+        +-----------------------------+
          ▲                         │                          │        │                             │
          │ RPC GetLedgers          │ 1. Connects & Polls      │        │ 4. Receives RawLedger       │
          │ or Storage Read         │ 2. Decodes Base64 Meta   │        │ 5. Unmarshals XDR           │
          └─────────────────────────┤ 3. Streams RawLedger msg │        │ 6. Processes TTP Events     │
                                    │   (via gRPC Stream)      │        │ 7. Streams TokenTransferEvent│
                                    +-------------▲------------+        +-------------▲---------------+
                                                  │ gRPC Stream                     │ gRPC Stream    
                                                  │ (RawLedger)                     │ (TokenTransferEvent)
                                                  │                                 │
                                                  │                                 │
                                            Consumer Applications
                                            (Connect via gRPC)

```

### Component Overview

1. **Data Sources**
   - **RPC Source**: Connects to a Stellar RPC endpoint and streams raw ledger data
   - **Data Lake Source**: Reads ledger data from storage (GCS, S3, or FS) and streams it

2. **Service A: `stellar-live-source` (Go Service)**
   - RPC-based service that connects to Stellar RPC endpoints
   - Handles continuous polling of the blockchain for new ledgers
   - Exposes a gRPC service that streams raw ledger data

3. **Service A: `stellar-live-source-datalake` (Go Service)**
   - Storage-based service that reads from data lakes
   - Uses `stellar-datastore` and `stellar-cdp` for efficient storage access
   - Exposes the same gRPC interface as the RPC service

4. **Service B: `ttp-processor` (Go Service)**
   - Consumes raw ledger data from either source service
   - Processes the data to extract Token Transfer Protocol (TTP) events
   - Exposes a gRPC service that streams the processed events

## Setup Instructions

### Prerequisites

- Go 1.21+
- Protocol Buffers compiler (protoc)
- Node.js 22+ (for example consumer)

### Install Protocol Buffers Compiler

- On Mac: `brew install protobuf`
- On Linux: `apt install protobuf-compiler`

### Install Go Protocol Buffers Plugins

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2
```

## Running the Services

### Option 1: Using RPC Source

1. **Start the Stellar Live Source Service (RPC)**

```bash
cd stellar-live-source
make build
RPC_ENDPOINT=https://soroban-testnet.stellar.org NETWORK_PASSPHRASE="Test SDF Network ; September 2015" ./stellar_live_source
```

### Option 2: Using Data Lake Source

1. **Start the Stellar Live Source Service (Data Lake)**

```bash
cd stellar-live-source-datalake
make build
STORAGE_TYPE="S3" BUCKET_NAME="my-stellar-ledgers" AWS_REGION="us-west-2" ./stellar_live_source_datalake
```

### 2. Start the TTP Processor Service

```bash
cd ttp-processor
make build
LIVE_SOURCE_ENDPOINT=localhost:50051 ./ttp_processor
```

### 3. Run the Example Consumer Application

The repository includes a Node.js example client that demonstrates how to connect to the TTP processor and consume events:

```bash
cd consumer_app/node
npm run build
npm start -- <start_ledger> <end_ledger>
```

Or with the compiled JavaScript:

```bash
node dist/index.js <start_ledger> <end_ledger>
```

## Configuration

### RPC Source Configuration

- `RPC_ENDPOINT`: URL of the Stellar RPC endpoint
- `NETWORK_PASSPHRASE`: Network passphrase for the Stellar network

### Data Lake Source Configuration

- `STORAGE_TYPE`: Type of storage backend ("GCS", "S3", or "FS")
- `BUCKET_NAME`: Name of the bucket or path to the data
- `AWS_REGION`: AWS region (required for S3)
- `S3_ENDPOINT_URL`: Custom S3 endpoint URL (optional)
- `S3_FORCE_PATH_STYLE`: Set to "true" for non-AWS S3 (optional)
- `LEDGERS_PER_FILE`: Number of ledgers per file (default: 64)
- `FILES_PER_PARTITION`: Number of files per partition (default: 10)

### TTP Processor Configuration

- `LIVE_SOURCE_ENDPOINT`: Address of the live source service (default: localhost:50051)

## Implementation Details

### stellar-live-source (RPC)

This service connects to the Stellar RPC endpoint and streams raw ledger data. It:
- Uses `github.com/stellar/stellar-rpc/client` to connect to the RPC endpoint
- Polls for new ledgers continually using cursors
- Decodes the Base64-encoded ledger metadata into raw XDR bytes
- Streams the raw ledger data over gRPC to consumers

### stellar-live-source-datalake

This service reads ledger data from a data lake and streams it. It:
- Uses `stellar-datastore` to read from various storage backends
- Uses `stellar-cdp` for efficient ledger processing
- Supports GCS, S3, and local filesystem storage
- Streams the raw ledger data over gRPC to consumers

### ttp-processor

This service consumes raw ledger data and extracts TTP events. It:
- Connects to either source service as a gRPC client
- Unmarshals the raw XDR bytes into `LedgerCloseMeta` objects
- Uses the `token_transfer.EventsProcessor` to extract TTP events
- Streams the processed events over gRPC to consumers

## Development

### Generating gRPC Code

For Go services:
```bash
cd protos
make generate-go
```

For Node.js example client:
```bash
cd consumer_app
make build-node
```

## License

This project is licensed under the Apache License 2.0. See the LICENSE file for details.


