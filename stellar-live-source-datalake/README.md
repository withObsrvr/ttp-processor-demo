# Stellar Live Source

This service streams raw Stellar ledger data via gRPC to downstream consumers. It can read ledgers from multiple upstream backend types:

- **RPC**: Stellar RPC endpoint via the official Stellar Go SDK RPC ledger backend
- **ARCHIVE**: Datalake/archive storage via GCS or S3 using the official Stellar Go SDK datastore and buffered storage backend
- **CAPTIVE_CORE**: Captive Core ledger backend

Despite the historical `stellar-live-source-datalake` directory name, this service is no longer datalake-only. Select exactly one upstream backend with `BACKEND_TYPE`.

## Features

- Reads ledger data from Stellar RPC, archive/datalake storage, or Captive Core
- Streams raw ledger data via gRPC
- Supports continuous streaming from a specified ledger
- Supports optional RPC authorization headers
- Efficient archive processing using buffered storage backend

## Prerequisites

- Go 1.21+
- Protocol Buffers compiler (protoc)
- Access to the selected upstream backend: Stellar RPC, GCS/S3 archive storage, or Captive Core

## Building

### Build with Nix (Recommended)

For a reproducible and reliable build experience, use the Makefile with Nix:

```bash
# Build the binary
make nix-build

# Run the application
make nix-run

# Build Docker image (Nix binary + Docker)
make docker-build
```

For pure Nix Docker image:
```bash
# Build a Docker image with Nix
make nix-docker

# Load the image into Docker
make nix-docker-load
```

The Nix flake in this directory defines the reproducible build environment used by these targets.

### Development Environment

Set up a complete development environment with Nix:

```bash
nix develop
```

This provides all necessary tools: Go, Protocol Buffers, Docker, etc.

### Manual Build

If you prefer not to use Nix:

1. Install dependencies:
   ```bash
   cd go
   go mod download
   ```

2. Generate gRPC code:
   ```bash
   make gen-proto
   ```

3. Build the service:
   ```bash
   make build-server
   ```

## Configuration

The service is configured via environment variables.

### Backend Selection

- `BACKEND_TYPE`: Upstream backend type. Supported values: `RPC`, `ARCHIVE`, or `CAPTIVE_CORE`.
  - Default: `CAPTIVE_CORE`
  - Legacy `STORAGE_TYPE=GCS` or `STORAGE_TYPE=S3` maps to `ARCHIVE` when `BACKEND_TYPE` is unset.

### Shared Configuration

- `NETWORK_PASSPHRASE`: Stellar network passphrase. Default: `Public Network; September 2015`
- `PORT`: gRPC service port. Default: `50053`
- `HEALTH_PORT`: Health check HTTP port. Default: `8088`

### RPC Backend Configuration

Required when `BACKEND_TYPE=RPC`:

- `RPC_ENDPOINT`: Stellar RPC endpoint URL
- `RPC_AUTH_HEADER`: Optional Authorization header value for RPC requests, for example `Api-Key xyz123`

### Archive / Datalake Backend Configuration

Required when `BACKEND_TYPE=ARCHIVE`:

- `ARCHIVE_STORAGE_TYPE`: Archive storage backend type. Supported values: `GCS` or `S3`
- `ARCHIVE_BUCKET_NAME`: Bucket name
- `ARCHIVE_PATH`: Optional path inside the bucket. For GCS, if unset, defaults to `landing/ledgers/testnet`
- `LEDGERS_PER_FILE`: Number of ledgers per file. Default: `64`
- `FILES_PER_PARTITION`: Number of files per partition. Default: `10`
- `BUFFER_SIZE`: Archive read-ahead buffer size. Default: `5`
- `NUM_WORKERS`: Archive backend worker count. Default: `2`

S3-specific options:

- `AWS_REGION`: AWS region. Default: `us-east-1`
- `S3_ENDPOINT_URL`: Custom S3 endpoint URL, for example MinIO
- `S3_FORCE_PATH_STYLE`: Set to `true` for path-style S3 URLs

Legacy archive variables are still accepted for compatibility:

- `STORAGE_TYPE`: Legacy alias for `ARCHIVE_STORAGE_TYPE` when set to `GCS` or `S3`
- `BUCKET_NAME`: Legacy alias for `ARCHIVE_BUCKET_NAME`

### Captive Core Backend Configuration

Required when `BACKEND_TYPE=CAPTIVE_CORE`:

- `STELLAR_CORE_BINARY_PATH`: Path to the Stellar Core binary
- `HISTORY_ARCHIVE_URLS`: Comma-separated history archive URLs
- `STELLAR_CORE_CONFIG_PATH`: Currently loaded by configuration but not used by backend construction

### Flowctl Integration

This service supports integration with the Obsrvr flowctl control plane. Enable it with:

- `ENABLE_FLOWCTL`: Set to "true" to enable flowctl integration
- `FLOWCTL_ENDPOINT`: The control plane endpoint (e.g., "localhost:8080")
- `SERVICE_ID`: Optional unique ID for this service instance

When enabled, the service registers with the configured flowctl control plane and exposes health/metrics data.

## Running

The service can be run in several ways depending on your environment and needs:

### Method 1: Direct Execution with Make

1. Set required environment variables for one backend. For example, archive/datalake:
   ```bash
   export BACKEND_TYPE="ARCHIVE"
   export ARCHIVE_STORAGE_TYPE="GCS"
   export ARCHIVE_BUCKET_NAME="my-stellar-ledgers"
   export NETWORK_PASSPHRASE="Test SDF Network ; September 2015"
   ```

2. Run the service:
   ```bash
   make run
   ```

### Method 2: Using Nix

For a reproducible environment with all dependencies:

```bash
# Run with Nix (loads flake.nix environment)
make nix-run

# Or enter Nix shell and run manually
nix develop
make run
```

### Method 3: Docker Container

Run the service in a containerized environment:

```bash
# Build the Docker image
make docker-build

# Run with explicit backend configuration
# Note: the current Makefile docker-run target is legacy; prefer explicit docker run.
docker run \
  -p 50053:50053 \
  -p 8088:8088 \
  -e BACKEND_TYPE=ARCHIVE \
  -e ARCHIVE_STORAGE_TYPE=GCS \
  -e ARCHIVE_BUCKET_NAME=my-bucket \
  stellar-live-source-datalake:latest

# Or use a Nix-built Docker image
make nix-docker
make nix-docker-load
```

### Method 4: Direct Go Execution

For development and debugging:

```bash
cd go
# Set environment variables for an RPC backend
export BACKEND_TYPE="RPC"
export RPC_ENDPOINT="https://soroban-testnet.stellar.org"
export NETWORK_PASSPHRASE="Test SDF Network ; September 2015"

# Run directly
go run main.go
```

### Backend Examples

#### Stellar RPC
```bash
export BACKEND_TYPE="RPC"
export RPC_ENDPOINT="https://soroban-testnet.stellar.org"
export RPC_AUTH_HEADER="Api-Key your-key" # optional
```

#### Google Cloud Storage archive/datalake
```bash
export BACKEND_TYPE="ARCHIVE"
export ARCHIVE_STORAGE_TYPE="GCS"
export ARCHIVE_BUCKET_NAME="stellar-ledgers-prod"
export ARCHIVE_PATH="landing/ledgers/testnet" # optional
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
```

#### Amazon S3 archive/datalake
```bash
export BACKEND_TYPE="ARCHIVE"
export ARCHIVE_STORAGE_TYPE="S3"
export ARCHIVE_BUCKET_NAME="stellar-ledgers"
export AWS_REGION="us-east-1"
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
```

#### MinIO or S3-compatible archive/datalake
```bash
export BACKEND_TYPE="ARCHIVE"
export ARCHIVE_STORAGE_TYPE="S3"
export ARCHIVE_BUCKET_NAME="stellar-ledgers"
export S3_ENDPOINT_URL="http://localhost:9000"
export S3_FORCE_PATH_STYLE="true"
export AWS_ACCESS_KEY_ID="minioadmin"
export AWS_SECRET_ACCESS_KEY="minioadmin"
```

#### Captive Core
```bash
export BACKEND_TYPE="CAPTIVE_CORE"
export STELLAR_CORE_BINARY_PATH="/path/to/stellar-core"
export HISTORY_ARCHIVE_URLS="https://history.stellar.org/prd/core-live/core_live_001"
export NETWORK_PASSPHRASE="Public Network; September 2015"
```

### Running with Flowctl Integration

To enable control plane integration:

```bash
export ENABLE_FLOWCTL="true"
export FLOWCTL_ENDPOINT="localhost:8080"
export SERVICE_ID="datalake-source-1"
make run
```

### Environment Variable Reference

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `BACKEND_TYPE` | No | `CAPTIVE_CORE` | Upstream backend: `RPC`, `ARCHIVE`, or `CAPTIVE_CORE` |
| `NETWORK_PASSPHRASE` | No | `Public Network; September 2015` | Stellar network passphrase |
| `PORT` | No | `50053` | gRPC service port |
| `HEALTH_PORT` | No | `8088` | Health check HTTP port |
| `RPC_ENDPOINT` | If RPC | - | Stellar RPC endpoint URL |
| `RPC_AUTH_HEADER` | No | - | Authorization header value for RPC requests |
| `ARCHIVE_STORAGE_TYPE` | If ARCHIVE | - | Archive storage backend: `GCS` or `S3` |
| `ARCHIVE_BUCKET_NAME` | If ARCHIVE | - | Archive bucket name |
| `ARCHIVE_PATH` | No | GCS: `landing/ledgers/testnet` | Optional archive path inside bucket |
| `AWS_REGION` | If S3 | `us-east-1` | AWS region for S3 storage |
| `S3_ENDPOINT_URL` | No | - | Custom S3 endpoint, for example MinIO |
| `S3_FORCE_PATH_STYLE` | No | `false` | Use path-style S3 URLs |
| `LEDGERS_PER_FILE` | No | `64` | Ledgers per archive file |
| `FILES_PER_PARTITION` | No | `10` | Files per archive partition |
| `BUFFER_SIZE` | No | `5` | Archive backend read-ahead buffer size |
| `NUM_WORKERS` | No | `2` | Archive backend worker count |
| `STELLAR_CORE_BINARY_PATH` | If CAPTIVE_CORE | - | Stellar Core binary path |
| `HISTORY_ARCHIVE_URLS` | If CAPTIVE_CORE | - | Comma-separated history archive URLs |
| `STORAGE_TYPE` | Legacy | - | Legacy alias for archive storage type (`GCS`/`S3`) |
| `BUCKET_NAME` | Legacy | - | Legacy alias for archive bucket name |
| `ENABLE_FLOWCTL` | No | `false` | Enable flowctl integration |
| `FLOWCTL_ENDPOINT` | If flowctl | - | Control plane endpoint |

The service will start listening on port 50053 (or configured `PORT`) and begin streaming ledger data when requested.

## gRPC Interface

The service exposes a single gRPC method:

```protobuf
service RawLedgerService {
    rpc StreamRawLedgers(StreamLedgersRequest) returns (stream RawLedger) {}
}
```

- `StreamLedgersRequest`: Contains the starting ledger sequence
- `RawLedger`: Contains the ledger sequence and raw XDR bytes

## Architecture

The service uses:
- Official Stellar Go SDK `ledgerbackend` implementations for RPC, archive/datalake storage, and Captive Core
- Official Stellar Go SDK `datastore` for archive/datalake reads from GCS or S3
- gRPC for streaming the data to consumers

## Development

### Module Structure

The project is organized as a Go module with the following structure:

```
github.com/withObsrvr/ttp-processor-demo/
└── stellar-live-source-datalake/
    ├── go/
    │   ├── gen/
    │   │   └── raw_ledger_service/
    │   │       ├── raw_ledger_service.pb.go
    │   │       └── raw_ledger_service_grpc.pb.go
    │   ├── server/
    │   │   └── server.go
    │   ├── main.go
    │   ├── go.mod
    │   └── go.sum
    ├── protos/
    │   └── raw_ledger_service/
    │       └── raw_ledger_service.proto
    ├── Makefile
    └── README.md
```

### Dependencies

The project depends on several key packages:

- `github.com/stellar/go-stellar-sdk`: For ledger backends, datastore access, and XDR types
- `google.golang.org/grpc`: For the streaming service API
- `go.uber.org/zap`: For structured logging
- `github.com/withobsrvr/flowctl`: For optional control plane integration

### Protobuf Generation

The gRPC service definitions are generated from Protocol Buffer files. The generated code is placed in the `go/gen/raw_ledger_service` directory and is included in the module. To ensure proper module resolution:

1. The `go/go.mod` file should have the correct module path:
   ```go
   module github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake
   ```

2. The generated protobuf code should be in the correct directory structure:
   ```
   go/gen/raw_ledger_service/
   ```

3. Ensure the generated code is not ignored by Git (check `.gitignore`)

### Building and Testing

1. Generate protobuf code:
   ```bash
   make gen-proto
   ```

2. Update dependencies:
   ```bash
   cd go && go mod tidy
   ```

3. Build the service:
   ```bash
   make build-server
   ```

4. Run tests:
   ```bash
   make test
   ```

## Troubleshooting

If you encounter issues with `go mod tidy` not finding the generated protobuf code:

1. Verify the module path in `go/go.mod` matches your import paths
2. Check that the generated code is in the correct directory structure
3. Ensure the generated code is not being ignored by Git
4. Try cleaning the module cache and rebuilding:
   ```bash
   cd go && go clean -modcache && go mod tidy
   ```

## License

This project is licensed under the Apache License 2.0. See the LICENSE file for details. 