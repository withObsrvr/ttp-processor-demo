# Stellar Live Source (Data Lake)

This service reads Stellar ledger data from a data lake (GCS, S3, or local filesystem) and streams it via gRPC to consumers. It uses the `stellar-datastore` and `stellar-cdp` packages to efficiently read and process ledger data from storage.

> **Note**: This service reads from **storage backends** (data lakes). If you need to stream data from **Stellar RPC endpoints**, use the `stellar-live-source` service instead, which connects directly to Stellar RPC nodes.

## Features

- Reads ledger data from various storage backends (GCS, S3, FS)
- Streams raw ledger data via gRPC
- Supports continuous streaming from a specified ledger
- Efficient processing using buffered storage backend

## Prerequisites

- Go 1.21+
- Protocol Buffers compiler (protoc)
- Access to a data lake containing Stellar ledger data

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

# Run Docker container
make docker-run
```

For pure Nix Docker image:
```bash
# Build a Docker image with Nix
make nix-docker

# Load the image into Docker
make nix-docker-load
```

See [NIX_USAGE.md](./NIX_USAGE.md) for detailed build options and troubleshooting.

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
   make generate-proto
   ```

3. Build the service:
   ```bash
   make build-server
   ```

## Configuration

The service is configured via environment variables:

### Storage Configuration

- `STORAGE_TYPE`: Type of storage backend ("GCS", "S3", or "FS")
- `BUCKET_NAME`: Name of the bucket or path to the data
- `AWS_REGION`: AWS region (required for S3)
- `S3_ENDPOINT_URL`: Custom S3 endpoint URL (optional)
- `S3_FORCE_PATH_STYLE`: Set to "true" for non-AWS S3 (optional)
- `LEDGERS_PER_FILE`: Number of ledgers per file (default: 64)
- `FILES_PER_PARTITION`: Number of files per partition (default: 10)

### Flowctl Integration

This service supports integration with the Obsrvr flowctl control plane. Enable it with:

- `ENABLE_FLOWCTL`: Set to "true" to enable flowctl integration
- `FLOWCTL_ENDPOINT`: The control plane endpoint (e.g., "localhost:8080")
- `SERVICE_ID`: Optional unique ID for this service instance

See [FLOWCTL_INTEGRATION.md](./FLOWCTL_INTEGRATION.md) for detailed integration instructions.

## Running

The service can be run in several ways depending on your environment and needs:

### Method 1: Direct Execution with Make

1. Set required environment variables:
   ```bash
   export STORAGE_TYPE="GCS"
   export BUCKET_NAME="my-stellar-ledgers"
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
# Build and run with Docker
make docker-build
make docker-run

# Or use Nix-built Docker image
make nix-docker
make nix-docker-load
docker run -e STORAGE_TYPE=GCS -e BUCKET_NAME=my-bucket stellar-live-source-datalake
```

### Method 4: Direct Go Execution

For development and debugging:

```bash
cd go
# Set environment variables
export STORAGE_TYPE="FS"
export BUCKET_NAME="./test-data"
export NETWORK_PASSPHRASE="Test SDF Network ; September 2015"

# Run directly
go run main.go
```

### Storage Backend Examples

#### Google Cloud Storage (GCS)
```bash
export STORAGE_TYPE="GCS"
export BUCKET_NAME="stellar-ledgers-prod"
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
```

#### Amazon S3
```bash
export STORAGE_TYPE="S3"
export BUCKET_NAME="stellar-ledgers"
export AWS_REGION="us-east-1"
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
```

#### Local Filesystem
```bash
export STORAGE_TYPE="FS"
export BUCKET_NAME="/path/to/ledger/data"
```

#### MinIO or S3-Compatible Storage
```bash
export STORAGE_TYPE="S3"
export BUCKET_NAME="stellar-ledgers"
export S3_ENDPOINT_URL="http://localhost:9000"
export S3_FORCE_PATH_STYLE="true"
export AWS_ACCESS_KEY_ID="minioadmin"
export AWS_SECRET_ACCESS_KEY="minioadmin"
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
| `STORAGE_TYPE` | Yes | - | Storage backend type: "GCS", "S3", or "FS" |
| `BUCKET_NAME` | Yes | - | Bucket name or filesystem path |
| `NETWORK_PASSPHRASE` | Yes | - | Stellar network passphrase |
| `PORT` | No | 50052 | gRPC service port |
| `HEALTH_PORT` | No | 8088 | Health check HTTP port |
| `AWS_REGION` | If S3 | - | AWS region for S3 storage |
| `S3_ENDPOINT_URL` | No | - | Custom S3 endpoint (e.g., MinIO) |
| `S3_FORCE_PATH_STYLE` | No | false | Use path-style S3 URLs |
| `LEDGERS_PER_FILE` | No | 64 | Ledgers per storage file |
| `FILES_PER_PARTITION` | No | 10 | Files per storage partition |
| `ENABLE_FLOWCTL` | No | false | Enable flowctl integration |
| `FLOWCTL_ENDPOINT` | If flowctl | - | Control plane endpoint |

The service will start listening on port 50052 (or configured PORT) and begin streaming ledger data when requested.

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
- `stellar-datastore` for reading from various storage backends
- `stellar-cdp` for efficient ledger processing
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

- `github.com/withObsrvr/stellar-datastore`: For reading from various storage backends
- `github.com/withObsrvr/stellar-cdp`: For efficient ledger processing
- `github.com/withObsrvr/stellar-ledgerbackend`: For ledger backend functionality

### Protobuf Generation

The gRPC service definitions are generated from Protocol Buffer files. The generated code is placed in the `go/gen/raw_ledger_service` directory and is included in the module. To ensure proper module resolution:

1. The `go/go.mod` file should have the correct module path:
   ```go
   module github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/go
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