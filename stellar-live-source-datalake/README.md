# Stellar Live Source (Data Lake)

This service reads Stellar ledger data from a data lake (GCS, S3, or local filesystem) and streams it via gRPC to consumers. It uses the `stellar-datastore` and `stellar-cdp` packages to efficiently read and process ledger data from storage.

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

1. Set required environment variables:
   ```bash
   export STORAGE_TYPE="GCS"
   export BUCKET_NAME="my-stellar-ledgers"
   ```

2. Run the service:
   ```bash
   make run
   ```

The service will start listening on port 50052 and begin streaming ledger data when requested.

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