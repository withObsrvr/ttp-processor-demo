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

1. Install dependencies:
   ```bash
   go mod download
   ```

2. Generate gRPC code:
   ```bash
   make generate-proto
   ```

3. Build the service:
   ```bash
   make build
   ```

## Configuration

The service is configured via environment variables:

- `STORAGE_TYPE`: Type of storage backend ("GCS", "S3", or "FS")
- `BUCKET_NAME`: Name of the bucket or path to the data
- `AWS_REGION`: AWS region (required for S3)
- `S3_ENDPOINT_URL`: Custom S3 endpoint URL (optional)
- `S3_FORCE_PATH_STYLE`: Set to "true" for non-AWS S3 (optional)
- `LEDGERS_PER_FILE`: Number of ledgers per file (default: 64)
- `FILES_PER_PARTITION`: Number of files per partition (default: 10)

## Running

1. Set required environment variables:
   ```bash
   export STORAGE_TYPE="S3"
   export BUCKET_NAME="my-stellar-ledgers"
   export AWS_REGION="us-west-2"
   ```

2. Run the service:
   ```bash
   make run
   ```

The service will start listening on port 50051 and begin streaming ledger data when requested.

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

### Directory Structure

```
stellar-live-source-datalake/
├── protos/
│   └── raw_ledger_service/
│       └── raw_ledger_service.proto
├── gen/
│   └── raw_ledger_service/
│       ├── raw_ledger_service.pb.go
│       └── raw_ledger_service_grpc.pb.go
├── server/
│   └── server.go
├── main.go
├── Makefile
└── go.mod
```

### Adding New Features

1. Modify the proto file in `protos/raw_ledger_service/`
2. Regenerate gRPC code: `make generate-proto`
3. Implement the new functionality in `server/server.go`
4. Update tests if necessary

## License

This project is licensed under the Apache License 2.0. See the LICENSE file for details. 