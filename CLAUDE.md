# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a microservices architecture for processing Token Transfer Protocol (TTP) events from the Stellar blockchain. The system consists of data sources, processors, and consumer applications that work together via gRPC streaming.

### Core Architecture

```
Stellar Data Source → stellar-live-source/datalake → ttp-processor → Consumer Apps
```

The repository is organized as a Go workspace with multiple services:
- `stellar-live-source`: RPC-based ledger data source
- `stellar-live-source-datalake`: Storage-based ledger data source  
- `ttp-processor`: Processes raw ledgers into TTP events
- `ledger-jsonrpc-processor`: Converts ledgers to JSON-RPC format
- `consumer_app/`: Multiple consumer implementations (Node.js, Go WASM, Rust WASM)
- `cli_tool/`: Command-line interface tool

## Build Commands

### Go Services
Each Go service has a Makefile with consistent targets:

```bash
# Build any Go service
cd <service-directory>
make build

# Generate protobuf code
make gen-proto

# Run the service
make run

# Clean build artifacts
make clean
```

### Consumer App (Node.js)
```bash
cd consumer_app/node
npm install
npm run build
npm start -- <start_ledger> <end_ledger>
```

### WebAssembly Consumer (Rust)
```bash
cd consumer_app/rust_wasm
wasm-pack build --target web
```

### Testing
```bash
# Go services
cd <service-directory>
go test ./...

# Node.js consumer
cd consumer_app/node
npm test  # if tests exist
```

## Development Workflow

### Protocol Buffer Generation
All services use protobuf for gRPC communication. Proto files are in `protos/` directories.

Key proto services:
- `raw_ledger_service`: Raw Stellar ledger data
- `event_service`: Processed TTP events  
- `ledger_jsonrpc_service`: JSON-RPC formatted data

### Go Workspace Structure
The project uses `go.work` to manage multiple Go modules:
- Services are independent Go modules
- Shared dependencies are managed at workspace level
- Use `go work use <path>` to add new services

### Environment Configuration
Services use environment variables for configuration:

**Data Sources:**
- `RPC_ENDPOINT`: Stellar RPC endpoint
- `NETWORK_PASSPHRASE`: Stellar network passphrase
- `STORAGE_TYPE`: For datalake source ("GCS", "S3", "FS")
- `BUCKET_NAME`: Storage bucket/path

**Processors:**
- `LIVE_SOURCE_ENDPOINT`: Address of data source service
- `NETWORK_PASSPHRASE`: Stellar network passphrase

**Flowctl Integration:**
- `ENABLE_FLOWCTL=true`: Enable control plane integration
- `FLOWCTL_ENDPOINT`: Control plane address
- `FLOWCTL_HEARTBEAT_INTERVAL`: Heartbeat interval (ms)

### Service Communication
- All inter-service communication uses gRPC streaming
- Services expose health endpoints for monitoring
- Metrics are reported to flowctl control plane when enabled

## Key Implementation Patterns

### gRPC Streaming
- Data flows as continuous streams between services
- Use bidirectional streaming for real-time processing
- Handle stream reconnection and error recovery

### Protobuf Code Generation
- Proto files define service contracts
- Generated Go code follows source_relative paths
- Client and server stubs are auto-generated

### Error Handling and Metrics
- Services track processing metrics (success/error counts, latency)
- Health endpoints expose service status and metrics
- Graceful shutdown handling for all services

### Consumer Implementations
Multiple consumer patterns are demonstrated:
- **Node.js**: Traditional gRPC client with TypeScript
- **Go WASM**: WebAssembly for browser deployment
- **Rust WASM**: Alternative WASM implementation
- All support flowctl integration for monitoring

## Dependencies

### Go Requirements
- Go 1.21+
- Protocol Buffers compiler (protoc)
- protoc-gen-go and protoc-gen-go-grpc plugins

### Node.js Requirements  
- Node.js 22+
- TypeScript for development
- @grpc/grpc-js for gRPC client

### WebAssembly Requirements
- Rust toolchain for Rust WASM
- wasm-pack for building WebAssembly modules
- Special considerations for browser gRPC (requires gRPC-Web proxy)

## Service Startup Order

1. Start data source service (stellar-live-source or stellar-live-source-datalake)
2. Start processor service (ttp-processor or ledger-jsonrpc-processor)  
3. Start consumer applications
4. Optional: Start flowctl control plane for monitoring

Each service will wait for dependencies and handle reconnection automatically.