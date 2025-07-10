# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a microservices architecture for processing Token Transfer Protocol (TTP) events from the Stellar blockchain. The system consists of data sources, processors, and consumer applications that work together via gRPC streaming.

### Core Architecture

```
Stellar Data Source → stellar-live-source/datalake → ttp-processor → Consumer Apps
                                                  → ledger-jsonrpc-processor → JSON-RPC API
```

The repository is organized as a Go workspace with multiple services:
- `stellar-live-source`: RPC-based ledger data source
- `stellar-live-source-datalake`: Storage-based ledger data source with Nix builds
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

### Testing
```bash
# Go services - run tests
cd <service-directory>/go
go test ./...

# Individual test files
go test -v ./path/to/package
```

### Consumer App (Node.js)
```bash
cd consumer_app/node
npm install
npm run build
npm start -- <start_ledger> <end_ledger>

# With flowctl integration
ENABLE_FLOWCTL=true npm start -- <start_ledger> <end_ledger>
```

### Consumer App (Go WASM)
```bash
cd consumer_app/go_wasm

# Build both WebAssembly and native versions
make all

# Build WebAssembly only
make build-wasm

# Build native version only  
make build-native

# Run native with mock server
make run-native

# Run native with real server
make run-real-server

# Serve WebAssembly in browser
make serve-wasm
```

### WebAssembly Consumer (Rust)
```bash
cd consumer_app/rust_wasm

# Build for web target
wasm-pack build --target web

# Build for Node.js target
wasm-pack build --target nodejs
```

### Nix Builds (stellar-live-source-datalake)
```bash
cd stellar-live-source-datalake

# Development shell with all dependencies
make nix-shell

# Build binary with Nix
make nix-build

# Build and run with Nix
make nix-run

# Build Docker image
make docker-build

# Run Docker container
make docker-run

# Full CI build process
make ci-build

# Vendor dependencies for offline builds
make vendor-all
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
- For Nix builds, Go workspace mode is disabled with `GOWORK=off`

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
- `FLOWCTL_ENDPOINT`: Control plane address (default: localhost:8080)
- `FLOWCTL_HEARTBEAT_INTERVAL`: Heartbeat interval (ms, default: 10000)
- `STELLAR_NETWORK`: Network name (default: testnet)
- `HEALTH_PORT`: Health check port (default: 8088)

### WebAssembly Build Targets
The codebase supports multiple WebAssembly implementations:

**Go WASM (`consumer_app/go_wasm`):**
- Uses build tags for JS/WASM vs native compilation
- Generates both `.wasm` file and native binary
- Includes web server for browser testing

**Rust WASM (`consumer_app/rust_wasm`):**
- Uses `wasm-pack` for building
- Supports both web and Node.js targets
- Includes example web application

### Service Communication
- All inter-service communication uses gRPC streaming
- Services expose health endpoints for monitoring
- Metrics are reported to flowctl control plane when enabled
- Services handle stream reconnection and error recovery

## Key Implementation Patterns

### gRPC Streaming Architecture
- Data flows as continuous streams between services
- Use bidirectional streaming for real-time processing
- Handle backpressure and flow control

### Protobuf Code Generation
- Proto files define service contracts
- Generated Go code follows source_relative paths
- Client and server stubs are auto-generated
- Consumer app pulls proto files from Stellar Go repository

### Error Handling and Metrics
- Services track processing metrics (success/error counts, latency)
- Health endpoints expose service status and metrics at `/health`
- Graceful shutdown handling for all services
- Metrics reporting to flowctl control plane

### Consumer Implementations
Multiple consumer patterns are demonstrated:
- **Node.js**: Traditional gRPC client with TypeScript, flowctl integration
- **Go WASM**: WebAssembly for browser deployment with native fallback
- **Rust WASM**: Alternative WASM implementation with gRPC-Web support
- All support flowctl integration for monitoring

### Nix Build System
The `stellar-live-source-datalake` service uses Nix for reproducible builds:
- Flake-based configuration for dependencies
- Supports both binary and Docker image builds
- Includes vendoring for offline builds
- CI-friendly build targets

## Dependencies

### Go Requirements
- Go 1.21+ (specified in go.work)
- Protocol Buffers compiler (protoc)
- protoc-gen-go and protoc-gen-go-grpc plugins

### Node.js Requirements  
- Node.js 22+ (specified in package.json engines)
- TypeScript for development
- @grpc/grpc-js for gRPC client

### WebAssembly Requirements
- **Go WASM**: Standard Go toolchain with js/wasm build tags
- **Rust WASM**: Rust toolchain with wasm-pack
- Special considerations for browser gRPC (requires gRPC-Web proxy)

### Nix Requirements (for stellar-live-source-datalake)
- Nix with flakes support enabled
- Development shell provides all build dependencies automatically

## Service Startup Order

1. Start data source service (stellar-live-source or stellar-live-source-datalake)
2. Start processor service (ttp-processor or ledger-jsonrpc-processor)  
3. Start consumer applications
4. Optional: Start flowctl control plane for monitoring

Each service will wait for dependencies and handle reconnection automatically.

## Special Build Considerations

### WebAssembly Builds
- Go WASM uses build tags to separate browser and native code
- Rust WASM requires specific dependency versions to avoid feature conflicts
- Browser deployment requires gRPC-Web proxy in front of gRPC services

### Nix Builds
- Use `make vendor-all` to prepare dependencies for offline Nix builds
- Go workspace mode is disabled (`GOWORK=off`) for consistent builds
- Supports both development builds and containerized deployments