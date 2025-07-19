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

### Datalake Service (stellar-live-source-datalake)
**Build:**
```bash
cd stellar-live-source-datalake
nix develop -c make build-server
```

**Run:**
```bash
# Source environment variables and run
source .secrets
./stellar_live_source_datalake

# Or run in background
source .secrets && ./stellar_live_source_datalake &
```

**Test:**
```bash
# Check health endpoint
curl -s localhost:8088/health | jq .

# Expected response:
# {
#   "status": "healthy",
#   "implementation": "stellar-go",
#   "metrics": { ... }
# }
```

### TTP Processor
**Build:**
```bash
cd ttp-processor
nix develop ../ -c make build-processor
```

**Run:**
```bash
# Set environment variables and run
NETWORK_PASSPHRASE="Test SDF Network ; September 2015" \
SOURCE_SERVICE_ADDRESS=localhost:50052 \
ENABLE_UNIFIED_EVENTS=true \
ENABLE_FLOWCTL=true \
PORT=50051 \
HEALTH_PORT=8089 \
./ttp_processor_server

# Or run in background
NETWORK_PASSPHRASE="Test SDF Network ; September 2015" \
SOURCE_SERVICE_ADDRESS=localhost:50052 \
ENABLE_UNIFIED_EVENTS=true \
ENABLE_FLOWCTL=true \
PORT=50051 \
HEALTH_PORT=8089 \
./ttp_processor_server &
```

**Test:**
```bash
# Check health endpoint
curl -s localhost:8089/health | jq .

# Expected response:
# {
#   "status": "healthy",
#   "metrics": {
#     "success_count": 0,
#     "error_count": 0,
#     "total_processed": 0,
#     ...
#   }
# }
```

### Consumer App (Node.js)
**Build:**
```bash
cd consumer_app/node
npm install
npm run build
```

**Run:**
```bash
# Basic run
npm start -- <start_ledger> <end_ledger>

# With flowctl integration and custom service address
TTP_SERVICE_ADDRESS=localhost:50051 \
ENABLE_FLOWCTL=true \
HEALTH_PORT=8091 \
npm start -- <start_ledger> <end_ledger>

# Using nix develop (recommended)
nix develop ../../ -c bash -c '
TTP_SERVICE_ADDRESS=localhost:50051 \
ENABLE_FLOWCTL=true \
HEALTH_PORT=8091 \
npm start -- <start_ledger> <end_ledger>
'
```

**Test:**
```bash
# Check health endpoint
curl -s localhost:8091/health | jq .

# Expected response:
# {
#   "status": "healthy",
#   "metrics": { ... }
# }
```

### Testing
```bash
# Go services - run tests
cd <service-directory>/go
go test ./...

# Individual test files
go test -v ./path/to/package
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

### Complete System Test

**1. Start Datalake Service:**
```bash
cd stellar-live-source-datalake
nix develop -c make build-server
source .secrets && ./stellar_live_source_datalake &

# Verify it's running
curl -s localhost:8088/health | jq .
```

**2. Start TTP Processor:**
```bash
cd ttp-processor
nix develop ../ -c make build-processor
NETWORK_PASSPHRASE="Test SDF Network ; September 2015" \
SOURCE_SERVICE_ADDRESS=localhost:50052 \
ENABLE_UNIFIED_EVENTS=true \
ENABLE_FLOWCTL=true \
PORT=50051 \
HEALTH_PORT=8089 \
./ttp_processor_server &

# Verify it's running
curl -s localhost:8089/health | jq .
```

**3. Start Consumer App:**
```bash
cd consumer_app/node
nix develop ../../ -c bash -c '
TTP_SERVICE_ADDRESS=localhost:50051 \
ENABLE_FLOWCTL=true \
HEALTH_PORT=8091 \
npm start -- 409907 409948
'

# Verify it's running in another terminal
curl -s localhost:8091/health | jq .
```

**4. Verify Communication:**
- Datalake service: `localhost:8088/health` (stellar-go implementation)
- TTP processor: `localhost:8089/health` (processing metrics)
- Consumer app: `localhost:8091/health` (consumption metrics)

**5. Kill Processes:**
```bash
# Kill all components
pkill -f stellar_live_source_datalake
pkill -f ttp_processor_server
pkill -f "npm start"
```

### Port Configuration

**Default Ports:**
- Datalake gRPC: `:50052`
- Datalake health: `:8088`
- TTP processor gRPC: `:50051`
- TTP processor health: `:8089`
- Consumer health: `:8091`

**Note:** Adjust ports if they conflict with existing services.

### Environment Variables

**Required .secrets file for datalake:**
```bash
export STORAGE_TYPE=GCS
export BUCKET_NAME=obsrvr-stellar-ledger-data-testnet-data
export ARCHIVE_PATH=landing/ledgers/testnet
export NETWORK_PASSPHRASE="Test SDF Network ; September 2015"
export ENABLE_FLOWCTL=true
export FLOWCTL_ENDPOINT=localhost:8080
export HEALTH_PORT=8088
```

**Required environment for TTP processor:**
```bash
NETWORK_PASSPHRASE="Test SDF Network ; September 2015"
SOURCE_SERVICE_ADDRESS=localhost:50052
ENABLE_UNIFIED_EVENTS=true
ENABLE_FLOWCTL=true
PORT=50051
HEALTH_PORT=8089
```

**Required environment for consumer app:**
```bash
TTP_SERVICE_ADDRESS=localhost:50051
ENABLE_FLOWCTL=true
HEALTH_PORT=8091
```

### Common Issues and Solutions

1. **Port conflicts:** Adjust PORT and HEALTH_PORT variables
2. **Archive access errors:** Verify .secrets file has correct GCS credentials
3. **Connection refused:** Ensure services start in correct order
4. **Process cleanup:** Use `pkill` to stop all related processes

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