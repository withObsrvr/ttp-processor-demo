# TTP Processor Demo

This project demonstrates a microservices architecture for processing Token Transfer Protocol (TTP) events from the Stellar blockchain and visualizing them in a Minecraft mod. The system processes live ledger data from a Stellar RPC endpoint, extracts TTP events, and makes them available to consumer applications.

## Architecture

The system is split into three main components that work together:

```plaintext
+--------------------------+        +--------------------------+        +-----------------------------+
| Stellar RPC Endpoint     |        |   Service A:             |        |   Service B:                |
| (e.g., Soroban Testnet)  |        | stellar-live-source (Go) |        |   ttp-processor (Go)        |
+--------------------------+        +--------------------------+        +-----------------------------+
          ▲                         │                          │        │                             │
          │ RPC GetLedgers          │ 1. Connects & Polls RPC  │        │ 4. Receives RawLedger       │
          └─────────────────────────┤                          ├────────► (gRPC Client to Service A)  │
                                    │ 2. Decodes Base64 Meta   │        │ 5. Unmarshals XDR           │
                                    │ 3. Streams RawLedger msg │        │ 6. Processes TTP Events     │
                                    │   (via gRPC Stream)      │        │ 7. Streams TokenTransferEvent│
                                    +-------------▲------------+        +-------------▲---------------+
                                                  │ gRPC Stream                     │ gRPC Stream
                                                  │ (RawLedger)                     │ (TokenTransferEvent)
+--------------------------+                      │                                 │
| Minecraft Mod (Java)     |                      │                                 │
+--------------------------+                      │                                 │
|   [Source Block]─────────-----------------------┘ Control (Optional)              │
|     - Display Status     │                                                        │
|                          │                                                        │
|   [Processor Block]──────---------------------------------------------------------┘ Control (e.g. Start/Stop Request)
|     - Display Status     │
|                          │
|   [Consumer Block]◄──────---------------------------------------------------------┘ Consumes Event Stream
|     - Visualize Events   |
+--------------------------+
```

### Component Overview

1. **Service A: `stellar-live-source` (Go Service)**
   - Connects to the Stellar RPC endpoint and streams raw ledger data
   - Handles continuous polling of the blockchain for new ledgers
   - Exposes a gRPC service that streams raw ledger data to consumers

2. **Service B: `ttp-processor` (Go Service)**
   - Consumes raw ledger data from the `stellar-live-source` service
   - Processes the data to extract Token Transfer Protocol (TTP) events
   - Exposes a gRPC service that streams the processed events to consumers

3. **Consumer Application (Minecraft Mod)**
   - Connects to the `ttp-processor` service via gRPC
   - Receives and visualizes TTP events in the Minecraft game environment
   - Provides blocks for controlling and monitoring the source and processor services

## Setup Instructions

### Prerequisites

- Go 1.21+
- Protocol Buffers compiler (protoc)
- Node.js 22+
- Java Development Kit (JDK) 17+ (for Minecraft mod)
- Minecraft with Forge (for running the mod)

### Install Protocol Buffers Compiler

- On Mac: `brew install protobuf`
- On Linux: `apt install protobuf-compiler`

### Install Go Protocol Buffers Plugins

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2
```

## Running the Services

### 1. Start the Stellar Live Source Service

```bash
cd stellar-live-source
make build
RPC_ENDPOINT=https://soroban-testnet.stellar.org NETWORK_PASSPHRASE="Test SDF Network ; September 2015" ./stellar_live_source
```

### 2. Start the TTP Processor Service

```bash
cd ttp-processor
make build
LIVE_SOURCE_ENDPOINT=localhost:50051 ./ttp_processor
```

### 3. Run the Consumer Application

#### Node.js Example Client

```bash
cd consumer_app/node
npm run build
npm start -- <start_ledger> <end_ledger>
```

Or with the compiled JavaScript:

```bash
node dist/index.js <start_ledger> <end_ledger>
```

## Implementation Details

### stellar-live-source

This service connects to the Stellar RPC endpoint and streams raw ledger data. It:
- Uses `github.com/stellar/stellar-rpc/client` to connect to the RPC endpoint
- Polls for new ledgers continually using cursors
- Decodes the Base64-encoded ledger metadata into raw XDR bytes
- Streams the raw ledger data over gRPC to consumers

### ttp-processor

This service consumes raw ledger data from the `stellar-live-source` service and extracts TTP events. It:
- Connects to the `stellar-live-source` service as a gRPC client
- Unmarshals the raw XDR bytes back into `LedgerCloseMeta` objects
- Uses the `token_transfer.EventsProcessor` to extract TTP events
- Streams the processed events over gRPC to consumers

### Consumer Application (Minecraft Mod)

The Minecraft mod provides an in-game visualization of TTP events. It:
- Connects to the `ttp-processor` service as a gRPC client
- Receives TTP events in real-time
- Visualizes different event types (Transfer, Mint, Burn) in the game environment
- Provides blocks for controlling and monitoring the source and processor services

## Development

### Generating gRPC Code

For Go:
```bash
cd protos
make generate-go
```

For Node.js:
```bash
cd consumer_app
make build-node
```

For Java (Minecraft mod):
```bash
cd minecraft-mod
./gradlew generateProto
```


