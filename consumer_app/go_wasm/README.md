# TTP Consumer Go WebAssembly Implementation

This project is a WebAssembly version of the TTP (Token Transfer Protocol) consumer application built with Go 1.24. It allows you to consume TTP events from a gRPC service in either a web browser (via WebAssembly) or as a native command-line application.

## Prerequisites

To build this project, you need:

1. [Go 1.24 or later](https://golang.org/dl/)
2. [Protobuf compiler](https://grpc.io/docs/protoc-installation/) (needed for gRPC code generation)

## Project Structure

- `/cmd/consumer_wasm` - Main application code
  - `main.go` - WebAssembly specific code (uses build constraints)
  - `main_native.go` - Native version code (uses build constraints)
- `/internal/client` - gRPC client implementation
- `/proto` - Protocol Buffer definitions
- `/web` - Web interface for the WebAssembly version, including `wasm_exec.js`
- `Makefile` - Build and run commands

## Building

This project can be built both as a WebAssembly module for browsers and as a native application. It uses Go build tags to separate WebAssembly and native code.

### Using the Makefile

The included Makefile provides several targets:

```bash
# Build both WebAssembly and native versions
make all

# Build only the WebAssembly version
make build-wasm

# Build only the native version
make build-native

# Run the native version
make run-native

# Start a HTTP server to serve the WebAssembly version
make serve-wasm

# Clean build artifacts
make clean
```

### Manual Building

#### WebAssembly Version

```bash
# Create output directory
mkdir -p dist/wasm

# Build the WebAssembly module
GOOS=js GOARCH=wasm go build -tags "js wasm" -o dist/wasm/consumer_wasm.wasm ./cmd/consumer_wasm

# Copy the included WebAssembly JavaScript support file
cp ./web/wasm_exec.js dist/wasm/

# Copy the HTML interface
cp ./web/index.html dist/wasm/
```

> Note: This project includes its own copy of `wasm_exec.js`, which is normally found in the Go installation directory at `$GOROOT/misc/wasm/wasm_exec.js`. Using our included version avoids issues with finding the file in different Go installations.

#### Native Version

```bash
# Create output directory
mkdir -p dist/bin

# Build the native binary
go build -tags "!js,!wasm" -o dist/bin/consumer_wasm ./cmd/consumer_wasm
```

## Usage

### WebAssembly Version

1. Build the WebAssembly version (see above)
2. Host the files in `dist/wasm` with a web server:

```bash
cd dist/wasm
python -m http.server 8080
```

3. Open a browser and navigate to `http://localhost:8080`
4. Use the web interface to connect to the gRPC service and fetch TTP events

### Native Version

The native version can be run from the command line:

```bash
./dist/bin/consumer_wasm <server_address> <start_ledger> <end_ledger>
```

For example:

```bash
./dist/bin/consumer_wasm localhost:50051 1 10
```

#### Using the Mock Client

For testing purposes, the application includes a mock client that returns predefined events without requiring a real gRPC server. To use it, specify `localhost:50054` or `mock` as the server address:

```bash
./dist/bin/consumer_wasm mock 1 10
```

This will return mock events based on the provided ledger range.

### Connecting to a Real gRPC Server

To connect to a real gRPC server:

```bash
./dist/bin/consumer_wasm localhost:50051 1 10
```

Make sure your gRPC server is running at the specified address (in this example, `localhost:50051`).

### Implementing Full gRPC Support

For a fully functional gRPC client (rather than the default implementation that falls back to mock data), you need to generate Go code from the protobuf definitions:

1. **Install the Protocol Buffer Compiler (protoc)**:

   Download and install `protoc` from the [official Protocol Buffers GitHub releases](https://github.com/protocolbuffers/protobuf/releases).

2. **Install Go plugins for protoc**:

   ```bash
   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
   ```

   Ensure that your `GOPATH/bin` directory is added to your PATH:
   
   ```bash
   # For Windows
   set PATH=%PATH%;%GOPATH%\bin
   
   # For Linux/macOS
   export PATH=$PATH:$GOPATH/bin
   ```

3. **Generate the Go code**:

   ```bash
   # On Windows
   .\generate_proto.bat
   
   # On Linux/macOS
   make generate-proto
   ```

4. **Update the client implementation**:

   ```bash
   # On Windows
   .\update_client.bat
   ```

   This script will:
   - Create a backup of your existing client.go
   - Generate a new client_gen.go file with adapter code to use the generated protobuf types
   - The generated code will replace the mock implementation with the real gRPC connection

5. **Connect to the real server**:

   Update the server address in the Makefile to point to your gRPC server:
   
   ```bash
   ./dist/bin/consumer_wasm localhost:50051 254838 254839
   ```

   **Note**: Even after implementing the real gRPC client, the mock client will still work as a fallback if you specify `mock` as the server address, which is useful for development and testing.

## Connecting to the Real Server vs. Using the Mock

This application can work with either a real gRPC server or a mock implementation for testing:

### Using the Mock Server

```bash
# Use the mock server implementation
make run-native
# Or directly:
./dist/bin/consumer_wasm mock 254838 254839
```

This will use the built-in mock implementation that generates synthetic events for testing.

### Using the Real gRPC Server

```bash
# Connect to a real gRPC server on localhost:50051
make run-real-server
# Or directly:
./dist/bin/consumer_wasm localhost:50051 254838 254839
```

Make sure your real gRPC server is running at the specified address before executing this command.

## Implementation Details

### Build Constraints

The project uses Go build constraints (build tags) to separate WebAssembly-specific code from native code:

- `main.go` uses `//go:build js && wasm` to only be included in WebAssembly builds
- `main_native.go` uses `//go:build !js && !wasm` to only be included in native builds

This approach allows us to maintain a single codebase that works in both environments without conditional imports.

### Mock Implementation

The client includes a mock implementation that generates synthetic events for testing without needing a real gRPC server. The mock is automatically used when:

1. The server address is `mock`, or
2. The server address is `localhost:50054`

It generates three types of events (transfer, mint, and fee) with realistic data based on the provided ledger range.

### JS/WASM API

When compiled to WebAssembly, the following JavaScript functions are exported:

- `getTTPEvents(serverAddress, startLedger, endLedger)` - Fetches TTP events from the specified ledger range
- `cleanupTTPClient()` - Cleans up resources used by the client

### Native CLI API

When compiled natively, the application accepts command-line arguments:

```
consumer_wasm <server_address> <start_ledger> <end_ledger>
```

## gRPC-Web Proxy

Note that for the WebAssembly version to work in a browser, you'll need a gRPC-Web proxy (like Envoy) in front of your gRPC server, as browsers cannot make direct gRPC calls.

## Differences from the Node.js Implementation

The Go WebAssembly implementation has a few key differences from the original Node.js implementation:

1. It can be used both as a command-line tool and as a WebAssembly module
2. The event handling is Promise-based in WebAssembly mode
3. It uses Go's syscall/js package to interface with JavaScript

## Troubleshooting

### Missing wasm_exec.js File

If you encounter issues with the `wasm_exec.js` file when building with your Go installation, you can use the copy included in the `web/` directory. This file is a standard part of Go's WebAssembly support and is included with this project to avoid installation-specific issues.

### Build Issues with syscall/js

If you encounter errors like:

```
imports syscall/js: build constraints exclude all Go files in /path/to/go/src/syscall/js
```

Make sure you're using the right build tags:
- For WebAssembly: `-tags "js wasm"` with `GOOS=js GOARCH=wasm`
- For native: `-tags "!js,!wasm"` without special environment variables

### Error: not implemented

If you see the error:
```
Warning: Using placeholder TokenTransferServiceClient
Error getting events: error calling GetTTPEvents: not implemented
```

This means you're trying to connect to a real gRPC server that doesn't exist or isn't accessible. You can:

1. Start the actual gRPC server at the specified address
2. Use the mock client by specifying `mock` as the server address:
   ```
   ./dist/bin/consumer_wasm mock 1 10
   ```

## License

Same as the original project. 