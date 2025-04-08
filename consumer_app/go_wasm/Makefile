.PHONY: all clean build-wasm build-native run-native run-real-server generate-proto

PROTO_DIR=./protos
GO_SRC_DIR=./cmd/consumer_wasm
GEN_DIR=$(GO_SRC_DIR)/gen

# Default target
all: build-wasm build-native

# Clean generated files
clean:
	rm -rf dist

# Generate Go code from protobuf definitions
generate-proto:
	./generate_proto.sh

# Create output directories
dist:
	mkdir -p dist/wasm
	mkdir -p dist/bin

# Build WebAssembly version
build-wasm: dist
	GOOS=js GOARCH=wasm go build -tags "js wasm" -o dist/wasm/consumer_wasm.wasm ./cmd/consumer_wasm
	# Copy our local wasm_exec.js instead of relying on GOROOT path
	cp ./web/wasm_exec.js dist/wasm/
	cp ./web/index.html dist/wasm/

# Build native version
build-native: dist
	go build -tags "!js,!wasm" -o dist/bin/consumer_wasm ./cmd/consumer_wasm

# Run the native version with mock server
run-native: build-native
	./dist/bin/consumer_wasm mock 254838 254839

# Run the native version with real server
run-real-server: build-native
	./dist/bin/consumer_wasm localhost:50054 254838 254839

# Run the WebAssembly version in a browser
serve-wasm: build-wasm
	cd dist/wasm && python3 -m http.server 8080 