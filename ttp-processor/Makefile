.PHONY: all init gen-proto build-processor run clean check-deps
.SILENT:

BINARY_NAME=ttp_processor_server
GO_SRC_DIR=./go
GEN_DIR=$(GO_SRC_DIR)/gen
PROTO_DIR=./protos
# Go package base for generated code SPECIFIC to this processor
GO_PACKAGE_PROCESSOR_BASE=github.com/withObsrvr/ttp-processor
# Go package base for the source service we IMPORT
GO_PACKAGE_SOURCE_BASE=github.com/withObsrvr/stellar-live-source
# Stellar Go proto base paths
STELLAR_GO_PROTO_BASE=github.com/stellar/go

# Location of the source service proto file (adjust path if needed)
# Assumes stellar-live-source is a sibling directory
SOURCE_PROTO_FILE=../stellar-live-source/protos/raw_ledger_service/raw_ledger_service.proto

all: build-processor

# Check dependencies without installing anything
check-deps:
	@echo "Checking build dependencies..."
	@if ! command -v protoc >/dev/null 2>&1; then \
		echo "❌ protoc is not installed"; \
		echo "   Install with: nix-shell -p protobuf (NixOS)"; \
		echo "   Or: sudo apt-get install protobuf-compiler (Ubuntu/Debian)"; \
		echo "   Or: brew install protobuf (macOS)"; \
		exit 1; \
	else \
		echo "✅ protoc found: $$(command -v protoc)"; \
	fi
	@if ! command -v curl >/dev/null 2>&1; then \
		echo "❌ curl is not installed"; \
		exit 1; \
	else \
		echo "✅ curl found: $$(command -v curl)"; \
	fi
	@if ! command -v go >/dev/null 2>&1; then \
		echo "❌ go is not installed"; \
		exit 1; \
	else \
		echo "✅ go found: $$(go version)"; \
	fi
	@echo "✅ All dependencies are available"

# Initialize Go module and install dependencies
init:
	set -e
	# Check for required tools
	@if ! command -v protoc >/dev/null 2>&1; then \
		echo "protoc is required but not installed."; \
		echo "On NixOS, you can install it with: nix-shell -p protobuf"; \
		echo "On Ubuntu/Debian: sudo apt-get install protobuf-compiler"; \
		echo "On macOS: brew install protobuf"; \
		exit 1; \
	fi
	@if ! command -v curl >/dev/null 2>&1; then \
		echo "curl is required but not installed."; \
		exit 1; \
	fi
	# Install required Go protoc plugins
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	# Create proto directories
	mkdir -p $(PROTO_DIR)/processors/token_transfer $(PROTO_DIR)/asset $(PROTO_DIR)/raw_ledger_service
	# Download Protocol 23 proto files from stellar/go
	curl -s -o $(PROTO_DIR)/processors/token_transfer/token_transfer_event.proto https://raw.githubusercontent.com/stellar/go/protocol-23/protos/processors/token_transfer/token_transfer_event.proto
	curl -s -o $(PROTO_DIR)/asset/asset.proto https://raw.githubusercontent.com/stellar/go/protocol-23/protos/asset/asset.proto
	# Copy raw_ledger_service proto from stellar-live-source
	cp ../stellar-live-source/protos/raw_ledger_service/raw_ledger_service.proto $(PROTO_DIR)/raw_ledger_service/
	# Fix go_package in raw_ledger_service.proto
	sed -i 's|option go_package = "stellar-live-source/gen/go/raw_ledger_service";|option go_package = "github.com/withObsrvr/ttp-processor/gen/raw_ledger_service";|' $(PROTO_DIR)/raw_ledger_service/raw_ledger_service.proto
	echo "✓ Init completed for ttp-processor"

gen-proto: init
	# Generate Go code from proto files
	set -e
	# Create generated code directory
	mkdir -p $(GEN_DIR)
	# Add Go bin to PATH for protoc plugins
	export PATH="$$(go env GOPATH)/bin:$$PATH"
	# Generate server code for event_service and client code for raw_ledger_service
	protoc \
		--proto_path=$(PROTO_DIR) \
		--go_out=$(GEN_DIR) \
		--go_opt=paths=source_relative \
		--go_opt=Mprocessors/token_transfer/token_transfer_event.proto=github.com/stellar/go/processors/token_transfer \
		--go_opt=Masset/asset.proto=github.com/stellar/go/asset \
		--go-grpc_out=$(GEN_DIR) \
		--go-grpc_opt=paths=source_relative \
		--go-grpc_opt=Mprocessors/token_transfer/token_transfer_event.proto=github.com/stellar/go/processors/token_transfer \
		--go-grpc_opt=Masset/asset.proto=github.com/stellar/go/asset \
		--experimental_allow_proto3_optional \
		event_service/event_service.proto \
		raw_ledger_service/raw_ledger_service.proto
	# After generation, tidy the go.mod file
	cd $(GO_SRC_DIR) && go mod tidy
	echo "✓ Proto generation completed for ttp-processor"

build-processor: gen-proto
	set -e
	cd $(GO_SRC_DIR) && go build -o ../$(BINARY_NAME) main.go
	echo "✓ Processor build completed: $(BINARY_NAME)"

run: build-processor
	set -a # Export all variables defined from now on
	# Source environment variables if .env file exists
	if [ -f .env ]; then . .env; fi
	set +a
	./$(BINARY_NAME)

clean:
	rm -rf $(BINARY_NAME) $(GEN_DIR) $(PROTO_DIR)/raw_ledger_service $(PROTO_DIR)/processors $(PROTO_DIR)/asset
	echo "✓ Cleaned ttp-processor build artifacts"