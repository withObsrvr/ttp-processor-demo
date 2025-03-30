.PHONY: all init gen-proto build-processor run clean
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

# Initialize Go module and install dependencies
init:
	set -e

	echo "✓ Init completed for ttp-processor"

gen-proto: init
	# Generate Go code from proto files
	set -e
	# Generate server code for event_service and client code for raw_ledger_service
	protoc \
		--proto_path=$(PROTO_DIR) \
		--go_out=$(GEN_DIR) \
		--go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR) \
		--go-grpc_opt=paths=source_relative \
		--experimental_allow_proto3_optional \
		event_service/event_service.proto \
		ingest/processors/token_transfer/token_transfer_event.proto \
		ingest/asset/asset.proto \
		raw_ledger_service/raw_ledger_service.proto
	# After generation, update the go.mod file to add replace directives
	cd $(GO_SRC_DIR) && echo 'replace github.com/withObsrvr/ttp-processor/gen/event_service => ./gen/event_service' >> go.mod
	cd $(GO_SRC_DIR) && echo 'replace github.com/withObsrvr/ttp-processor/server => ./server' >> go.mod
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
	rm -rf $(BINARY_NAME) $(GEN_DIR) $(PROTO_DIR)/raw_ledger_service $(PROTO_DIR)/ingest
	echo "✓ Cleaned ttp-processor build artifacts"