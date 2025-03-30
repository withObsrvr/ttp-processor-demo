.PHONY: all init gen-proto build-server run clean
.SILENT:

BINARY_NAME=stellar_live_source_server
GO_SRC_DIR=./go
GEN_DIR=$(GO_SRC_DIR)/gen
PROTO_DIR=./protos
GO_PACKAGE_BASE=github.com/stellar/stellar-live-source

all: build-server

init:
	set -e
	# Ensure Go module exists, create if not
	cd $(GO_SRC_DIR) && go mod init $(GO_PACKAGE_BASE) > /dev/null 2>&1 || true
	cd $(GO_SRC_DIR) && go mod tidy
	rm -rf $(GEN_DIR) > /dev/null 2>&1
	mkdir -p $(GEN_DIR) > /dev/null 2>&1
	echo "✓ Init completed for stellar-live-source"

gen-proto: init
	# Generate Go code from proto file
	set -e
	protoc \
		--proto_path=$(PROTO_DIR) \
		--go_out=$(GEN_DIR) \
		--go_opt=paths=source_relative \
		--go_opt=Mraw_ledger_service/raw_ledger_service.proto=$(GO_PACKAGE_BASE)/gen/raw_ledger_service \
		--go-grpc_out=$(GEN_DIR) \
		--go-grpc_opt=paths=source_relative \
		--go-grpc_opt=Mraw_ledger_service/raw_ledger_service.proto=$(GO_PACKAGE_BASE)/gen/raw_ledger_service \
		raw_ledger_service/raw_ledger_service.proto
	echo "✓ Proto generation completed for stellar-live-source"

build-server: gen-proto
	set -e
	cd $(GO_SRC_DIR) && go build -o ../$(BINARY_NAME) main.go
	echo "✓ Server build completed: $(BINARY_NAME)"

run: build-server
	set -a # Export all variables defined from now on
	# Source environment variables if .env file exists
	if [ -f .env ]; then . .env; fi
	set +a
	./$(BINARY_NAME)

clean:
	rm -rf $(BINARY_NAME) $(GEN_DIR)
	echo "✓ Cleaned stellar-live-source build artifacts"