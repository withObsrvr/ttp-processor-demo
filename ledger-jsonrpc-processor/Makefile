# Makefile for the Ledger-to-JSON-RPC processor

# Variables
GO := go
PROTOC := protoc
BINARY_NAME := ledger-jsonrpc-processor
BINARY_DIR := bin
SRC_DIR := go
PROTO_DIR := protos
GEN_DIR := gen
MODULE := github.com/withObsrvr/ledger-jsonrpc-processor

.PHONY: all clean build test proto run docker

all: proto build

# Build the binary
build: proto
	mkdir -p $(BINARY_DIR)
	$(GO) build -o $(BINARY_DIR)/$(BINARY_NAME) $(SRC_DIR)/main.go

# Run tests
test:
	$(GO) test -v ./...

# Generate protobuf code
proto:
	mkdir -p $(GEN_DIR)/ledger_jsonrpc_service
	mkdir -p $(GEN_DIR)/raw_ledger_service
	
	# Generate protobuf code for the ledger-jsonrpc service
	$(PROTOC) --proto_path=$(PROTO_DIR) \
		--go_out=$(GEN_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR) --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/ledger_jsonrpc_service/ledger_jsonrpc_service.proto
	
	# Generate protobuf code for the raw ledger service (dependency)
	$(PROTOC) --proto_path=$(PROTO_DIR) \
		--go_out=$(GEN_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR) --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/raw_ledger_service/raw_ledger_service.proto

# Run the processor
run: build
	./$(BINARY_DIR)/$(BINARY_NAME)

# Build Docker image
docker: build
	docker build -t obsrvr/ledger-jsonrpc:1.0 -f Dockerfile.ledger_jsonrpc .

# Clean build artifacts
clean:
	rm -rf $(BINARY_DIR)
	rm -rf $(GEN_DIR)

# Help target
help:
	@echo "Available targets:"
	@echo "  all        - Generate protobuf code and build binary"
	@echo "  build      - Build the binary"
	@echo "  test       - Run tests"
	@echo "  proto      - Generate protobuf code"
	@echo "  run        - Run the processor"
	@echo "  docker     - Build Docker image"
	@echo "  clean      - Clean build artifacts"