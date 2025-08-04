.PHONY: build clean gen-proto run test help

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod

# Binary names
BINARY_NAME=contract-data-processor
CONSUMER_BINARY=postgresql-consumer

# Directories
PROTO_DIR=proto
GO_DIR=go
CONSUMER_DIR=consumer/postgresql

# Build the main processor
build:
	cd $(GO_DIR) && $(GOBUILD) -o ../$(BINARY_NAME) -v

# Build the PostgreSQL consumer
build-consumer:
	cd $(CONSUMER_DIR) && $(GOBUILD) -o ../../../$(CONSUMER_BINARY) -v

# Build all binaries
build-all: build build-consumer

# Generate protobuf code
gen-proto:
	protoc \
		--go_out=$(GO_DIR) \
		--go_opt=paths=source_relative \
		--go-grpc_out=$(GO_DIR) \
		--go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/*.proto

# Run the processor
run: build
	./$(BINARY_NAME)

# Run the PostgreSQL consumer
run-consumer: build-consumer
	./$(CONSUMER_BINARY)

# Clean build artifacts
clean:
	$(GOCLEAN)
	rm -f $(BINARY_NAME)
	rm -f $(CONSUMER_BINARY)

# Run tests
test:
	cd $(GO_DIR) && $(GOTEST) -v ./...

# Download dependencies
deps:
	cd $(GO_DIR) && $(GOMOD) download

# Tidy dependencies
tidy:
	cd $(GO_DIR) && $(GOMOD) tidy

# Help command
help:
	@echo "Available targets:"
	@echo "  build          - Build the contract data processor"
	@echo "  build-consumer - Build the PostgreSQL consumer"
	@echo "  build-all      - Build all binaries"
	@echo "  gen-proto      - Generate protobuf code"
	@echo "  run            - Build and run the processor"
	@echo "  run-consumer   - Build and run the PostgreSQL consumer"
	@echo "  clean          - Remove build artifacts"
	@echo "  test           - Run tests"
	@echo "  deps           - Download dependencies"
	@echo "  tidy           - Tidy dependencies"
	@echo "  help           - Show this help message"