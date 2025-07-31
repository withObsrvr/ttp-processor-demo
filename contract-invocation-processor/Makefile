# Contract Invocation Processor Makefile
# Build automation for the Stellar contract invocation processor

.PHONY: init gen-proto build run clean test build-consumers help

# Default target
.DEFAULT_GOAL := help

# Configuration
PROTO_DIR := protos
GO_DIR := go
GEN_DIR := $(GO_DIR)/gen
BINARY_NAME := contract-invocation-processor

# Colors for output
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

help: ## Display this help message
	@echo "Contract Invocation Processor Build System"
	@echo ""
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(GREEN)%-15s$(NC) %s\n", $$1, $$2}' $(MAKEFILE_LIST)

init: ## Initialize Go module and install dependencies
	@echo "$(YELLOW)Initializing Go module...$(NC)"
	cd $(GO_DIR) && go mod init github.com/withobsrvr/contract-invocation-processor
	cd $(GO_DIR) && go get google.golang.org/grpc
	cd $(GO_DIR) && go get google.golang.org/protobuf
	cd $(GO_DIR) && go get github.com/stellar/go
	cd $(GO_DIR) && go get go.uber.org/zap
	cd $(GO_DIR) && go get github.com/withobsrvr/flowctl/pkg/api
	cd $(GO_DIR) && go mod tidy
	@echo "$(GREEN)Go module initialized successfully$(NC)"

check-protoc: ## Check if protoc is installed
	@which protoc > /dev/null || (echo "$(RED)Error: protoc not found. Please install Protocol Buffers compiler$(NC)" && exit 1)
	@which protoc-gen-go > /dev/null || (echo "$(RED)Error: protoc-gen-go not found. Install with: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest$(NC)" && exit 1)
	@which protoc-gen-go-grpc > /dev/null || (echo "$(RED)Error: protoc-gen-go-grpc not found. Install with: go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest$(NC)" && exit 1)
	@echo "$(GREEN)Protocol Buffers tools are installed$(NC)"

gen-proto: check-protoc ## Generate Go code from proto files
	@echo "$(YELLOW)Generating protobuf code...$(NC)"
	@mkdir -p $(GEN_DIR)
	
	# Generate contract_invocation_event.proto
	protoc --go_out=$(GEN_DIR) --go-grpc_out=$(GEN_DIR) \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		-I $(PROTO_DIR) \
		$(PROTO_DIR)/contract_invocation/contract_invocation_event.proto
	
	# Generate contract_invocation_service.proto
	protoc --go_out=$(GEN_DIR) --go-grpc_out=$(GEN_DIR) \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		-I $(PROTO_DIR) \
		$(PROTO_DIR)/contract_invocation_service/contract_invocation_service.proto
	
	@echo "$(GREEN)Protobuf code generated successfully$(NC)"

build: gen-proto ## Build the binary
	@echo "$(YELLOW)Building contract invocation processor...$(NC)"
	cd $(GO_DIR) && CGO_ENABLED=0 go build -o ../$(BINARY_NAME) .
	@echo "$(GREEN)Binary built successfully: $(BINARY_NAME)$(NC)"

run: build ## Build and run the processor with default settings
	@echo "$(YELLOW)Starting contract invocation processor...$(NC)"
	NETWORK_PASSPHRASE="Public Global Stellar Network ; September 2015" \
	SOURCE_SERVICE_ADDRESS=localhost:50052 \
	PROCESSOR_PORT=50054 \
	HEALTH_PORT=8089 \
	ENABLE_FLOWCTL=true \
	FLOWCTL_ENDPOINT=localhost:8080 \
	LOG_LEVEL=info \
	./$(BINARY_NAME)

run-testnet: build ## Run processor configured for testnet
	@echo "$(YELLOW)Starting contract invocation processor for testnet...$(NC)"
	NETWORK_PASSPHRASE="Test SDF Network ; September 2015" \
	SOURCE_SERVICE_ADDRESS=localhost:50052 \
	PROCESSOR_PORT=50054 \
	HEALTH_PORT=8089 \
	ENABLE_FLOWCTL=true \
	FLOWCTL_ENDPOINT=localhost:8080 \
	LOG_LEVEL=debug \
	./$(BINARY_NAME)

test: ## Run Go tests
	@echo "$(YELLOW)Running tests...$(NC)"
	cd $(GO_DIR) && go test -v ./...
	@echo "$(GREEN)Tests completed$(NC)"

test-coverage: ## Run tests with coverage report
	@echo "$(YELLOW)Running tests with coverage...$(NC)"
	cd $(GO_DIR) && go test -v -coverprofile=coverage.out ./...
	cd $(GO_DIR) && go tool cover -html=coverage.out -o coverage.html
	@echo "$(GREEN)Coverage report generated: $(GO_DIR)/coverage.html$(NC)"

lint: ## Run Go linting
	@echo "$(YELLOW)Running linter...$(NC)"
	cd $(GO_DIR) && golangci-lint run ./...
	@echo "$(GREEN)Linting completed$(NC)"

format: ## Format Go code
	@echo "$(YELLOW)Formatting Go code...$(NC)"
	cd $(GO_DIR) && go fmt ./...
	@echo "$(GREEN)Code formatted$(NC)"

clean: ## Clean build artifacts
	@echo "$(YELLOW)Cleaning build artifacts...$(NC)"
	rm -rf $(GEN_DIR)
	rm -f $(BINARY_NAME)
	rm -f $(GO_DIR)/coverage.out $(GO_DIR)/coverage.html
	@echo "$(GREEN)Clean completed$(NC)"

clean-deps: ## Clean Go module cache
	@echo "$(YELLOW)Cleaning Go module cache...$(NC)"
	cd $(GO_DIR) && go clean -modcache
	@echo "$(GREEN)Dependencies cleaned$(NC)"

build-consumers: ## Build consumer applications
	@echo "$(YELLOW)Building consumer applications...$(NC)"
	@if [ -d ../consumer_app/contract_invocation_node ]; then \
		echo "Building Node.js consumer..."; \
		$(MAKE) -C ../consumer_app/contract_invocation_node build || echo "$(RED)Node.js consumer build failed$(NC)"; \
	fi
	@if [ -d ../consumer_app/contract_invocation_go_wasm ]; then \
		echo "Building Go WASM consumer..."; \
		$(MAKE) -C ../consumer_app/contract_invocation_go_wasm build || echo "$(RED)Go WASM consumer build failed$(NC)"; \
	fi
	@echo "$(GREEN)Consumer applications built$(NC)"

docker-build: ## Build Docker image
	@echo "$(YELLOW)Building Docker image...$(NC)"
	docker build -t contract-invocation-processor:latest .
	@echo "$(GREEN)Docker image built successfully$(NC)"

docker-run: docker-build ## Build and run Docker container
	@echo "$(YELLOW)Running Docker container...$(NC)"
	docker run --rm -p 50054:50054 -p 8089:8089 \
		-e NETWORK_PASSPHRASE="Public Global Stellar Network ; September 2015" \
		-e SOURCE_SERVICE_ADDRESS=host.docker.internal:50052 \
		-e ENABLE_FLOWCTL=true \
		-e FLOWCTL_ENDPOINT=host.docker.internal:8080 \
		contract-invocation-processor:latest

install-tools: ## Install required development tools
	@echo "$(YELLOW)Installing development tools...$(NC)"
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@echo "$(GREEN)Development tools installed$(NC)"

setup: install-tools init ## Complete setup for development
	@echo "$(GREEN)Development environment setup completed!$(NC)"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Implement the server code in $(GO_DIR)/main.go and $(GO_DIR)/server/"
	@echo "  2. Run 'make build' to compile the processor"
	@echo "  3. Run 'make test' to run tests"
	@echo "  4. Run 'make run' to start the processor"

# Development helpers
dev-watch: ## Watch for changes and rebuild (requires entr)
	@echo "$(YELLOW)Watching for changes... (requires 'entr' to be installed)$(NC)"
	find $(GO_DIR) $(PROTO_DIR) -name "*.go" -o -name "*.proto" | entr -r make build

validate: gen-proto lint test ## Run full validation (proto gen, lint, test)
	@echo "$(GREEN)All validation checks passed!$(NC)"

# CI/CD targets
ci-test: gen-proto test lint ## Run CI tests
	@echo "$(GREEN)CI tests completed$(NC)"

ci-build: gen-proto build ## Run CI build
	@echo "$(GREEN)CI build completed$(NC)"