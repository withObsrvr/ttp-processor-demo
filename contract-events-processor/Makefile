.PHONY: all init gen-proto build run clean vendor nix-build nix-run docker-build docker-run

BINARY_NAME=contract_events_processor
GO_SRC_DIR=./go
GEN_DIR=$(GO_SRC_DIR)/gen
PROTO_DIR=./protos
GO_PACKAGE_BASE=github.com/withObsrvr/contract-events-processor

# Location of stellar-live-source proto file
SOURCE_PROTO_FILE=../stellar-live-source/protos/raw_ledger_service/raw_ledger_service.proto

all: build

# Initialize Go module
init:
	@echo "Initializing contract-events-processor..."
	@# Only download if gen directories exist
	@if [ -d "$(GEN_DIR)/contract_event_service" ] && [ -d "$(GEN_DIR)/raw_ledger_service" ]; then \
		cd $(GO_SRC_DIR) && go mod download; \
	fi
	@echo "✓ Init completed"

gen-proto: init
	@echo "Generating protobuf code..."
	@mkdir -p $(GEN_DIR)/contract_event_service
	@mkdir -p $(GEN_DIR)/raw_ledger_service
	@# Copy stellar-live-source proto file
	@cp $(SOURCE_PROTO_FILE) $(PROTO_DIR)/raw_ledger_service.proto
	@# Generate code for both services
	@protoc \
		--proto_path=$(PROTO_DIR) \
		--go_out=$(GEN_DIR)/contract_event_service \
		--go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR)/contract_event_service \
		--go-grpc_opt=paths=source_relative \
		--experimental_allow_proto3_optional \
		contract_event_service.proto
	@protoc \
		--proto_path=$(PROTO_DIR) \
		--go_out=$(GEN_DIR)/raw_ledger_service \
		--go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR)/raw_ledger_service \
		--go-grpc_opt=paths=source_relative \
		raw_ledger_service.proto
	@# Create go.mod files for generated packages
	@printf 'module github.com/withObsrvr/contract-events-processor/gen/contract_event_service\n\ngo 1.24\n\nrequire (\n\tgoogle.golang.org/grpc v1.72.0\n\tgoogle.golang.org/protobuf v1.36.6\n)\n' > $(GEN_DIR)/contract_event_service/go.mod
	@printf 'module github.com/withObsrvr/contract-events-processor/gen/raw_ledger_service\n\ngo 1.24\n\nrequire (\n\tgoogle.golang.org/grpc v1.72.0\n\tgoogle.golang.org/protobuf v1.36.6\n)\n' > $(GEN_DIR)/raw_ledger_service/go.mod
	@cd $(GEN_DIR)/contract_event_service && go mod tidy
	@cd $(GEN_DIR)/raw_ledger_service && go mod tidy
	@cd $(GO_SRC_DIR) && go mod tidy
	@echo "✓ Proto generation completed"

build: gen-proto
	@echo "Building contract-events-processor..."
	@cd $(GO_SRC_DIR) && go build -o ../$(BINARY_NAME) main.go
	@echo "✓ Build completed: $(BINARY_NAME)"

run: build
	@set -a; \
	if [ -f .env ]; then . .env; fi; \
	set +a; \
	./$(BINARY_NAME)

vendor:
	@echo "Vendoring Go dependencies..."
	@cd $(GO_SRC_DIR) && GOWORK=off go mod tidy && GOWORK=off go mod vendor
	@echo "✓ Dependencies vendored"

nix-build: vendor
	@echo "Building with Nix..."
	@nix build
	@echo "✓ Nix build completed"

nix-run:
	@echo "Running with Nix..."
	@nix run

docker-build: vendor
	@echo "Building Docker image with Nix..."
	@nix build .#docker
	@echo "Loading image into Docker..."
	@docker load < result
	@echo "✓ Docker image built: withobsrvr/contract-events-processor:latest"

docker-run:
	@echo "Running Docker container..."
	@docker run --rm \
		-p 50053:50053 \
		-p 8089:8089 \
		-e LIVE_SOURCE_ADDRESS=host.docker.internal:50052 \
		-e NETWORK_PASSPHRASE="Test SDF Network ; September 2015" \
		withobsrvr/contract-events-processor:latest

clean:
	@rm -rf $(BINARY_NAME) $(GEN_DIR) $(PROTO_DIR)/raw_ledger_service.proto
	@rm -rf result
	@rm -rf $(GO_SRC_DIR)/vendor
	@echo "✓ Cleaned build artifacts"
