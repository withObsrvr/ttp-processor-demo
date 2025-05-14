.PHONY: build clean generate-proto nix-build nix-shell nix-docker nix-docker-load nix-run vendor docker-build docker-run

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
BINARY_NAME=stellar_live_source_datalake

# Nix parameters
NIX=nix
NIX_BUILD=$(NIX) build
NIX_DEVELOP=$(NIX) develop
NIX_RUN=$(NIX) run

# Docker parameters
DOCKER_IMAGE=stellar-live-source-datalake
DOCKER_TAG=latest

# Proto parameters
PROTOC=protoc
GO_SRC_DIR=./go
GEN_DIR=$(GO_SRC_DIR)/gen
PROTO_DIR=./protos
GO_PACKAGE_BASE=github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake

all: build-server

# Build the server
build-server: gen-proto
	set -e
	cd $(GO_SRC_DIR) && $(GOBUILD) -o ../$(BINARY_NAME) main.go
	echo "✓ Server build completed: $(BINARY_NAME)"

# Build with vendored dependencies (offline mode)
build-server-offline:
	set -e
	cd $(GO_SRC_DIR) && GO111MODULE=on GOPROXY=off go build -mod=vendor -o ../$(BINARY_NAME) main.go
	echo "✓ Server build completed in offline mode: $(BINARY_NAME)"

# Clean build artifacts
clean:
	$(GOCLEAN)
	rm -f $(BINARY_NAME)
	rm -rf $(GEN_DIR)
	rm -rf result

# Initialize build environment
init:
	set -e
	rm -rf $(GEN_DIR) > /dev/null 2>&1
	mkdir -p $(GEN_DIR) > /dev/null 2>&1
	echo "✓ Init completed for stellar-live-source-datalake"

# Generate protobuf code
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
	# After generation, update the go.mod file to add replace directives
	cd $(GO_SRC_DIR) && echo 'replace github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/gen/raw_ledger_service => ./gen/raw_ledger_service' >> go.mod
	cd $(GO_SRC_DIR) && echo 'replace github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/server => ./server' >> go.mod
	cd $(GO_SRC_DIR) && go mod tidy
	echo "✓ Proto generation completed for stellar-live-source-datalake"

# Run the server
run: build-server
	./$(BINARY_NAME)

# Run tests
test:
	$(GOTEST) -v ./...

# Vendor dependencies for offline builds
vendor:
	cd $(GO_SRC_DIR) && $(GOCMD) mod tidy && $(GOCMD) mod vendor
	echo "✓ Dependencies vendored for offline builds"

# --- Nix-based build targets ---

# Build with Nix
nix-build:
	$(NIX_BUILD)
	@echo "✓ Nix build completed. Binary is available at ./result/bin/$(BINARY_NAME)"

# Build Docker image with Nix
nix-docker:
	$(NIX_BUILD) .#docker
	@echo "✓ Docker image built with Nix. Load with: make nix-docker-load"

# Load Nix-built Docker image
nix-docker-load:
	docker load < result
	@echo "✓ Docker image loaded: $(DOCKER_IMAGE):$(DOCKER_TAG)"

# Run with Nix
nix-run:
	$(NIX_RUN)

# Enter Nix development shell
nix-shell:
	$(NIX_DEVELOP)

# --- Docker targets ---

# Build Docker image from Nix-built binary
docker-build: nix-build
	cp ./result/bin/$(BINARY_NAME) ./$(BINARY_NAME)
	docker build -t $(DOCKER_IMAGE):$(DOCKER_TAG) .
	rm -f ./$(BINARY_NAME)
	@echo "✓ Docker image built: $(DOCKER_IMAGE):$(DOCKER_TAG)"

# Run Docker container
docker-run:
	docker run -p 50052:50052 -p 8088:8088 \
		-e STORAGE_TYPE=FS \
		-e ENABLE_FLOWCTL=true \
		-e FLOWCTL_ENDPOINT=host.docker.internal:8080 \
		$(DOCKER_IMAGE):$(DOCKER_TAG)

# --- CI/CD shortcuts ---

# Complete build process (for CI)
ci-build: nix-build nix-docker

# Release process (build and load Docker image)
release: nix-build docker-build