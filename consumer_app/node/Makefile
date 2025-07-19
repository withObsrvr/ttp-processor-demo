.PHONY: build clean install gen-proto nix-build nix-shell nix-docker nix-docker-load nix-run docker-build docker-run

# Node.js parameters
NODE_CMD=node
NPM_CMD=npm
BINARY_NAME=ttp-consumer-node
BUILD_DIR=dist

# Nix parameters
NIX=nix
NIX_BUILD=$(NIX) build
NIX_DEVELOP=$(NIX) develop
NIX_RUN=$(NIX) run

# Docker parameters
DOCKER_IMAGE=ttp-consumer-node
DOCKER_TAG=latest

all: build

# Install dependencies
install:
	$(NPM_CMD) install
	echo "✓ Dependencies installed"

# Generate protobuf files
gen-proto: install
	# Create proto directories and download Protocol 23 proto files
	mkdir -p ../protos/processors/token_transfer ../protos/asset ../protos/event_service
	# Download Protocol 23 proto files from stellar/go to match ttp-processor structure
	curl -s -o ../protos/processors/token_transfer/token_transfer_event.proto https://raw.githubusercontent.com/stellar/go/protocol-23/protos/processors/token_transfer/token_transfer_event.proto
	curl -s -o ../protos/asset/asset.proto https://raw.githubusercontent.com/stellar/go/protocol-23/protos/asset/asset.proto
	# Copy event_service proto from ttp-processor
	cp ../../ttp-processor/protos/event_service/event_service.proto ../protos/event_service/
	# Generate TypeScript protobuf files
	mkdir -p gen
	protoc \
		--plugin=protoc-gen-ts=./node_modules/.bin/protoc-gen-ts \
		--proto_path=../protos \
		--ts_out=grpc_js:./gen \
		--ts_opt=esModuleInterop=true \
		--experimental_allow_proto3_optional \
		event_service/event_service.proto \
		processors/token_transfer/token_transfer_event.proto \
		asset/asset.proto
	echo "✓ Protobuf generation completed"

# Build the application
build: gen-proto
	$(NPM_CMD) run build
	echo "✓ Application build completed: $(BUILD_DIR)"

# Clean build artifacts
clean:
	rm -rf $(BUILD_DIR)
	rm -rf gen
	rm -rf node_modules
	rm -rf result
	echo "✓ Clean completed"

# Run the application
run: build
	$(NPM_CMD) run start

# Run in development mode
dev: install
	$(NPM_CMD) run dev

# Run tests (if any)
test:
	$(NPM_CMD) test

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
	docker run -p 8088:8088 \
		-e NODE_ENV=production \
		-e ENABLE_FLOWCTL=true \
		-e FLOWCTL_ENDPOINT=host.docker.internal:8080 \
		$(DOCKER_IMAGE):$(DOCKER_TAG)

# --- CI/CD shortcuts ---

# Complete build process (for CI)
ci-build: nix-build nix-docker

# Release process (build and load Docker image)
release: nix-build docker-build