.PHONY: build clean test run deps docker-build docker-push

# Variables
BINARY_NAME := token-discovery-processor
BUILD_DIR := bin
GO_DIR := go
DOCKER_IMAGE := token-discovery-processor
DOCKER_TAG := latest

# Build the binary
build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	cd $(GO_DIR) && CGO_ENABLED=0 go build -ldflags="-w -s" -o ../$(BUILD_DIR)/$(BINARY_NAME) .
	@echo "Built: $(BUILD_DIR)/$(BINARY_NAME)"

# Clean build artifacts
clean:
	@echo "Cleaning..."
	rm -rf $(BUILD_DIR)
	cd $(GO_DIR) && go clean

# Run tests
test:
	cd $(GO_DIR) && go test -v ./...

# Update dependencies
deps:
	cd $(GO_DIR) && go mod tidy
	cd $(GO_DIR) && go mod download

# Run locally
run: build
	./$(BUILD_DIR)/$(BINARY_NAME) -config config.yaml

# Build Docker image
docker-build:
	docker build -t $(DOCKER_IMAGE):$(DOCKER_TAG) .

# Push Docker image
docker-push: docker-build
	docker push $(DOCKER_IMAGE):$(DOCKER_TAG)

# Run with Docker
docker-run:
	docker run --rm \
		-e SILVER_HOT_HOST=host.docker.internal \
		-e TOKEN_REGISTRY_HOST=host.docker.internal \
		-p 8096:8096 \
		$(DOCKER_IMAGE):$(DOCKER_TAG)

# Apply migrations
migrate:
	@echo "Applying migrations..."
	psql -h localhost -p 5434 -d token_registry -f migrations/001_create_discovered_tokens.sql

# Check health
health:
	curl -s http://localhost:8096/health | jq .

# Check stats
stats:
	curl -s http://localhost:8096/stats | jq .

# Check metrics (Prometheus format)
metrics:
	curl -s http://localhost:8096/metrics
