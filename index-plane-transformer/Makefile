.PHONY: build run clean docker-build docker-push test init

# Service name
SERVICE := index-plane-transformer
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
REGISTRY := withobsrvr

# Build the service
build:
	@echo "Building $(SERVICE)..."
	cd go && go build -o ../$(SERVICE) .
	@echo "✅ Build complete: ./$(SERVICE)"

# Initialize Go module and dependencies
init:
	@echo "Initializing Go module..."
	cd go && go mod init github.com/$(REGISTRY)/$(SERVICE) || true
	cd go && go mod tidy
	@echo "✅ Dependencies installed"

# Run the service locally
run: build
	@echo "Running $(SERVICE)..."
	./$(SERVICE) -config config.yaml

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	rm -f $(SERVICE)
	rm -rf go/vendor
	@echo "✅ Clean complete"

# Build Docker image
docker-build:
	@echo "Building Docker image: $(REGISTRY)/$(SERVICE):$(VERSION)"
	docker build -t $(REGISTRY)/$(SERVICE):$(VERSION) .
	docker tag $(REGISTRY)/$(SERVICE):$(VERSION) $(REGISTRY)/$(SERVICE):latest
	@echo "✅ Docker image built"

# Push Docker image to registry
docker-push: docker-build
	@echo "Pushing Docker image to registry..."
	docker push $(REGISTRY)/$(SERVICE):$(VERSION)
	docker push $(REGISTRY)/$(SERVICE):latest
	@echo "✅ Docker image pushed"

# Run tests
test:
	@echo "Running tests..."
	cd go && go test -v ./...

# Install dependencies
deps:
	@echo "Installing dependencies..."
	cd go && go get github.com/duckdb/duckdb-go/v2
	cd go && go get github.com/lib/pq
	cd go && go get gopkg.in/yaml.v3
	cd go && go mod tidy
	@echo "✅ Dependencies installed"

# Format code
fmt:
	@echo "Formatting code..."
	cd go && go fmt ./...
	@echo "✅ Code formatted"

# Lint code
lint:
	@echo "Linting code..."
	cd go && go vet ./...
	@echo "✅ Linting complete"

# Show help
help:
	@echo "Available targets:"
	@echo "  make build        - Build the service binary"
	@echo "  make run          - Run the service locally"
	@echo "  make clean        - Clean build artifacts"
	@echo "  make docker-build - Build Docker image"
	@echo "  make docker-push  - Build and push Docker image"
	@echo "  make test         - Run tests"
	@echo "  make deps         - Install dependencies"
	@echo "  make fmt          - Format code"
	@echo "  make lint         - Lint code"
	@echo "  make init         - Initialize Go module"
