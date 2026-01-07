.PHONY: build run clean test docker-build docker-push

# Service name
SERVICE := silver-cold-flusher
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
REGISTRY := withobsrvr

# Build the flusher binary
build:
	@echo "Building silver-cold-flusher..."
	@cd go && go build -o ../bin/silver-cold-flusher
	@echo "Binary created at bin/silver-cold-flusher"

# Run the flusher
run:
	@./bin/silver-cold-flusher -config config.yaml

# Clean build artifacts
clean:
	@echo "Cleaning..."
	@rm -rf bin/
	@echo "Clean complete"

# Run tests
test:
	@cd go && go test -v ./...

# Install dependencies
deps:
	@cd go && go mod tidy
	@cd go && go mod download

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
