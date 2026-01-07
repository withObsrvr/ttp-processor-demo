.PHONY: build run clean docker-build docker-push

# Service name
SERVICE := postgres-ducklake-flusher
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
REGISTRY := withobsrvr

build:
	cd go && go build -o bin/postgres-ducklake-flusher

run: build
	./go/bin/postgres-ducklake-flusher -config config.yaml

clean:
	rm -rf go/bin

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
