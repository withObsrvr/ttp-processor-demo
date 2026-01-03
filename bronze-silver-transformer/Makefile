.PHONY: all build clean test run docker-build docker-run deploy-systemd install-systemd check-health

# Variables
BINARY_NAME=bronze-silver-transformer
BUILD_DIR=bin
CONFIG_FILE=config.yaml
DOCKER_IMAGE=$(BINARY_NAME):latest

all: build

# Build binary
build:
	@echo "Building $(BINARY_NAME)..."
	cd go && go build -o ../$(BUILD_DIR)/$(BINARY_NAME)
	@echo "Binary created at $(BUILD_DIR)/$(BINARY_NAME)"

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	rm -rf $(BUILD_DIR)
	rm -f go/go.sum

# Run tests
test:
	@echo "Running tests..."
	cd go && go test -v ./...

# Run locally (development)
run: build
	@echo "Running $(BINARY_NAME) with $(CONFIG_FILE)..."
	./$(BUILD_DIR)/$(BINARY_NAME) -config $(CONFIG_FILE)

# Run with test config
run-test: build
	@echo "Running $(BINARY_NAME) with test config..."
	./$(BUILD_DIR)/$(BINARY_NAME) -config config.test.yaml

# Build Docker image
docker-build:
	@echo "Building Docker image $(DOCKER_IMAGE)..."
	docker build -t $(DOCKER_IMAGE) .

# Run Docker container
docker-run: docker-build
	@echo "Running Docker container..."
	docker run -d \
		--name $(BINARY_NAME) \
		--restart unless-stopped \
		-p 8093:8093 \
		-v $(PWD)/$(CONFIG_FILE):/app/config.yaml:ro \
		$(DOCKER_IMAGE)

# Stop Docker container
docker-stop:
	@echo "Stopping Docker container..."
	docker stop $(BINARY_NAME) || true
	docker rm $(BINARY_NAME) || true

# Deploy with docker-compose
docker-compose-up:
	@echo "Starting services with docker-compose..."
	docker-compose up -d

docker-compose-down:
	@echo "Stopping services with docker-compose..."
	docker-compose down

# Install systemd service (requires sudo)
install-systemd: build
	@echo "Installing systemd service..."
	sudo mkdir -p /opt/$(BINARY_NAME)
	sudo cp $(BUILD_DIR)/$(BINARY_NAME) /opt/$(BINARY_NAME)/
	sudo cp $(CONFIG_FILE) /opt/$(BINARY_NAME)/
	sudo cp $(BINARY_NAME).service /etc/systemd/system/
	sudo systemctl daemon-reload
	@echo "Service installed. Enable and start with:"
	@echo "  sudo systemctl enable $(BINARY_NAME)"
	@echo "  sudo systemctl start $(BINARY_NAME)"

# Deploy to server (systemd)
deploy-systemd: install-systemd
	@echo "Enabling and starting $(BINARY_NAME) service..."
	sudo systemctl enable $(BINARY_NAME)
	sudo systemctl restart $(BINARY_NAME)
	@echo "Service deployed and started"
	@echo "Check status: sudo systemctl status $(BINARY_NAME)"

# Check health endpoint
check-health:
	@echo "Checking health endpoint..."
	@curl -f http://localhost:8093/health || echo "Health check failed"

# View service logs (systemd)
logs-systemd:
	sudo journalctl -u $(BINARY_NAME) -f

# View service logs (docker)
logs-docker:
	docker logs -f $(BINARY_NAME)

# View service status (systemd)
status-systemd:
	sudo systemctl status $(BINARY_NAME)

# View service status (docker)
status-docker:
	docker ps -f name=$(BINARY_NAME)

# Restart service (systemd)
restart-systemd:
	sudo systemctl restart $(BINARY_NAME)

# Restart service (docker)
restart-docker: docker-stop docker-run

# Development: format code
fmt:
	cd go && go fmt ./...

# Development: lint code
lint:
	cd go && golangci-lint run ./...

# Development: run with hot reload (requires air)
dev:
	cd go && air

# Help
help:
	@echo "Available targets:"
	@echo "  build              - Build binary"
	@echo "  clean              - Clean build artifacts"
	@echo "  test               - Run tests"
	@echo "  run                - Run locally with config.yaml"
	@echo "  run-test           - Run locally with config.test.yaml"
	@echo ""
	@echo "Docker deployment:"
	@echo "  docker-build       - Build Docker image"
	@echo "  docker-run         - Run Docker container"
	@echo "  docker-stop        - Stop Docker container"
	@echo "  docker-compose-up  - Start with docker-compose"
	@echo "  docker-compose-down- Stop docker-compose"
	@echo ""
	@echo "Systemd deployment:"
	@echo "  install-systemd    - Install systemd service files"
	@echo "  deploy-systemd     - Deploy and start systemd service"
	@echo "  logs-systemd       - View systemd logs"
	@echo "  status-systemd     - Check systemd status"
	@echo "  restart-systemd    - Restart systemd service"
	@echo ""
	@echo "Monitoring:"
	@echo "  check-health       - Check health endpoint"
	@echo "  logs-docker        - View Docker logs"
	@echo "  status-docker      - Check Docker status"
	@echo ""
	@echo "Development:"
	@echo "  fmt                - Format code"
	@echo "  lint               - Lint code"
	@echo "  dev                - Run with hot reload (requires air)"
