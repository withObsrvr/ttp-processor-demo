.PHONY: all build clean run test

# Variables
BINARY_NAME=silver-realtime-transformer
BUILD_DIR=bin
CONFIG_FILE=config.yaml

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

# Run locally (development)
run: build
	@echo "Running $(BINARY_NAME) with $(CONFIG_FILE)..."
	./$(BUILD_DIR)/$(BINARY_NAME) -config $(CONFIG_FILE)

# Download dependencies
deps:
	@echo "Downloading dependencies..."
	cd go && go mod download

# Run tests
test:
	@echo "Running tests..."
	cd go && go test -v ./...

# Tidy modules
tidy:
	@echo "Tidying go modules..."
	cd go && go mod tidy

# Install dependencies
install:
	cd go && go get github.com/lib/pq
	cd go && go get gopkg.in/yaml.v3
	cd go && go mod tidy
