.PHONY: all build run clean test

# Build the ingester
all: build

build:
	cd go && go build -o ../bin/stellar-postgres-ingester .

# Run the ingester
run: build
	./bin/stellar-postgres-ingester -config config.yaml

# Run with example config
run-example: build
	@if [ ! -f config.yaml ]; then \
		echo "Creating config.yaml from config.yaml.example..."; \
		cp config.yaml.example config.yaml; \
	fi
	./bin/stellar-postgres-ingester -config config.yaml

# Clean build artifacts
clean:
	rm -rf bin/
	cd go && go clean

# Run tests
test:
	cd go && go test -v ./...

# Download dependencies
deps:
	cd go && go mod download
	cd go && go mod tidy

# Format code
fmt:
	cd go && go fmt ./...

# Check for common issues
vet:
	cd go && go vet ./...

# Install to GOPATH
install:
	cd go && go install .

# Show help
help:
	@echo "Available targets:"
	@echo "  build        - Build the ingester binary"
	@echo "  run          - Build and run with config.yaml"
	@echo "  run-example  - Build and run with config.yaml.example"
	@echo "  clean        - Remove build artifacts"
	@echo "  test         - Run tests"
	@echo "  deps         - Download and tidy dependencies"
	@echo "  fmt          - Format code"
	@echo "  vet          - Run go vet"
	@echo "  install      - Install to GOPATH"
