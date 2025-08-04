.PHONY: build run test clean deps

# Build the Arrow consumer demo
build: deps
	go build -o arrow-consumer-demo .

# Download dependencies
deps:
	go mod download
	go mod tidy

# Run with example parameters
run: build
	./arrow-consumer-demo 1000 1100

# Run with custom endpoint
run-custom: build
	./arrow-consumer-demo 1000 1100 localhost:8815

# Test against mock data
test-mock:
	MOCK_DATA=true go run ../../stellar-arrow-source/go/main.go &
	sleep 2
	./arrow-consumer-demo 1000 1100
	pkill -f stellar-arrow-source

# Clean build artifacts
clean:
	rm -f arrow-consumer-demo
	go clean -cache

# Run go tests
test:
	go test -v ./...

# Format code
fmt:
	go fmt ./...

# Run linter
lint:
	golangci-lint run

help:
	@echo "Available targets:"
	@echo "  build       - Build the Arrow consumer demo"
	@echo "  run         - Run demo with default parameters (ledgers 1000-1100)"
	@echo "  run-custom  - Run demo with custom endpoint"
	@echo "  test-mock   - Test against mock stellar-arrow-source"
	@echo "  clean       - Remove build artifacts"
	@echo "  deps        - Download dependencies"
	@echo "  fmt         - Format code"
	@echo "  lint        - Run linter"