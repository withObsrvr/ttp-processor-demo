.PHONY: build run clean test

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
