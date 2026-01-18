.PHONY: all build run clean swagger swagger-install test

# Binary name
BINARY_NAME=stellar-query-api

# Build the binary
build:
	go build -o $(BINARY_NAME) .

# Run the service
run: build
	./$(BINARY_NAME)

# Run tests
test:
	go test ./...

# Clean build artifacts
clean:
	rm -f $(BINARY_NAME)
	rm -rf docs/

# Install swag CLI tool
swagger-install:
	go install github.com/swaggo/swag/cmd/swag@v1.16.4

# Generate swagger docs
# Requires swag CLI: make swagger-install
swagger:
	swag init --parseDependency --parseInternal --parseDepth 1 \
		--generalInfo main_silver.go \
		--output docs \
		--outputTypes go,json,yaml
	@echo "Swagger docs generated in docs/"
	@echo "Access Swagger UI at http://localhost:PORT/swagger/index.html when server is running"

# Format swagger annotations
swagger-fmt:
	swag fmt

# Full rebuild with swagger
all: clean swagger build

# Development: regenerate docs and run
dev: swagger run
