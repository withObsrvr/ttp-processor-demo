.PHONY: build run clean

BINARY_NAME=contract-events-consumer

build:
	@echo "Building contract events consumer..."
	@go build -o $(BINARY_NAME) main.go
	@echo "✓ Build completed: $(BINARY_NAME)"

run: build
	@./$(BINARY_NAME) 1000 1100

clean:
	@rm -f $(BINARY_NAME)
	@echo "✓ Cleaned"
