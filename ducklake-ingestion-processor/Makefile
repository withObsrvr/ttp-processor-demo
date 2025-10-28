.PHONY: build run clean query

build:
	@echo "Building ducklake-ingestion-processor..."
	cd go && GOWORK=off go build -o ../ducklake-ingestion .
	@echo "✓ Build complete: ./ducklake-ingestion"

run: build
	@echo "Running ducklake-ingestion-processor..."
	./ducklake-ingestion

clean:
	@echo "Cleaning build artifacts..."
	rm -f ducklake-ingestion
	@echo "✓ Clean complete"

query:
	@echo "Querying DuckLake..."
	./query.sh
