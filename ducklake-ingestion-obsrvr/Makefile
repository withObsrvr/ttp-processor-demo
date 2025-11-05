.PHONY: build run clean query metadata-query help

help:
	@echo "DuckLake Ingestion Processor - Obsrvr v2.0"
	@echo ""
	@echo "Available targets:"
	@echo "  make build            - Build ducklake-ingestion-obsrvr"
	@echo "  make run              - Build and run processor"
	@echo "  make clean            - Remove build artifacts"
	@echo "  make query            - Query data tables"
	@echo "  make metadata-query   - Query metadata tables"
	@echo ""

build:
	@echo "Building ducklake-ingestion-obsrvr..."
	cd go && GOWORK=off go build -o ../ducklake-ingestion-obsrvr .
	@echo "✓ Build complete: ./ducklake-ingestion-obsrvr"

run: build
	@echo "Running ducklake-ingestion-obsrvr..."
	./ducklake-ingestion-obsrvr

clean:
	@echo "Cleaning build artifacts..."
	rm -f ducklake-ingestion-obsrvr
	@echo "✓ Clean complete"

query:
	@echo "Querying DuckLake data tables..."
	./query.sh

metadata-query:
	@echo "Querying Obsrvr metadata tables..."
	@echo "This will show dataset registry, lineage, quality checks, and schema changes"
	./metadata-query.sh
