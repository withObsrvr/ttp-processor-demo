.PHONY: poc build run clean help

help:
	@echo "ducklake-ingestion-obsrvr-v2 (Appender API)"
	@echo ""
	@echo "Day 1: POC Validation"
	@echo "  make poc     - Build and run Appender API POC"
	@echo ""
	@echo "Available targets:"
	@echo "  make build   - Build v2 processor"
	@echo "  make run     - Build and run processor"
	@echo "  make clean   - Remove build artifacts"
	@echo ""

poc:
	@echo "🔬 Building Simple Appender POC (no DuckLake)..."
	cd go && GOWORK=off go build -o ../appender_simple_poc appender_simple_poc.go
	@echo "✓ Simple POC built"
	@echo ""
	@echo "Running simple POC..."
	@echo ""
	./appender_simple_poc

poc-ducklake:
	@echo "🔬 Building DuckLake Appender POC..."
	cd go && GOWORK=off go build -o ../appender_ducklake_poc appender_ducklake_poc.go
	@echo "✓ DuckLake POC built"
	@echo ""
	@echo "Running DuckLake POC..."
	@echo ""
	./appender_ducklake_poc

build:
	@echo "Building ducklake-ingestion-obsrvr-v2..."
	cd go && GOWORK=off go build -o ../ducklake-ingestion-obsrvr-v2 .
	@echo "✓ Build complete: ./ducklake-ingestion-obsrvr-v2"

run: build
	@echo "Running ducklake-ingestion-obsrvr-v2..."
	./ducklake-ingestion-obsrvr-v2

clean:
	@echo "Cleaning build artifacts..."
	rm -f appender_poc appender_simple_poc appender_ducklake_poc
	rm -f ducklake-ingestion-obsrvr ducklake-ingestion-obsrvr-v2
	rm -f ducklake-query
	rm -rf poc_test_data
	@echo "✓ Clean complete"
