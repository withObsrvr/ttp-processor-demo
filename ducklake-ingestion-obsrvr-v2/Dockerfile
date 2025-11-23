# Multi-stage build for ducklake-ingestion-obsrvr-v2
# Follows flowctl component image specification
#
# NOTE: This Dockerfile must be built from ~/Documents/ (parent of both flowctl and ttp-processor-demo)
# because go.mod has replace directives pointing to ../../../flowctl
#
# Usage:
#   cd ~/Documents
#   docker build -f ttp-processor-demo/ducklake-ingestion-obsrvr-v2/Dockerfile -t withobsrvr/ducklake-ingestion-obsrvr-v2:latest .

# Stage 1: Builder - Use Debian for glibc compatibility with DuckDB
FROM golang:1.24-bookworm AS builder

# Install build dependencies including gcc for DuckDB CGO
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    make \
    gcc \
    g++ \
    libc6-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy only the required directories (flowctl + ttp-processor-demo)
# This keeps the build context smaller while satisfying replace directives
COPY flowctl ./flowctl
COPY ttp-processor-demo ./ttp-processor-demo

# Set working directory to the ducklake processor
WORKDIR /build/ttp-processor-demo/ducklake-ingestion-obsrvr-v2/go

# Build the binary with CGO enabled for DuckDB
# Using dynamic linking (no -static) because DuckDB requires glibc
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 GOWORK=off go build \
    -ldflags="-s -w" \
    -o ducklake-ingestion-obsrvr-v2 \
    .

# Stage 2: Runtime - Use Debian slim for glibc compatibility
FROM debian:bookworm-slim

# Install runtime dependencies for DuckDB
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -g 65532 nonroot \
    && useradd -u 65532 -g nonroot -s /bin/false nonroot

# Copy binary to /app/ following flowctl component image spec
COPY --from=builder /build/ttp-processor-demo/ducklake-ingestion-obsrvr-v2/go/ducklake-ingestion-obsrvr-v2 /app/ducklake-ingestion-obsrvr-v2

# Ensure binary is executable
RUN chmod +x /app/ducklake-ingestion-obsrvr-v2

# Required OCI labels for flowctl component discovery
LABEL io.flowctl.component.type="processor"
LABEL io.flowctl.component.api-version="v1"
LABEL io.flowctl.component.name="ducklake-ingestion-obsrvr-v2"
LABEL io.flowctl.component.description="DuckDB/DuckLake ingestion processor for Stellar data"
LABEL io.flowctl.component.input-events="raw_ledger_service.RawLedgerChunk"

# Additional metadata labels
LABEL org.opencontainers.image.source="https://github.com/withObsrvr/ttp-processor-demo"
LABEL org.opencontainers.image.title="DuckLake Ingestion Processor v2"
LABEL org.opencontainers.image.description="Stellar blockchain data processor for DuckDB/DuckLake"
LABEL org.opencontainers.image.vendor="Obsrvr"

# Health check port
EXPOSE 8082

# Run as non-root user
USER nonroot:nonroot

# Set working directory
WORKDIR /app

# Entrypoint
ENTRYPOINT ["/app/ducklake-ingestion-obsrvr-v2"]
