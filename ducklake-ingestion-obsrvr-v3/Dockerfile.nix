# Multi-stage Dockerfile using Nix for reproducible builds
# Based on "Using Nix with Dockerfiles" pattern
#
# This approach provides:
# - Reproducible builds across dev/CI/production
# - Single source of truth (flake.nix)
# - Eliminates "works on my machine" problems
# - Deterministic dependencies via Nix
#
# Prerequisites:
# - Must have go/vendor directory populated (run: cd go && GOWORK=off go mod vendor)
# - Nix flakes must be enabled
#
# Usage from repository root:
#   docker build -f ducklake-ingestion-obsrvr-v3/Dockerfile.nix -t ducklake-ingestion-obsrvr-v3:nix ducklake-ingestion-obsrvr-v3

# ============================================================================
# Stage 1: Nix Builder
# ============================================================================
FROM nixos/nix:latest AS builder

# Enable flakes support
RUN echo "experimental-features = nix-command flakes" >> /etc/nix/nix.conf

# Copy the entire source tree (flake.nix needs full context)
WORKDIR /build
COPY . .

# Build with Nix using the flake
# This will:
# 1. Use vendored Go dependencies (vendorHash = null)
# 2. Build both ducklake-ingestion-obsrvr-v3 and resolver-test
# 3. Install to /nix/store with deterministic hash
RUN nix build --print-build-logs

# Extract the binary from Nix store to a known location
# The Nix store path is deterministic but includes a hash, so we copy it out
RUN mkdir -p /output && \
    cp -L result/bin/ducklake-ingestion-obsrvr-v3 /output/ && \
    cp -L result/bin/resolver-test /output/

# ============================================================================
# Stage 2: Minimal Runtime
# ============================================================================
FROM debian:bookworm-slim

# Install only runtime dependencies needed for DuckDB
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -g 65532 nonroot \
    && useradd -u 65532 -g nonroot -s /bin/false nonroot

# Copy Nix-built binaries from builder stage
COPY --from=builder /output/ducklake-ingestion-obsrvr-v3 /app/ducklake-ingestion-obsrvr-v3
COPY --from=builder /output/resolver-test /app/resolver-test

# Ensure binaries are executable
RUN chmod +x /app/ducklake-ingestion-obsrvr-v3 /app/resolver-test

# OCI labels for flowctl component discovery
LABEL io.flowctl.component.type="processor"
LABEL io.flowctl.component.api-version="v1"
LABEL io.flowctl.component.name="ducklake-ingestion-obsrvr-v3"
LABEL io.flowctl.component.description="DuckLake ingestion with DuckDB catalog (Nix-built, v3)"
LABEL io.flowctl.component.input-events="raw_ledger_service.RawLedgerChunk"
LABEL io.flowctl.component.output-events="bronze.parquet"

# Additional metadata labels
LABEL org.opencontainers.image.source="https://github.com/withObsrvr/ttp-processor-demo"
LABEL org.opencontainers.image.title="DuckLake Ingestion Obsrvr v3 (Nix)"
LABEL org.opencontainers.image.description="DuckDB catalog ingestion with bug fixes, built with Nix for reproducibility"
LABEL org.opencontainers.image.vendor="Obsrvr"
LABEL org.opencontainers.image.version="3.0.0"
LABEL build.system="nix"
LABEL build.reproducible="true"

# Health check port
EXPOSE 8082

# Run as non-root user
USER nonroot:nonroot

# Set working directory
WORKDIR /app

# Entrypoint
ENTRYPOINT ["/app/ducklake-ingestion-obsrvr-v3"]

# ============================================================================
# Build Instructions
# ============================================================================
#
# Step 1: Vendor dependencies (required for Nix build)
#   cd go && GOWORK=off go mod vendor && cd ..
#
# Step 2: Build Docker image
#   docker build -f Dockerfile.nix -t ducklake-ingestion-obsrvr-v2:nix .
#
# Step 3: Run container
#   docker run -v $(pwd)/config:/app/config ducklake-ingestion-obsrvr-v2:nix -config /app/config/testnet.yaml
#
# ============================================================================
# Comparison: Traditional vs Nix Builds
# ============================================================================
#
# Traditional Dockerfile:
#   - Uses go mod download (network dependency during build)
#   - Different dependency versions between dev/CI/prod possible
#   - Build results depend on network/cache state
#   - "Works on my machine" problems
#
# Nix Dockerfile:
#   - Uses vendored dependencies (offline builds)
#   - Identical dependency resolution via flake.lock
#   - Deterministic builds (same inputs = same outputs)
#   - Single source of truth (flake.nix)
#   - Same environment in dev shell, CI, and production
#
# ============================================================================
