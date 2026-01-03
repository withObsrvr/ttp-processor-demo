FROM golang:1.24-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git make gcc g++ && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy go module files
COPY go/go.mod go/go.sum ./
RUN go mod download

# Copy source code
COPY go/ ./

# Build binary
RUN CGO_ENABLED=1 go build -o bronze-silver-transformer

FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tzdata wget && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -g 1000 stellar && \
    useradd -u 1000 -g stellar -s /bin/bash -m stellar

WORKDIR /app

# Copy binary and config
COPY --from=builder /build/bronze-silver-transformer .
COPY config.yaml .

# Change ownership
RUN chown -R stellar:stellar /app

USER stellar

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:8093/health || exit 1

# Expose health endpoint
EXPOSE 8093

# Run service
ENTRYPOINT ["./bronze-silver-transformer"]
CMD ["-config", "config.yaml"]
