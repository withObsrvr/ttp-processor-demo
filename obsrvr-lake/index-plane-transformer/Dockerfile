FROM golang:1.24-bookworm AS builder

# Install build dependencies for C/C++ compilation
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    make \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy go module files
COPY go/go.mod go/go.sum ./
RUN go mod download

# Copy source code
COPY go/*.go ./

# Build binary with CGO enabled (required for DuckDB)
RUN CGO_ENABLED=1 go build -o index-plane-transformer .

FROM debian:bookworm-slim

# Install runtime dependencies including C++ standard library
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tzdata \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /root/

# Copy binary from builder
COPY --from=builder /build/index-plane-transformer .

# Copy configuration template
COPY config.yaml /local/config.yaml

# Expose health check port
EXPOSE 8096

# Run the service
ENTRYPOINT ["./index-plane-transformer"]
CMD ["-config", "/local/config.yaml"]
