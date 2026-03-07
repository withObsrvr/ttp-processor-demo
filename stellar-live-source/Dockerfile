FROM golang:1.24-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git make protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

# Install Go protobuf plugins
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

WORKDIR /build

# Copy proto files
COPY protos/ ./protos/

# Copy go module files
COPY go/go.mod go/go.sum ./
RUN go mod download

# Copy source code
COPY go/ ./

# Generate protobuf code and build binary
RUN mkdir -p gen && \
    protoc \
      --proto_path=protos \
      --go_out=gen \
      --go_opt=paths=source_relative \
      --go_opt=Mraw_ledger_service/raw_ledger_service.proto=github.com/stellar/stellar-live-source/gen/raw_ledger_service \
      --go-grpc_out=gen \
      --go-grpc_opt=paths=source_relative \
      --go-grpc_opt=Mraw_ledger_service/raw_ledger_service.proto=github.com/stellar/stellar-live-source/gen/raw_ledger_service \
      raw_ledger_service/raw_ledger_service.proto && \
    go mod tidy && \
    CGO_ENABLED=0 go build -o stellar-live-source main.go

FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tzdata wget && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -g 1000 stellar && \
    useradd -u 1000 -g stellar -s /bin/bash -m stellar

WORKDIR /app

# Copy binary
COPY --from=builder /build/stellar-live-source .

# Change ownership
RUN chown -R stellar:stellar /app

USER stellar

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:8088/health || exit 1

# Expose gRPC and health ports
EXPOSE 50052 8088

# Run service
ENTRYPOINT ["./stellar-live-source"]
