# Build stage
FROM golang:1.22-alpine AS build

# Install dependencies
RUN apk add --no-cache git make protoc protobuf-dev

# Set working directory
WORKDIR /app

# Copy go module files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code and build files
COPY protos/ ./protos/
COPY go/ ./go/
COPY Makefile ./Makefile

# Build the binary
RUN make -f Makefile.ledger_jsonrpc build

# Final stage
FROM alpine:latest

# Install dependencies
RUN apk add --no-cache ca-certificates tzdata

# Set working directory
WORKDIR /app

# Copy binary from build stage
COPY --from=build /app/bin/ledger-jsonrpc-processor .

# Set environment variables
ENV PORT=50053
ENV HEALTH_PORT=8089
ENV RETENTION_WINDOW_DAYS=7
ENV BLOOM_FP_RATE=0.01
ENV ENABLE_JSON_XDR=false

# Expose ports
EXPOSE 50053
EXPOSE 8089

# Run the binary
ENTRYPOINT ["/app/ledger-jsonrpc-processor"]