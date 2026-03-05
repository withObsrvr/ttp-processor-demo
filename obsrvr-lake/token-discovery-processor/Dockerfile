# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /build

# Install build dependencies
RUN apk add --no-cache git

# Copy go mod files
COPY go/go.mod go/go.sum* ./

# Download dependencies
RUN go mod download

# Copy source code
COPY go/ ./

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o token-discovery-processor .

# Runtime stage
FROM alpine:3.19

WORKDIR /app

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

# Copy binary from builder
COPY --from=builder /build/token-discovery-processor /app/

# Copy config
COPY config.yaml /local/config.yaml

# Create non-root user
RUN adduser -D -u 1000 appuser
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8096/health || exit 1

# Expose health port
EXPOSE 8096

ENTRYPOINT ["/app/token-discovery-processor"]
CMD ["-config", "/local/config.yaml"]
