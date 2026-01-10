FROM golang:1.24-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make

WORKDIR /workspace

# Copy workspace dependencies
COPY stellar-live-source-datalake/go ./stellar-live-source-datalake/go

# Copy service code
COPY stellar-postgres-ingester/go ./stellar-postgres-ingester/go

# Build from service directory
WORKDIR /workspace/stellar-postgres-ingester/go
RUN go mod download
RUN go mod tidy
RUN CGO_ENABLED=0 go build -o stellar-postgres-ingester

FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

# Create non-root user
RUN addgroup -g 1000 stellar && \
    adduser -D -u 1000 -G stellar stellar

WORKDIR /app

# Copy binary and config
COPY --from=builder /workspace/stellar-postgres-ingester/go/stellar-postgres-ingester .
COPY stellar-postgres-ingester/config.yaml .

# Change ownership
RUN chown -R stellar:stellar /app

USER stellar

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:8088/health || exit 1

# Expose health endpoint
EXPOSE 8088

# Run service
ENTRYPOINT ["./stellar-postgres-ingester"]
CMD ["-config", "config.yaml"]
