# Build stage
FROM golang:1.21-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make

# Set working directory
WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -o postgresql-consumer .

# Runtime stage
FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

# Create non-root user
RUN adduser -D -u 1000 consumer

# Copy binary from builder
COPY --from=builder /build/postgresql-consumer /usr/local/bin/
COPY --from=builder /build/schema.sql /etc/postgresql-consumer/

# Set ownership
RUN chown -R consumer:consumer /etc/postgresql-consumer

# Switch to non-root user
USER consumer

# Expose metrics port
EXPOSE 9090

# Set entrypoint
ENTRYPOINT ["postgresql-consumer"]