FROM golang:1.23-alpine AS builder

# Install dependencies
RUN apk add --no-cache make git protoc protobuf-dev

# Set working directory
WORKDIR /app

# Copy source code
COPY . .

# Install Go protobuf plugins
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Generate protobuf code and build
RUN make gen-proto
RUN cd go && GOWORK=off CGO_ENABLED=0 GOOS=linux go build -o ../contract-invocation-processor .

# Production image
FROM alpine:latest

# Install CA certificates for gRPC
RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy binary from builder
COPY --from=builder /app/contract-invocation-processor .

# Expose ports
EXPOSE 50054 8089

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:8089/health || exit 1

# Run the binary
CMD ["./contract-invocation-processor"]