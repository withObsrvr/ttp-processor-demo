# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copy go module files
COPY go/go.mod go/go.sum ./go/
COPY ../../../go/gen ../../go/gen/

# Download dependencies
WORKDIR /app/go
RUN go mod download

# Copy source
COPY go/ ./

# Build
RUN go build -o ../contract-events-postgres-consumer main.go

# Runtime stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates postgresql-client

WORKDIR /root/

COPY --from=builder /app/contract-events-postgres-consumer .

ENTRYPOINT ["./contract-events-postgres-consumer"]
CMD ["1000", "0"]
