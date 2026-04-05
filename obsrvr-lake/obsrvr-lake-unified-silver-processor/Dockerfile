# Build from REPO ROOT: docker build -f obsrvr-lake/obsrvr-lake-unified-silver-processor/Dockerfile .
FROM golang:1.25-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    git make gcc g++ && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy the unified processor dependency (for proto gen package)
COPY obsrvr-lake/obsrvr-lake-unified-processor/go /deps/obsrvr-lake-unified-processor/go

# Copy source
COPY obsrvr-lake/obsrvr-lake-unified-silver-processor/go/ ./

# Fix replace directive, download + build
RUN sed -i 's|../../obsrvr-lake-unified-processor/go|/deps/obsrvr-lake-unified-processor/go|' go.mod && \
    go mod download && \
    CGO_ENABLED=1 go build -o obsrvr-lake-unified-silver-processor

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tzdata wget && \
    rm -rf /var/lib/apt/lists/*

RUN groupadd -g 1000 stellar && \
    useradd -u 1000 -g stellar -s /bin/bash -m stellar

WORKDIR /app
COPY --from=builder /build/obsrvr-lake-unified-silver-processor .
RUN chown -R stellar:stellar /app

USER stellar

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:8099/health || exit 1

EXPOSE 8099

ENTRYPOINT ["./obsrvr-lake-unified-silver-processor"]
CMD ["-config", "/local/config.yaml"]
