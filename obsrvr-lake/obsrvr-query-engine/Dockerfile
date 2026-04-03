FROM golang:1.25-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    git make gcc g++ && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY go/ ./

RUN sed -i '/^toolchain/d' go.mod && \
    CGO_ENABLED=1 go build -o obsrvr-query-engine

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tzdata wget && \
    rm -rf /var/lib/apt/lists/*

RUN groupadd -g 1000 stellar && \
    useradd -u 1000 -g stellar -s /bin/bash -m stellar

WORKDIR /app
COPY --from=builder /build/obsrvr-query-engine .
RUN chown -R stellar:stellar /app

USER stellar

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:8092/health || exit 1

EXPOSE 8092

ENTRYPOINT ["./obsrvr-query-engine"]
CMD ["/local/config.yaml"]
