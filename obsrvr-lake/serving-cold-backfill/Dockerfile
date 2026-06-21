FROM golang:1.25-bookworm AS build

WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends build-essential ca-certificates && rm -rf /var/lib/apt/lists/*
COPY go/go.mod go/go.sum ./
RUN GOWORK=off go mod download
COPY go/ ./
ARG VERSION=dev
RUN GOWORK=off CGO_ENABLED=1 go build -ldflags "-X main.Version=${VERSION}" -o /out/serving-cold-backfill .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libstdc++6 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/serving-cold-backfill /usr/local/bin/serving-cold-backfill
ENTRYPOINT ["/usr/local/bin/serving-cold-backfill"]
