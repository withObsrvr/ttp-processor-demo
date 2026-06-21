FROM golang:1.25-bookworm AS build

WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends build-essential ca-certificates && rm -rf /var/lib/apt/lists/*
COPY go/go.mod go/go.sum ./
RUN GOWORK=off go mod download
COPY go/ ./
ARG VERSION=dev
RUN GOWORK=off CGO_ENABLED=1 go build -ldflags "-X main.Version=${VERSION}" -o /out/silver-current-state-projector .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libstdc++6 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/silver-current-state-projector /usr/local/bin/silver-current-state-projector
ENTRYPOINT ["/usr/local/bin/silver-current-state-projector"]
