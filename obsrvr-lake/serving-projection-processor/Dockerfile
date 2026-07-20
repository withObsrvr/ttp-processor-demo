FROM golang:1.26.1-bookworm AS build
WORKDIR /src
COPY go ./go
RUN cd go && go mod download && go build -o /out/serving-projection-processor .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /out/serving-projection-processor /app/serving-projection-processor
COPY config.yaml.example /app/config.yaml.example
ENTRYPOINT ["/app/serving-projection-processor"]
