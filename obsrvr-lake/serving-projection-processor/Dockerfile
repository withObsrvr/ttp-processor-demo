FROM golang:1.26.1-bookworm AS build
WORKDIR /src
COPY go ./go
RUN cd go && go mod download && go build -o /out/serving-projection-processor .

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=build /out/serving-projection-processor /app/serving-projection-processor
COPY config.yaml.example /app/config.yaml.example
ENTRYPOINT ["/app/serving-projection-processor"]
