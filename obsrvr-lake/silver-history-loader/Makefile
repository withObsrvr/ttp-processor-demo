.PHONY: all build run clean test fmt vet deps docker-build docker-push help

SERVICE_DIR := $(CURDIR)
GO_SRC_DIR := go
BINARY_NAME := silver-history-loader
GIT_SHA := $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)
DOCKER_IMAGE ?= withobsrvr/$(BINARY_NAME)
DOCKER_TAG ?= latest
GOENV := GOWORK=off CGO_ENABLED=1

all: build

build:
	@mkdir -p bin
	cd $(GO_SRC_DIR) && $(GOENV) go build -ldflags "-X main.Version=$(GIT_SHA)" -o ../bin/$(BINARY_NAME) .
	@echo "✓ $(BINARY_NAME) → bin/$(BINARY_NAME)"

run: build
	./bin/$(BINARY_NAME) $(ARGS)

test:
	cd $(GO_SRC_DIR) && $(GOENV) go test ./...

fmt:
	cd $(GO_SRC_DIR) && go fmt ./...

vet:
	cd $(GO_SRC_DIR) && $(GOENV) go vet ./...

deps:
	cd $(GO_SRC_DIR) && $(GOENV) go mod tidy

docker-build:
	docker build --build-arg VERSION=$(GIT_SHA) -t $(DOCKER_IMAGE):$(DOCKER_TAG) .

docker-push: docker-build
	docker push $(DOCKER_IMAGE):$(DOCKER_TAG)

clean:
	rm -rf bin/
	cd $(GO_SRC_DIR) && go clean

help:
	@echo "silver-history-loader — bounded Bronze DuckLake -> Silver DuckLake batch loader"
	@echo "  make run ARGS=\"--network mainnet --start-ledger 3 --end-ledger 1000 --bronze-ducklake-catalog ... --bronze-data-path ... --silver-ducklake-catalog ... --silver-data-path ...\""
