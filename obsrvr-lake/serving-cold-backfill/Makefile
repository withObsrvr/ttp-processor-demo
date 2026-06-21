.PHONY: all build run test fmt vet clean docker-build docker-push help

GO_SRC_DIR := go
BINARY_NAME := serving-cold-backfill
GIT_SHA := $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)
BUILD_TIMESTAMP := $(shell date -u +%Y%m%d%H%M%S)
BUILD_DATE := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
VERSION_TAG := $(GIT_SHA)-$(BUILD_TIMESTAMP)
DOCKER_IMAGE := withobsrvr/$(BINARY_NAME)
DOCKER_TAG ?= latest
GOENV := GOWORK=off CGO_ENABLED=1 GOCACHE=/tmp/ttp-processor-demo-go-cache

all: build

build:
	@mkdir -p bin
	cd $(GO_SRC_DIR) && $(GOENV) go build -ldflags "-X main.Version=$(GIT_SHA)" -o ../bin/$(BINARY_NAME) .
	@echo "$(BINARY_NAME) -> bin/$(BINARY_NAME)"

run: build
	./bin/$(BINARY_NAME) $(ARGS)

test:
	cd $(GO_SRC_DIR) && $(GOENV) go test ./...

fmt:
	cd $(GO_SRC_DIR) && $(GOENV) go fmt ./...

vet:
	cd $(GO_SRC_DIR) && $(GOENV) go vet ./...

clean:
	rm -rf bin/
	cd $(GO_SRC_DIR) && $(GOENV) go clean

docker-build:
	docker build \
		--build-arg VERSION=$(GIT_SHA) \
		--label org.opencontainers.image.version=$(VERSION_TAG) \
		--label org.opencontainers.image.revision=$(GIT_SHA) \
		--label org.opencontainers.image.created=$(BUILD_DATE) \
		--label org.opencontainers.image.source=https://github.com/withObsrvr/ttp-processor-demo \
		-t $(DOCKER_IMAGE):$(DOCKER_TAG) \
		-t $(DOCKER_IMAGE):$(VERSION_TAG) \
		.

docker-push: docker-build
	docker push $(DOCKER_IMAGE):$(DOCKER_TAG)
	docker push $(DOCKER_IMAGE):$(VERSION_TAG)
	@echo "pushed $(DOCKER_IMAGE):$(DOCKER_TAG) and $(DOCKER_IMAGE):$(VERSION_TAG)"

help:
	@echo "serving-cold-backfill flowctl-compatible skeleton"
	@echo "  make run ARGS=\"--network mainnet --start-ledger 3 --end-ledger 100002 --bronze-ducklake-catalog ... --bronze-data-path ... --silver-ducklake-catalog ... --silver-data-path ... --target-postgres ... --manifest-path /tmp/serving-backfill.jsonl\""
