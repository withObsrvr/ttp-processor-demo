.PHONY: all build build-backfill-balance-holder build-backfill-token-metadata \
        build-all build-offline run run-example clean test test-race \
        test-coverage deps vendor fmt vet lint install \
        docker-build docker-buildx docker-push docker-run docker-shell docker-clean \
        ci-build release help

# ---- Variables --------------------------------------------------------------

# Repo root resolved via git (this service depends on stellar-live-source-datalake/go
# at the repo root via a go.mod replace directive — Docker context MUST be repo root)
REPO_ROOT   := $(shell git rev-parse --show-toplevel 2>/dev/null || echo "$(CURDIR)/../..")
SERVICE_DIR := obsrvr-lake/stellar-postgres-ingester
GO_SRC_DIR  := go

# Binaries
BINARY_NAME      := stellar-postgres-ingester
BACKFILL_BALANCE := backfill-balance-holder
BACKFILL_TOKEN   := backfill-token-metadata

# Docker (org rule: all obsrvr-lake images live under withobsrvr/, never obsrvr-lake/)
DOCKER_ORG       := withobsrvr
DOCKER_IMAGE     := $(DOCKER_ORG)/stellar-postgres-ingester
GIT_SHA          := $(shell git rev-parse --short HEAD 2>/dev/null || echo "dev")
BUILD_TIMESTAMP  := $(shell date -u +%Y%m%d%H%M%S)
BUILD_DATE       := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
# Production tag convention: <git-short-sha>-<YYYYMMDDhhmmss>
VERSION_TAG      := $(GIT_SHA)-$(BUILD_TIMESTAMP)
DOCKER_TAG       ?= latest
DOCKER_PLATFORM  ?= linux/amd64

# Go
GOCMD   := go
GOBUILD := $(GOCMD) build
GOTEST  := $(GOCMD) test
GOCLEAN := $(GOCMD) clean

# Default
all: build

# ---- Build (Go) -------------------------------------------------------------

build:
	@echo "→ building $(BINARY_NAME) (sha: $(GIT_SHA))"
	@mkdir -p bin
	cd $(GO_SRC_DIR) && $(GOBUILD) -o ../bin/$(BINARY_NAME) .
	@echo "✓ $(BINARY_NAME) → bin/$(BINARY_NAME)"

build-backfill-balance-holder:
	@mkdir -p bin
	cd $(GO_SRC_DIR) && $(GOBUILD) -o ../bin/$(BACKFILL_BALANCE) ./cmd/backfill-balance-holder
	@echo "✓ $(BACKFILL_BALANCE) → bin/$(BACKFILL_BALANCE)"

build-backfill-token-metadata:
	@mkdir -p bin
	cd $(GO_SRC_DIR) && $(GOBUILD) -o ../bin/$(BACKFILL_TOKEN) ./cmd/backfill-token-metadata
	@echo "✓ $(BACKFILL_TOKEN) → bin/$(BACKFILL_TOKEN)"

build-all: build build-backfill-balance-holder build-backfill-token-metadata
	@echo "✓ all binaries built into bin/"

build-offline:
	@mkdir -p bin
	cd $(GO_SRC_DIR) && GO111MODULE=on GOPROXY=off $(GOBUILD) -mod=vendor -o ../bin/$(BINARY_NAME) .
	@echo "✓ $(BINARY_NAME) (offline/vendored) → bin/$(BINARY_NAME)"

# ---- Run --------------------------------------------------------------------

run: build
	./bin/$(BINARY_NAME) -config config.yaml

run-example: build
	@if [ ! -f config.yaml ]; then \
		echo "Creating config.yaml from config.yaml.example..."; \
		cp config.yaml.example config.yaml; \
	fi
	./bin/$(BINARY_NAME) -config config.yaml

# ---- Quality ----------------------------------------------------------------

test:
	cd $(GO_SRC_DIR) && $(GOTEST) -v ./...

test-race:
	cd $(GO_SRC_DIR) && $(GOTEST) -race -v ./...

test-coverage:
	cd $(GO_SRC_DIR) && $(GOTEST) -coverprofile=coverage.out -covermode=atomic ./...
	cd $(GO_SRC_DIR) && $(GOCMD) tool cover -func=coverage.out

fmt:
	cd $(GO_SRC_DIR) && $(GOCMD) fmt ./...

vet:
	cd $(GO_SRC_DIR) && $(GOCMD) vet ./...

lint:
	@command -v golangci-lint >/dev/null 2>&1 || { \
	  echo "golangci-lint not installed; install with: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"; \
	  exit 1; }
	cd $(GO_SRC_DIR) && golangci-lint run ./...

deps:
	cd $(GO_SRC_DIR) && $(GOCMD) mod download
	cd $(GO_SRC_DIR) && $(GOCMD) mod tidy

vendor:
	cd $(GO_SRC_DIR) && $(GOCMD) mod tidy && $(GOCMD) mod vendor
	@echo "✓ vendored ($(GO_SRC_DIR)/vendor)"

install:
	cd $(GO_SRC_DIR) && $(GOCMD) install .

clean:
	rm -rf bin/
	rm -f $(GO_SRC_DIR)/coverage.out
	cd $(GO_SRC_DIR) && $(GOCLEAN)

# ---- Docker -----------------------------------------------------------------
#
# IMPORTANT: docker-build runs from REPO ROOT. The Dockerfile COPYs
#   stellar-live-source-datalake/go (workspace dep at repo root) AND
#   obsrvr-lake/stellar-postgres-ingester. Building from this dir won't work.
#
# Tagging: every build produces TWO tags
#   $(DOCKER_IMAGE):$(DOCKER_TAG)     (default "latest", overridable)
#   $(DOCKER_IMAGE):$(VERSION_TAG)    (<git-sha>-<timestamp>, matches prod convention)

docker-build:
	@echo "→ building $(DOCKER_IMAGE)"
	@echo "  context : $(REPO_ROOT)"
	@echo "  tags    : $(DOCKER_TAG), $(VERSION_TAG)"
	cd $(REPO_ROOT) && docker build \
		-f $(SERVICE_DIR)/Dockerfile \
		-t $(DOCKER_IMAGE):$(DOCKER_TAG) \
		-t $(DOCKER_IMAGE):$(VERSION_TAG) \
		--label org.opencontainers.image.version=$(VERSION_TAG) \
		--label org.opencontainers.image.revision=$(GIT_SHA) \
		--label org.opencontainers.image.created=$(BUILD_DATE) \
		--label org.opencontainers.image.source=https://github.com/withObsrvr/ttp-processor-demo \
		.
	@echo "✓ built $(DOCKER_IMAGE):{$(DOCKER_TAG),$(VERSION_TAG)}"

docker-buildx:
	cd $(REPO_ROOT) && docker buildx build \
		--platform $(DOCKER_PLATFORM) \
		-f $(SERVICE_DIR)/Dockerfile \
		-t $(DOCKER_IMAGE):$(DOCKER_TAG) \
		-t $(DOCKER_IMAGE):$(VERSION_TAG) \
		.

docker-push: docker-build
	docker push $(DOCKER_IMAGE):$(DOCKER_TAG)
	docker push $(DOCKER_IMAGE):$(VERSION_TAG)
	@echo "✓ pushed $(DOCKER_IMAGE):{$(DOCKER_TAG),$(VERSION_TAG)}"

docker-run:
	docker run --rm -it \
		-p 8088:8088 -p 50054:50054 \
		-v $(CURDIR)/config.yaml:/app/config.yaml:ro \
		$(DOCKER_IMAGE):$(DOCKER_TAG)

docker-shell:
	docker run --rm -it --entrypoint sh $(DOCKER_IMAGE):$(DOCKER_TAG)

docker-clean:
	-docker rmi $(DOCKER_IMAGE):$(DOCKER_TAG) 2>/dev/null
	-docker rmi $(DOCKER_IMAGE):$(VERSION_TAG) 2>/dev/null

# ---- CI / Release -----------------------------------------------------------

ci-build: deps vet build test

release: docker-build docker-push
	@echo "✓ released $(DOCKER_IMAGE):$(VERSION_TAG)"

# ---- Help -------------------------------------------------------------------

help:
	@echo "stellar-postgres-ingester — Makefile targets"
	@echo ""
	@echo "Build:"
	@echo "  build                          Build main binary → bin/"
	@echo "  build-backfill-balance-holder  Build balance-holder backfill cmd"
	@echo "  build-backfill-token-metadata  Build token-metadata backfill cmd"
	@echo "  build-all                      Build all three binaries"
	@echo "  build-offline                  Build using vendored deps"
	@echo ""
	@echo "Run:"
	@echo "  run                            Build + run with config.yaml"
	@echo "  run-example                    Build + run (auto-create config from example)"
	@echo ""
	@echo "Quality:"
	@echo "  test / test-race / test-coverage"
	@echo "  fmt / vet / lint               (lint requires golangci-lint)"
	@echo "  deps                           Download + tidy dependencies"
	@echo "  vendor                         Vendor dependencies for offline builds"
	@echo "  install                        go install to GOPATH"
	@echo "  clean                          Remove bin/ and coverage artifacts"
	@echo ""
	@echo "Docker (context = repo root, image = $(DOCKER_IMAGE)):"
	@echo "  docker-build                   Build image, tag :$(DOCKER_TAG) and :<sha>-<timestamp>"
	@echo "  docker-buildx                  Multi-platform build via buildx"
	@echo "  docker-push                    Build + push both tags"
	@echo "  docker-run                     Run locally (mounts ./config.yaml)"
	@echo "  docker-shell                   Drop into a shell inside the image"
	@echo "  docker-clean                   Remove local image tags"
	@echo ""
	@echo "CI / Release:"
	@echo "  ci-build                       deps + vet + build + test"
	@echo "  release                        docker-build + docker-push"
	@echo ""
	@echo "Override variables on the command line, e.g.:"
	@echo "  make docker-build DOCKER_TAG=v1.2.3"
	@echo "  make docker-buildx DOCKER_PLATFORM=linux/amd64,linux/arm64"
