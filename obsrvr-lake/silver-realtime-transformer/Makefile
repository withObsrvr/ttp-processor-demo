.PHONY: all build build-offline run clean test test-race test-coverage \
        deps vendor fmt vet lint install \
        docker-build docker-buildx docker-push docker-run docker-shell docker-clean \
        ci-build release help

# ---- Variables --------------------------------------------------------------

# Self-contained build (Docker context = SERVICE DIR).
SERVICE_DIR := $(CURDIR)
GO_SRC_DIR  := go

BINARY_NAME := silver-realtime-transformer

# Docker (org rule: all obsrvr-lake images live under withobsrvr/, never obsrvr-lake/)
DOCKER_ORG       := withobsrvr
DOCKER_IMAGE     := $(DOCKER_ORG)/$(BINARY_NAME)
GIT_SHA          := $(shell git rev-parse --short HEAD 2>/dev/null || echo "dev")
BUILD_TIMESTAMP  := $(shell date -u +%Y%m%d%H%M%S)
BUILD_DATE       := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
# Production tag convention: <git-short-sha>-<YYYYMMDDhhmmss>
VERSION_TAG      := $(GIT_SHA)-$(BUILD_TIMESTAMP)
DOCKER_TAG       ?= latest
DOCKER_PLATFORM  ?= linux/amd64

# Health endpoint port (matches Dockerfile EXPOSE + Nomad job)
HEALTH_PORT      := 8094

# Go (DuckDB requires CGO; schema files are embedded via go:embed)
GOCMD   := go
GOBUILD := CGO_ENABLED=1 $(GOCMD) build
GOTEST  := CGO_ENABLED=1 $(GOCMD) test
GOCLEAN := $(GOCMD) clean

# Default
all: build

# ---- Build (Go) -------------------------------------------------------------

build:
	@echo "→ building $(BINARY_NAME) (sha: $(GIT_SHA))"
	@mkdir -p bin
	cd $(GO_SRC_DIR) && $(GOBUILD) -o ../bin/$(BINARY_NAME) .
	@echo "✓ $(BINARY_NAME) → bin/$(BINARY_NAME)"

build-offline:
	@mkdir -p bin
	cd $(GO_SRC_DIR) && GO111MODULE=on GOPROXY=off $(GOBUILD) -mod=vendor -o ../bin/$(BINARY_NAME) .
	@echo "✓ $(BINARY_NAME) (offline/vendored) → bin/$(BINARY_NAME)"

# ---- Run --------------------------------------------------------------------

run: build
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
# Self-contained build: context = service dir. Dockerfile COPYs go/ (schema
# embedded via go:embed) and config.yaml. CGO required for DuckDB.
#
# Tagging: every build produces TWO tags
#   $(DOCKER_IMAGE):$(DOCKER_TAG)     (default "latest", overridable)
#   $(DOCKER_IMAGE):$(VERSION_TAG)    (<git-sha>-<timestamp>, matches prod convention)

docker-build:
	@echo "→ building $(DOCKER_IMAGE)"
	@echo "  context : $(SERVICE_DIR)"
	@echo "  tags    : $(DOCKER_TAG), $(VERSION_TAG)"
	docker build \
		-t $(DOCKER_IMAGE):$(DOCKER_TAG) \
		-t $(DOCKER_IMAGE):$(VERSION_TAG) \
		--label org.opencontainers.image.version=$(VERSION_TAG) \
		--label org.opencontainers.image.revision=$(GIT_SHA) \
		--label org.opencontainers.image.created=$(BUILD_DATE) \
		--label org.opencontainers.image.source=https://github.com/withObsrvr/ttp-processor-demo \
		$(SERVICE_DIR)
	@echo "✓ built $(DOCKER_IMAGE):{$(DOCKER_TAG),$(VERSION_TAG)}"

docker-buildx:
	docker buildx build \
		--platform $(DOCKER_PLATFORM) \
		-t $(DOCKER_IMAGE):$(DOCKER_TAG) \
		-t $(DOCKER_IMAGE):$(VERSION_TAG) \
		$(SERVICE_DIR)

docker-push: docker-build
	docker push $(DOCKER_IMAGE):$(DOCKER_TAG)
	docker push $(DOCKER_IMAGE):$(VERSION_TAG)
	@echo "✓ pushed $(DOCKER_IMAGE):{$(DOCKER_TAG),$(VERSION_TAG)}"

docker-run:
	docker run --rm -it \
		-p $(HEALTH_PORT):$(HEALTH_PORT) \
		-v $(CURDIR)/config.yaml:/app/config.yaml:ro \
		$(DOCKER_IMAGE):$(DOCKER_TAG)

docker-shell:
	docker run --rm -it --entrypoint bash $(DOCKER_IMAGE):$(DOCKER_TAG)

docker-clean:
	-docker rmi $(DOCKER_IMAGE):$(DOCKER_TAG) 2>/dev/null
	-docker rmi $(DOCKER_IMAGE):$(VERSION_TAG) 2>/dev/null

# ---- CI / Release -----------------------------------------------------------

ci-build: deps vet build test

release: docker-build docker-push
	@echo "✓ released $(DOCKER_IMAGE):$(VERSION_TAG)"

# ---- Help -------------------------------------------------------------------

help:
	@echo "silver-realtime-transformer — Makefile targets"
	@echo ""
	@echo "Build (CGO_ENABLED=1 — DuckDB requires it):"
	@echo "  build                          Build binary → bin/"
	@echo "  build-offline                  Build using vendored deps"
	@echo ""
	@echo "Run:"
	@echo "  run                            Build + run with config.yaml"
	@echo ""
	@echo "Quality:"
	@echo "  test / test-race / test-coverage"
	@echo "  fmt / vet / lint               (lint requires golangci-lint)"
	@echo "  deps                           Download + tidy dependencies"
	@echo "  vendor                         Vendor dependencies for offline builds"
	@echo "  install                        go install to GOPATH"
	@echo "  clean                          Remove bin/ and coverage artifacts"
	@echo ""
	@echo "Docker (context = service dir, image = $(DOCKER_IMAGE)):"
	@echo "  docker-build                   Build image, tag :$(DOCKER_TAG) and :<sha>-<timestamp>"
	@echo "  docker-buildx                  Multi-platform build via buildx"
	@echo "  docker-push                    Build + push both tags"
	@echo "  docker-run                     Run locally (mounts ./config.yaml, exposes :$(HEALTH_PORT))"
	@echo "  docker-shell                   Drop into a bash shell inside the image"
	@echo "  docker-clean                   Remove local image tags"
	@echo ""
	@echo "CI / Release:"
	@echo "  ci-build                       deps + vet + build + test"
	@echo "  release                        docker-build + docker-push"
	@echo ""
	@echo "Override variables on the command line, e.g.:"
	@echo "  make docker-build DOCKER_TAG=v1.2.3"
	@echo "  make docker-buildx DOCKER_PLATFORM=linux/amd64,linux/arm64"
