.PHONY: all build build-offline run clean test test-race test-coverage \
        deps vendor fmt vet lint install \
        docker-build docker-buildx docker-push docker-run docker-shell docker-clean \
        ci-build release help

# ---- Variables --------------------------------------------------------------
#
# NOTE: stellar-history-loader is a CLI BATCH TOOL, not a long-running service.
# `make run` passes ARGS=... through to the binary; there is no HEALTH_PORT.
# Example: make run ARGS="--start 1 --end 1000 --output /tmp/out ..."

SERVICE_DIR := $(CURDIR)
GO_SRC_DIR  := go
BINARY_NAME := stellar-history-loader

DOCKER_ORG       := withobsrvr
DOCKER_IMAGE     := $(DOCKER_ORG)/$(BINARY_NAME)
GIT_SHA          := $(shell git rev-parse --short HEAD 2>/dev/null || echo "dev")
BUILD_TIMESTAMP  := $(shell date -u +%Y%m%d%H%M%S)
BUILD_DATE       := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
VERSION_TAG      := $(GIT_SHA)-$(BUILD_TIMESTAMP)
DOCKER_TAG       ?= latest
DOCKER_PLATFORM  ?= linux/amd64

# DuckDB — CGO required. This module is deliberately outside go.work
# (uses duckdb-go v2 which bundles a newer DuckDB than other services).
# GOWORK=off enforces that.
GOCMD      := go
GOENV      := GOWORK=off CGO_ENABLED=1
GOBUILD    := $(GOENV) $(GOCMD) build
GOTEST     := $(GOENV) $(GOCMD) test
GOCLEAN    := $(GOCMD) clean
LDFLAGS    := -X main.Version=$(GIT_SHA)

all: build

# ---- Build ------------------------------------------------------------------

build:
	@echo "→ building $(BINARY_NAME) (sha: $(GIT_SHA))"
	@mkdir -p bin
	cd $(GO_SRC_DIR) && $(GOBUILD) -ldflags "$(LDFLAGS)" -o ../bin/$(BINARY_NAME) .
	@echo "✓ $(BINARY_NAME) → bin/$(BINARY_NAME)"

build-offline:
	@mkdir -p bin
	cd $(GO_SRC_DIR) && GO111MODULE=on GOPROXY=off $(GOBUILD) -mod=vendor -ldflags "$(LDFLAGS)" -o ../bin/$(BINARY_NAME) .
	@echo "✓ $(BINARY_NAME) (offline/vendored) → bin/$(BINARY_NAME)"

# ---- Run --------------------------------------------------------------------

# Pass arguments via ARGS; e.g.
#   make run ARGS="--start 1 --end 1000 --storage-type FS --output /tmp/out"
run: build
	./bin/$(BINARY_NAME) $(ARGS)

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
	cd $(GO_SRC_DIR) && $(GOENV) $(GOCMD) vet ./...

lint:
	@command -v golangci-lint >/dev/null 2>&1 || { \
	  echo "golangci-lint not installed; install with: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"; \
	  exit 1; }
	cd $(GO_SRC_DIR) && golangci-lint run ./...

deps:
	cd $(GO_SRC_DIR) && $(GOENV) $(GOCMD) mod download
	cd $(GO_SRC_DIR) && $(GOENV) $(GOCMD) mod tidy

vendor:
	cd $(GO_SRC_DIR) && $(GOENV) $(GOCMD) mod tidy && $(GOENV) $(GOCMD) mod vendor
	@echo "✓ vendored ($(GO_SRC_DIR)/vendor)"

install:
	cd $(GO_SRC_DIR) && $(GOENV) $(GOCMD) install .

clean:
	rm -rf bin/
	rm -f $(GO_SRC_DIR)/coverage.out
	cd $(GO_SRC_DIR) && $(GOCLEAN)

# ---- Docker -----------------------------------------------------------------

docker-build:
	@echo "→ building $(DOCKER_IMAGE)"
	@echo "  context : $(SERVICE_DIR)"
	@echo "  tags    : $(DOCKER_TAG), $(VERSION_TAG)"
	docker build \
		-t $(DOCKER_IMAGE):$(DOCKER_TAG) \
		-t $(DOCKER_IMAGE):$(VERSION_TAG) \
		--build-arg VERSION=$(GIT_SHA) \
		--label org.opencontainers.image.version=$(VERSION_TAG) \
		--label org.opencontainers.image.revision=$(GIT_SHA) \
		--label org.opencontainers.image.created=$(BUILD_DATE) \
		--label org.opencontainers.image.source=https://github.com/withObsrvr/ttp-processor-demo \
		$(SERVICE_DIR)
	@echo "✓ built $(DOCKER_IMAGE):{$(DOCKER_TAG),$(VERSION_TAG)}"

docker-buildx:
	docker buildx build --platform $(DOCKER_PLATFORM) \
		--build-arg VERSION=$(GIT_SHA) \
		-t $(DOCKER_IMAGE):$(DOCKER_TAG) -t $(DOCKER_IMAGE):$(VERSION_TAG) $(SERVICE_DIR)

docker-push: docker-build
	docker push $(DOCKER_IMAGE):$(DOCKER_TAG)
	docker push $(DOCKER_IMAGE):$(VERSION_TAG)
	@echo "✓ pushed $(DOCKER_IMAGE):{$(DOCKER_TAG),$(VERSION_TAG)}"

# Batch-tool docker-run: pass ARGS through, e.g.
#   make docker-run ARGS="--start 1 --end 1000 --output /out ..."
docker-run:
	docker run --rm -it $(DOCKER_IMAGE):$(DOCKER_TAG) $(ARGS)

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
	@echo "stellar-history-loader — Makefile targets"
	@echo ""
	@echo "NOTE: CLI BATCH TOOL (not a long-running service). Pass flags via ARGS:"
	@echo "  make run ARGS=\"--start 1 --end 1000 --output /tmp/out --storage-type FS\""
	@echo ""
	@echo "Build (CGO_ENABLED=1 for DuckDB; GOWORK=off — outside go.work):"
	@echo "  build / build-offline                  Version embedded via -ldflags"
	@echo ""
	@echo "Run (binary takes CLI flags, not a config file):"
	@echo "  run ARGS=\"...\"                         Pass flags through"
	@echo ""
	@echo "Quality:"
	@echo "  test / test-race / test-coverage / fmt / vet / lint"
	@echo "  deps / vendor / install / clean"
	@echo ""
	@echo "Docker (image = $(DOCKER_IMAGE)):"
	@echo "  docker-build / docker-buildx / docker-push"
	@echo "  docker-run ARGS=\"...\"                   Pass flags through"
	@echo "  docker-shell / docker-clean"
	@echo ""
	@echo "CI / Release: ci-build / release"
