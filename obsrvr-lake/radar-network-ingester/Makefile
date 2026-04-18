.PHONY: all build build-offline run clean test test-race test-coverage \
        deps vendor fmt vet lint install schema \
        docker-build docker-buildx docker-push docker-run docker-shell docker-clean \
        ci-build release help

# ---- Variables --------------------------------------------------------------
#
# Dev-stage service — no Dockerfile exists yet, so Docker targets will fail
# until one is added. Keep the Makefile ready so that adding a Dockerfile
# immediately wires up docker-build/push.

SERVICE_DIR := $(CURDIR)
GO_SRC_DIR  := go
BINARY_NAME := radar-network-ingester

DOCKER_ORG       := withobsrvr
DOCKER_IMAGE     := $(DOCKER_ORG)/$(BINARY_NAME)
GIT_SHA          := $(shell git rev-parse --short HEAD 2>/dev/null || echo "dev")
BUILD_TIMESTAMP  := $(shell date -u +%Y%m%d%H%M%S)
BUILD_DATE       := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
VERSION_TAG      := $(GIT_SHA)-$(BUILD_TIMESTAMP)
DOCKER_TAG       ?= latest
DOCKER_PLATFORM  ?= linux/amd64

# This module is outside go.work; GOWORK=off enforces isolation.
GOCMD   := go
GOENV   := GOWORK=off
GOBUILD := $(GOENV) $(GOCMD) build
GOTEST  := $(GOENV) $(GOCMD) test
GOCLEAN := $(GOCMD) clean

all: build

# ---- Build ------------------------------------------------------------------

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
	./bin/$(BINARY_NAME)

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
	rm -f $(BINARY_NAME)
	rm -f $(GO_SRC_DIR)/coverage.out
	cd $(GO_SRC_DIR) && $(GOCLEAN)

# ---- Schema ----------------------------------------------------------------

# Print the psql command to apply the schema file (do not auto-run — PG host
# varies per env).
schema:
	@echo "Apply network-topology schema with:"
	@echo "  psql -h <host> -p <port> -U <user> -d stellar_hot -f schema/init_network_topology.sql"

# ---- Docker -----------------------------------------------------------------

docker-build:
	@if [ ! -f "$(SERVICE_DIR)/Dockerfile" ]; then \
	  echo "✗ no Dockerfile in $(SERVICE_DIR) — add one before running docker-build"; \
	  exit 1; \
	fi
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
	docker buildx build --platform $(DOCKER_PLATFORM) \
		-t $(DOCKER_IMAGE):$(DOCKER_TAG) -t $(DOCKER_IMAGE):$(VERSION_TAG) $(SERVICE_DIR)

docker-push: docker-build
	docker push $(DOCKER_IMAGE):$(DOCKER_TAG)
	docker push $(DOCKER_IMAGE):$(VERSION_TAG)
	@echo "✓ pushed $(DOCKER_IMAGE):{$(DOCKER_TAG),$(VERSION_TAG)}"

docker-run:
	docker run --rm -it $(DOCKER_IMAGE):$(DOCKER_TAG)

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
	@echo "radar-network-ingester — Makefile targets"
	@echo ""
	@echo "NOTE: dev-stage service (no Dockerfile yet — docker-build fails until added)."
	@echo ""
	@echo "Build (GOWORK=off — module outside workspace):"
	@echo "  build / build-offline"
	@echo ""
	@echo "Run:"
	@echo "  run                            Build + run"
	@echo ""
	@echo "Schema:"
	@echo "  schema                         Print psql command for init_network_topology.sql"
	@echo ""
	@echo "Quality:"
	@echo "  test / test-race / test-coverage / fmt / vet / lint"
	@echo "  deps / vendor / install / clean"
	@echo ""
	@echo "Docker (image = $(DOCKER_IMAGE); requires Dockerfile):"
	@echo "  docker-build / docker-buildx / docker-push"
	@echo "  docker-run / docker-shell / docker-clean"
	@echo ""
	@echo "CI / Release: ci-build / release"
