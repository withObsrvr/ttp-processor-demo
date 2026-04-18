.PHONY: all build build-offline run run-mainnet run-testnet clean test test-race test-coverage \
        deps vendor fmt vet lint install \
        swagger swagger-install swagger-fmt \
        docker-build docker-buildx docker-push docker-run docker-shell docker-clean \
        ci-build release help

# ---- Variables --------------------------------------------------------------

SERVICE_DIR := $(CURDIR)
GO_SRC_DIR  := go
BINARY_NAME := stellar-query-api

# Primary entrypoint is main_silver.go — main_bronze.go.bak is the retired variant.
# Dockerfile CMD defaults to config.silver.yaml; mirror that here.
DEFAULT_CONFIG := config.silver.yaml

DOCKER_ORG       := withobsrvr
DOCKER_IMAGE     := $(DOCKER_ORG)/$(BINARY_NAME)
GIT_SHA          := $(shell git rev-parse --short HEAD 2>/dev/null || echo "dev")
BUILD_TIMESTAMP  := $(shell date -u +%Y%m%d%H%M%S)
BUILD_DATE       := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
VERSION_TAG      := $(GIT_SHA)-$(BUILD_TIMESTAMP)
DOCKER_TAG       ?= latest
DOCKER_PLATFORM  ?= linux/amd64

HEALTH_PORT      := 8092

# DuckDB — CGO required.
GOCMD   := go
GOBUILD := CGO_ENABLED=1 $(GOCMD) build
GOTEST  := CGO_ENABLED=1 $(GOCMD) test
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
	./bin/$(BINARY_NAME) -config $(DEFAULT_CONFIG)

run-testnet: build
	@if [ ! -f config.yaml ]; then echo "config.yaml missing"; exit 1; fi
	./bin/$(BINARY_NAME) -config config.yaml

run-mainnet: build
	@if [ ! -f config-mainnet.yaml ]; then echo "config-mainnet.yaml missing (see config-mainnet.yaml.example)"; exit 1; fi
	./bin/$(BINARY_NAME) -config config-mainnet.yaml

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
	rm -rf $(GO_SRC_DIR)/docs
	cd $(GO_SRC_DIR) && $(GOCLEAN)

# ---- Swagger ----------------------------------------------------------------

swagger-install:
	$(GOCMD) install github.com/swaggo/swag/cmd/swag@v1.16.4

swagger:
	@command -v swag >/dev/null 2>&1 || { \
	  echo "swag not installed; run: make swagger-install"; exit 1; }
	cd $(GO_SRC_DIR) && swag init --parseDependency --parseInternal --parseDepth 1 \
		--generalInfo main_silver.go \
		--output docs \
		--outputTypes go,json,yaml
	@echo "✓ swagger docs → $(GO_SRC_DIR)/docs/"

swagger-fmt:
	cd $(GO_SRC_DIR) && swag fmt

# ---- Docker -----------------------------------------------------------------

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
	docker buildx build --platform $(DOCKER_PLATFORM) \
		-t $(DOCKER_IMAGE):$(DOCKER_TAG) -t $(DOCKER_IMAGE):$(VERSION_TAG) $(SERVICE_DIR)

docker-push: docker-build
	docker push $(DOCKER_IMAGE):$(DOCKER_TAG)
	docker push $(DOCKER_IMAGE):$(VERSION_TAG)
	@echo "✓ pushed $(DOCKER_IMAGE):{$(DOCKER_TAG),$(VERSION_TAG)}"

docker-run:
	docker run --rm -it -p $(HEALTH_PORT):$(HEALTH_PORT) \
		-v $(CURDIR)/$(DEFAULT_CONFIG):/app/$(DEFAULT_CONFIG):ro \
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
	@echo "stellar-query-api — Makefile targets"
	@echo ""
	@echo "NOTE: HTTP API serving hot/cold/serving/index layers. Entrypoint"
	@echo "      is main_silver.go (main_bronze.go.bak is retired)."
	@echo "      Default config: $(DEFAULT_CONFIG) (matches Dockerfile CMD)."
	@echo ""
	@echo "Build (CGO_ENABLED=1 — DuckDB requires it):"
	@echo "  build / build-offline"
	@echo ""
	@echo "Run:"
	@echo "  run                            Build + run with $(DEFAULT_CONFIG)"
	@echo "  run-testnet                    Build + run with config.yaml"
	@echo "  run-mainnet                    Build + run with config-mainnet.yaml"
	@echo ""
	@echo "Swagger (requires swag CLI — install via make swagger-install):"
	@echo "  swagger                        Generate docs → go/docs/"
	@echo "  swagger-fmt                    Format @annotations in-place"
	@echo "  swagger-install                Install swag v1.16.4"
	@echo ""
	@echo "Quality:"
	@echo "  test / test-race / test-coverage / fmt / vet / lint"
	@echo "  deps / vendor / install / clean (also removes go/docs/)"
	@echo ""
	@echo "Docker (image = $(DOCKER_IMAGE)):"
	@echo "  docker-build / docker-buildx / docker-push"
	@echo "  docker-run (HEALTH_PORT=$(HEALTH_PORT), mounts $(DEFAULT_CONFIG))"
	@echo "  docker-shell / docker-clean"
	@echo ""
	@echo "CI / Release: ci-build / release"
	@echo ""
	@echo "Note: go/Makefile has a legacy dev-only variant that builds into go/."
	@echo "      Prefer this root-level Makefile (binary goes to bin/)."
