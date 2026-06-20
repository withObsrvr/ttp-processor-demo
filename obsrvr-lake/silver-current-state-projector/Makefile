.PHONY: all build run test fmt vet clean help

GO_SRC_DIR := go
BINARY_NAME := silver-current-state-projector
GIT_SHA := $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)
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

help:
	@echo "silver-current-state-projector flowctl-compatible skeleton"
	@echo "  make run ARGS=\"--network mainnet --start-ledger 3 --end-ledger 100002 --silver-ducklake-catalog ... --silver-data-path ... --manifest-path /tmp/silver-current.jsonl\""
