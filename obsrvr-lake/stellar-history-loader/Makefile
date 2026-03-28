.PHONY: build clean run

BINARY := stellar-history-loader
GO_DIR := go
VERSION := $(shell git rev-parse --short HEAD 2>/dev/null || echo "dev")

build:
	cd $(GO_DIR) && GOWORK=off go build -ldflags "-X main.Version=$(VERSION)" -o ../bin/$(BINARY) .

clean:
	rm -rf bin/

run: build
	./bin/$(BINARY) $(ARGS)

vet:
	cd $(GO_DIR) && GOWORK=off go vet ./...

test:
	cd $(GO_DIR) && GOWORK=off go test ./...
