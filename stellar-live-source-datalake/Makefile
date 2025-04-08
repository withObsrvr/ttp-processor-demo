.PHONY: build clean generate-proto

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
BINARY_NAME=stellar_live_source_datalake

# Proto parameters
PROTOC=protoc
PROTO_DIR=protos
GEN_DIR=gen

all: generate-proto build

build:
	$(GOBUILD) -o $(BINARY_NAME) main.go

clean:
	$(GOCLEAN)
	rm -f $(BINARY_NAME)
	rm -rf $(GEN_DIR)

generate-proto:
	mkdir -p $(GEN_DIR)
	$(PROTOC) --go_out=$(GEN_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR) --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/raw_ledger_service/raw_ledger_service.proto

run: build
	./$(BINARY_NAME)

test:
	$(GOTEST) -v ./... 