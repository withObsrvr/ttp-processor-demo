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
GO_SRC_DIR=./go
GEN_DIR=$(GO_SRC_DIR)/gen
PROTO_DIR=./protos
GO_PACKAGE_BASE=github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake

all: build-server



build-server: gen-proto
	set -e
	cd $(GO_SRC_DIR) && $(GOBUILD) -o ../$(BINARY_NAME) main.go
	echo "✓ Server build completed: $(BINARY_NAME)"

clean:
	$(GOCLEAN)
	rm -f $(BINARY_NAME)
	rm -rf $(GEN_DIR)
init:
	set -e
	# Ensure Go module exists, create if not

	rm -rf $(GEN_DIR) > /dev/null 2>&1
	mkdir -p $(GEN_DIR) > /dev/null 2>&1
	echo "✓ Init completed for stellar-live-source-datalake"

		
gen-proto: init
	# Generate Go code from proto file
	set -e
	protoc \
		--proto_path=$(PROTO_DIR) \
		--go_out=$(GEN_DIR) \
		--go_opt=paths=source_relative \
		--go_opt=Mraw_ledger_service/raw_ledger_service.proto=$(GO_PACKAGE_BASE)/gen/raw_ledger_service \
		--go-grpc_out=$(GEN_DIR) \
		--go-grpc_opt=paths=source_relative \
		--go-grpc_opt=Mraw_ledger_service/raw_ledger_service.proto=$(GO_PACKAGE_BASE)/gen/raw_ledger_service \
		raw_ledger_service/raw_ledger_service.proto
	# After generation, update the go.mod file to add replace directives
	cd $(GO_SRC_DIR) && echo 'replace github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/gen/raw_ledger_service => ./gen/raw_ledger_service' >> go.mod
	cd $(GO_SRC_DIR) && echo 'replace github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/server => ./server' >> go.mod
	cd $(GO_SRC_DIR) && go mod tidy
	echo "✓ Proto generation completed for stellar-live-source-datalake"
run: build
	./$(BINARY_NAME)

test:
	$(GOTEST) -v ./... 