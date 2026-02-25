.PHONY: build clean gen-proto run test

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
BINARY_NAME=radar-network-source

# Proto parameters
PROTOC=protoc
GO_SRC_DIR=./go
GEN_DIR=$(GO_SRC_DIR)/gen
PROTO_DIR=./protos
GO_PACKAGE_BASE=github.com/withObsrvr/ttp-processor-demo/obsrvr-lake/radar-network-source

all: build

# Build the server
build: gen-proto
	cd $(GO_SRC_DIR) && GOWORK=off $(GOBUILD) -o ../$(BINARY_NAME) .
	@echo "Build completed: $(BINARY_NAME)"

# Clean build artifacts
clean:
	$(GOCLEAN)
	rm -f $(BINARY_NAME)
	rm -rf $(GEN_DIR)

# Initialize build environment
init:
	rm -rf $(GEN_DIR) > /dev/null 2>&1 || true
	mkdir -p $(GEN_DIR) > /dev/null 2>&1

# Generate protobuf code
gen-proto: init
	$(PROTOC) \
		--proto_path=$(PROTO_DIR) \
		--go_out=$(GEN_DIR) \
		--go_opt=paths=source_relative \
		--go_opt=Mnetwork_topology_service/network_topology_service.proto=$(GO_PACKAGE_BASE)/gen/network_topology_service \
		--go-grpc_out=$(GEN_DIR) \
		--go-grpc_opt=paths=source_relative \
		--go-grpc_opt=Mnetwork_topology_service/network_topology_service.proto=$(GO_PACKAGE_BASE)/gen/network_topology_service \
		network_topology_service/network_topology_service.proto
	@echo "Proto generation completed"

# Run the server
run: build
	./$(BINARY_NAME)

# Run tests
test:
	cd $(GO_SRC_DIR) && $(GOTEST) -v ./...
