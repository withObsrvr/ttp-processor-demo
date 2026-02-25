.PHONY: build clean run test schema

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
BINARY_NAME=stellarbeat-network-ingester

GO_SRC_DIR=./go

all: build

# Build the ingester
build:
	cd $(GO_SRC_DIR) && GOWORK=off $(GOBUILD) -o ../$(BINARY_NAME) .
	@echo "Build completed: $(BINARY_NAME)"

# Clean build artifacts
clean:
	$(GOCLEAN)
	rm -f $(BINARY_NAME)

# Run the ingester
run: build
	./$(BINARY_NAME)

# Run tests
test:
	cd $(GO_SRC_DIR) && $(GOTEST) -v ./...

# Apply schema to stellar_hot database
schema:
	@echo "Apply with: psql -h localhost -p 5434 -U stellar -d stellar_hot -f schema/init_network_topology.sql"
