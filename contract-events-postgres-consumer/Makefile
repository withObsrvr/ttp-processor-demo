.PHONY: build run clean init-db vendor nix-build nix-run docker-build docker-run

BINARY_NAME=contract-events-postgres-consumer
GO_DIR=./go

build:
	@echo "Building PostgreSQL consumer..."
	@cd $(GO_DIR) && go build -o ../$(BINARY_NAME)
	@echo "✓ Build completed: $(BINARY_NAME)"

run: build
	@./$(BINARY_NAME) 1000 1100

init-db:
	@echo "Initializing database schema..."
	@psql -h $(POSTGRES_HOST) -p $(POSTGRES_PORT) -U $(POSTGRES_USER) -d $(POSTGRES_DB) -f config/schema/contract_events.sql
	@echo "✓ Schema initialized"

vendor:
	@echo "Vendoring Go dependencies..."
	@cd $(GO_DIR) && GOWORK=off go mod tidy && GOWORK=off go mod vendor
	@echo "✓ Dependencies vendored"

nix-build: vendor
	@echo "Building with Nix..."
	@nix build
	@echo "✓ Nix build completed"

nix-run:
	@echo "Running with Nix..."
	@nix run

docker-build: vendor
	@echo "Building Docker image with Nix..."
	@nix build .#docker
	@echo "Loading image into Docker..."
	@docker load < result
	@echo "✓ Docker image built: withobsrvr/contract-events-postgres-consumer:latest"

docker-run:
	@echo "Running Docker container..."
	@docker run --rm \
		-e POSTGRES_HOST=host.docker.internal \
		-e POSTGRES_PORT=5433 \
		-e CONTRACT_EVENTS_SERVICE_ADDRESS=host.docker.internal:50053 \
		withobsrvr/contract-events-postgres-consumer:latest \
		1000 1100

clean:
	@rm -f $(BINARY_NAME)
	@rm -rf result
	@rm -rf $(GO_DIR)/vendor
	@echo "✓ Cleaned"
