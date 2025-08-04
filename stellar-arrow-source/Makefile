.PHONY: build test test-integration nix-shell nix-build gen-proto run clean benchmark test-arrow-schemas test-arrow-flight test-data-compatibility

SERVICE_NAME = stellar-arrow-source

# Nix-based development
nix-shell:
	nix develop

nix-build:
	nix build

nix-run:
	nix run

# Go build and test
build:
	cd go && go build -o ../$(SERVICE_NAME) .

gen-proto:
	protoc --go_out=go --go_opt=paths=source_relative \
		--go-grpc_out=go --go-grpc_opt=paths=source_relative \
		protos/*/*.proto

test:
	cd go && go test -v ./...

test-integration: build
	cd go && go test -v -tags=integration ./...

# Arrow-specific tests
test-arrow-schemas:
	cd go && go test -v ./schema -run TestSchemaCompatibility

test-arrow-flight:
	cd go && go test -v ./server -run TestFlightServer

test-data-compatibility:
	./scripts/test_xdr_arrow_conversion.sh

# Performance benchmarks
benchmark:
	cd go && go test -bench=. -benchmem ./...

run: build
	./$(SERVICE_NAME)

clean:
	rm -f $(SERVICE_NAME)
	rm -rf go/gen/

docker-build:
	docker build -t $(SERVICE_NAME):latest .

docker-run:
	docker run -p 8815:8815 -p 8088:8088 $(SERVICE_NAME):latest

# Development helpers
dev-setup:
	cd go && go mod tidy
	cd go && go mod download

lint:
	cd go && golangci-lint run

format:
	cd go && gofmt -w .
	cd go && goimports -w .