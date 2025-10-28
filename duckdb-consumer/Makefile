.PHONY: proto build clean run test

# Generate protobuf code
proto:
	protoc --proto_path=protos \
		--go_out=go \
		--go-grpc_out=go \
		protos/account_balance_service.proto

# Build the binary
build:
	cd go && GOWORK=off go build -o ../bin/duckdb-consumer .

# Clean build artifacts
clean:
	rm -rf bin/duckdb-consumer
	rm -rf go/gen
	rm -f *.duckdb *.duckdb.wal

# Run the consumer (requires account-balance-processor to be running)
run:
	./bin/duckdb-consumer

# Run tests
test:
	cd go && GOWORK=off go test -v ./...

# Full rebuild
rebuild: clean proto build
