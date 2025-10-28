.PHONY: proto build clean run test

# Generate protobuf code
proto:
	protoc --proto_path=protos \
		--go_out=go \
		--go-grpc_out=go \
		protos/account_balance_service.proto \
		protos/raw_ledger_service/raw_ledger_service.proto

# Build the binary
build:
	cd go && GOWORK=off go build -o ../bin/account-balance-processor .

# Clean build artifacts
clean:
	rm -rf bin/account-balance-processor
	rm -rf go/gen

# Run the server (requires stellar-live-source-datalake to be running)
run:
	./bin/account-balance-processor

# Run tests
test:
	cd go && GOWORK=off go test -v ./...

# Full rebuild
rebuild: clean proto build
