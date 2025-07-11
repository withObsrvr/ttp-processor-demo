.PHONY: all init gen-proto build-server
.SILENT:

PROTOS_BASE_URL := https://raw.githubusercontent.com/stellar/go/master/protos

all: build-server

# Initialize Go module and install dependencies
init:	
	set -e
	rm -rf go/gen > /dev/null 2>&1
	mkdir -p go/gen > /dev/null 2>&1
	mkdir -p protos/ingest/processors/token_transfer > /dev/null 2>&1
	mkdir -p protos/ingest/asset > /dev/null 2>&1

	# Proto files are already in the repo, no need to download
	echo "✓ Init completed"


gen-proto: init
	# Generate Go code from proto files
	set -e
	protoc \
		--proto_path=./protos \
		--go_out=go/gen \
		--go_opt=Mingest/processors/token_transfer/token_transfer_event.proto=github.com/stellar/go/ingest/processors/token_transfer \
		--go_opt=Mingest/asset/asset.proto=github.com/stellar/go/ingest/asset \
		--go_opt=paths=source_relative \
		--go-grpc_out=go/gen \
		--go-grpc_opt=Mingest/processors/token_transfer/token_transfer_event.proto=github.com/stellar/go/ingest/processors/token_transfer \
		--go-grpc_opt=Mingest/asset/asset.proto=github.com/stellar/go/ingest/asset \
		--go-grpc_opt=paths=source_relative \
		event_service/event_service.proto > /dev/null 2>&1
	echo "✓ Proto generation completed"

build-server: gen-proto
	set -e
	cd go && go build -o ../ttp_grpc_server main.go
	echo "✓ Server build completed"