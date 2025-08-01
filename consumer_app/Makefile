.PHONY: all init gen-proto build-node
.SILENT:

PROTOS_BASE_URL := https://raw.githubusercontent.com/stellar/go/master/protos

# install protoc
# brew install protobuf
# Install protoc plugins for language specific code generation
# this is using node, the example project installed the grpc/proto plugins
# locally using the package.json already.

all: build-node

init:
	set -e
	rm -rf node/node_modules > /dev/null 2>&1
	rm -rf node/gen > /dev/null 2>&1
	mkdir -p node/gen > /dev/null 2>&1
	rm -rf protos/event_service > /dev/null 2>&1
	mkdir -p protos/event_service > /dev/null 2>&1
	rm -rf protos/ingest/processors/token_transfer > /dev/null 2>&1
	mkdir -p protos/ingest/processors/token_transfer > /dev/null 2>&1
	rm -rf protos/ingest/asset > /dev/null 2>&1
	mkdir -p protos/ingest/asset > /dev/null 2>&1

	cp ../ttp-processor/protos/ingest/processors/token_transfer/token_transfer_event.proto protos/ingest/processors/token_transfer/token_transfer_event.proto
	cp ../ttp-processor/protos/ingest/asset/asset.proto protos/ingest/asset/asset.proto
	cp ../ttp-processor/protos/event_service/event_service.proto protos/event_service/event_service.proto
	cd ./node; npm install > /dev/null
	echo "✓ Init completed"

gen-proto: init
	# Generate Node code from proto files
	set -e
	protoc \
		--plugin=protoc-gen-ts=./node/node_modules/.bin/protoc-gen-ts \
		--proto_path=./protos \
		--ts_out=grpc_js:./node/gen \
		--ts_opt=esModuleInterop=true \
		--experimental_allow_proto3_optional \
		event_service/event_service.proto \
		ingest/processors/token_transfer/token_transfer_event.proto \
		ingest/asset/asset.proto > /dev/null
	echo "✓ Proto generation completed"

build-node: gen-proto
	set -e
	cd node && npm run build > /dev/null 2>&1
	echo "✓ Node build completed"