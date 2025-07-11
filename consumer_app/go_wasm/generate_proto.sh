#!/bin/bash

# Set directories
PROTO_DIR="$(pwd)/protos"
GEN_DIR="$(pwd)/cmd/consumer_wasm/gen"

# Create output directory
mkdir -p $GEN_DIR

# Clean existing files
rm -rf $GEN_DIR/*

# Change to proto directory
cd $PROTO_DIR

echo "Generating protobuf files from $PROTO_DIR to $GEN_DIR..."

# Generate asset proto
protoc \
    --go_out=$GEN_DIR \
    --go_opt=paths=source_relative \
    --experimental_allow_proto3_optional \
    ingest/asset/asset.proto

# Generate token transfer proto
protoc \
    --go_out=$GEN_DIR \
    --go_opt=paths=source_relative \
    --go_opt=Mingest/asset/asset.proto=github.com/stellar/ttp-processor-demo/consumer_app/go_wasm/cmd/consumer_wasm/gen/ingest/asset \
    --experimental_allow_proto3_optional \
    ingest/processors/token_transfer/token_transfer_event.proto

# Generate event service proto and gRPC
protoc \
    --go_out=$GEN_DIR \
    --go_opt=paths=source_relative \
    --go_opt=Mingest/processors/token_transfer/token_transfer_event.proto=github.com/stellar/ttp-processor-demo/consumer_app/go_wasm/cmd/consumer_wasm/gen/ingest/processors/token_transfer \
    --go-grpc_out=$GEN_DIR \
    --go-grpc_opt=paths=source_relative \
    --experimental_allow_proto3_optional \
    event_service/event_service.proto

echo "Protobuf generation complete!"

# List generated files
echo "Generated files:"
find $GEN_DIR -type f -name "*.pb.go" | sort