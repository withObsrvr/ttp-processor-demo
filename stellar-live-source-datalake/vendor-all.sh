#!/bin/bash
set -e

echo "========================================"
echo "Generating protobuf files and vendoring dependencies"
echo "========================================"

# Disable Go workspace mode
export GOWORK=off
export GO111MODULE=on

# Generate protobuf files
echo "Generating protobuf files..."
mkdir -p go/gen/raw_ledger_service

# Run protoc directly
protoc \
  --proto_path=./protos \
  --go_out=./go/gen \
  --go_opt=paths=source_relative \
  --go_opt=Mraw_ledger_service/raw_ledger_service.proto=github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/gen/raw_ledger_service \
  --go-grpc_out=./go/gen \
  --go-grpc_opt=paths=source_relative \
  --go-grpc_opt=Mraw_ledger_service/raw_ledger_service.proto=github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/gen/raw_ledger_service \
  ./protos/raw_ledger_service/raw_ledger_service.proto
  
echo "✓ Protobuf files generated successfully"

# Vendor dependencies
echo "Updating go.mod with replace directives..."
cd go
echo 'replace github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/gen/raw_ledger_service => ./gen/raw_ledger_service' >> go.mod
echo 'replace github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/server => ./server' >> go.mod

echo "Running go mod tidy..."
GOWORK=off go mod tidy

echo "Running go mod vendor..."
GOWORK=off go mod vendor

echo "✓ Dependencies successfully vendored"
echo "You can now build with: nix build"