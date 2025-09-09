#!/bin/bash
set -e

# Generate Go code from proto files
echo "Generating Go code from proto files..."

# Ensure the output directory exists
mkdir -p ../go/protos/contractdata

# Generate the code
protoc \
  --go_out=../go/protos \
  --go_opt=paths=source_relative \
  --go-grpc_out=../go/protos \
  --go-grpc_opt=paths=source_relative \
  contract_data_service.proto

echo "Proto generation complete!"