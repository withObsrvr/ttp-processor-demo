# PowerShell script to generate protobuf code

# Check if protoc is available
if (-not (Get-Command protoc -ErrorAction SilentlyContinue)) {
    Write-Error "protoc is not available in your PATH. Please install it first."
    exit 1
}

# Run protoc command with the correct parameters
protoc -I=. `
       --proto_path=./protos `
       --go_out=./cmd/consumer_wasm/gen `
       --go_opt=paths=source_relative `
       --go-grpc_out=./cmd/consumer_wasm/gen `
       --go-grpc_opt=paths=source_relative `
       --experimental_allow_proto3_optional `
       event_service/event_service.proto `
       ingest/processors/token_transfer/token_transfer_event.proto `
       ingest/asset/asset.proto

Write-Host "Protobuf code generation complete!" -ForegroundColor Green 