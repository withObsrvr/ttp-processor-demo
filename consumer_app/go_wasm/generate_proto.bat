@echo off
echo Generating protobuf code...

protoc -I=. ^
       --proto_path=./protos ^
       --go_out=./cmd/consumer_wasm/gen ^
       --go_opt=paths=source_relative ^
       --go-grpc_out=./cmd/consumer_wasm/gen ^
       --go-grpc_opt=paths=source_relative ^
       --experimental_allow_proto3_optional ^
       event_service/event_service.proto ^
       ingest/processors/token_transfer/token_transfer_event.proto ^
       ingest/asset/asset.proto

if %ERRORLEVEL% EQU 0 (
    echo Protobuf code generation complete!
) else (
    echo Error generating protobuf code.
)

pause 