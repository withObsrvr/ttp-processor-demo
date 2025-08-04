#!/bin/bash

# Install protoc-gen-go and protoc-gen-go-grpc
echo "Installing protobuf Go plugins..."

cd go

go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

echo "Protobuf Go plugins installed successfully"
echo "Note: You still need protoc compiler installed separately"