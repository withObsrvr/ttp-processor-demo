#!/usr/bin/bash
set -e

# This script copies the necessary proto files from the cli_tool to the Minecraft mod
# and prepares them for Java code generation

# Base directories
CLI_TOOL_DIR="../ttp-processor"
PROTOS_DIR="src/main/proto"

# Create the necessary directories
mkdir -p ${PROTOS_DIR}/event_service
mkdir -p ${PROTOS_DIR}/ingest/processors/token_transfer
mkdir -p ${PROTOS_DIR}/ingest/asset

# Copy proto files
echo "Copying proto files..."
cp ${CLI_TOOL_DIR}/protos/event_service/event_service.proto ${PROTOS_DIR}/event_service/
cp ${CLI_TOOL_DIR}/protos/ingest/processors/token_transfer/token_transfer_event.proto ${PROTOS_DIR}/ingest/processors/token_transfer/
cp ${CLI_TOOL_DIR}/protos/ingest/asset/asset.proto ${PROTOS_DIR}/ingest/asset/

# Add Java package options to proto files
echo "Adding Java package options to proto files..."

# Adding Java package option to event_service.proto
sed -i 's|option go_package = "cli_tool/gen/go/event_service";|option go_package = "cli_tool/gen/go/event_service";\noption java_package = "com.stellar.ttpmod.grpc";\noption java_multiple_files = true;|' ${PROTOS_DIR}/event_service/event_service.proto

# Adding Java package option to token_transfer_event.proto
sed -i 's|option go_package = "github.com/stellar/go/ingest/processors/token_transfer";|option go_package = "github.com/stellar/go/ingest/processors/token_transfer";\noption java_package = "com.stellar.ttpmod.grpc";\noption java_multiple_files = true;|' ${PROTOS_DIR}/ingest/processors/token_transfer/token_transfer_event.proto

# Adding Java package option to asset.proto
sed -i 's|option go_package = "github.com/stellar/go/ingest/asset";|option go_package = "github.com/stellar/go/ingest/asset";\noption java_package = "com.stellar.ttpmod.grpc";\noption java_multiple_files = true;|' ${PROTOS_DIR}/ingest/asset/asset.proto

echo "Proto files setup completed. You can now build the Minecraft mod with './gradlew build'" 