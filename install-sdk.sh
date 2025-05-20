#!/bin/bash
set -e

# Define the source and destination
SRC_DIR="/tmp/flowctl-sdk"
DEST_DIR="/home/tmosleyiii/projects/obsrvr/flowctl-sdk"

# Create the destination directory if it doesn't exist
mkdir -p "$DEST_DIR"

# Copy all files
echo "Copying Flowctl Processor SDK to $DEST_DIR..."
cp -r "$SRC_DIR"/* "$DEST_DIR"/

# Ensure the package script is executable
chmod +x "$DEST_DIR"/package.sh

echo "SDK installed successfully at $DEST_DIR"
echo ""
echo "Next steps:"
echo "1. Create a GitHub repository for the SDK"
echo "2. Push the code to the repository"
echo "3. Use the SDK in your processor projects"
echo ""
echo "To run the example processor:"
echo "  cd $DEST_DIR"
echo "  make run-example"