#!/bin/bash
set -e

echo "Building Stellar Live Source Datalake Docker image"

# Step 1: Build the binary with Nix
echo "Building binary with Nix..."
nix build

# Step 2: Build the Docker image
echo "Building Docker image..."
docker build -t stellar-live-source-datalake:latest .

echo "🎉 Docker image built successfully: stellar-live-source-datalake:latest"
echo ""
echo "You can run it with:"
echo "docker run -p 50052:50052 -p 8088:8088 \\"
echo "  -v \$HOME/.config/gcloud:/root/.config/gcloud:ro \\"
echo "  -e GOOGLE_APPLICATION_CREDENTIALS=/root/.config/gcloud/application_default_credentials.json \\"
echo "  -e STORAGE_TYPE=GCS \\"
echo "  -e BUCKET_NAME=\"obsrvr-stellar-ledger-data-testnet-data/landing/ledgers\" \\"
echo "  -e LEDGERS_PER_FILE=1 \\"
echo "  -e FILES_PER_PARTITION=64000 \\"
echo "  -e ENABLE_FLOWCTL=true \\"
echo "  -e FLOWCTL_ENDPOINT=host.docker.internal:8080 \\"
echo "  stellar-live-source-datalake:latest"