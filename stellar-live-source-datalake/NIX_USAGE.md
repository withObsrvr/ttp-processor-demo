# Nix Flake Integration for Stellar Live Source Datalake

This project uses Nix flakes for development and building. Nix provides a reliable, reproducible build environment that works the same way on any machine.

## Prerequisites

- Install Nix: https://nixos.org/download.html
- Enable flakes (required):

Add to `~/.config/nix/nix.conf` or `/etc/nix/nix.conf`:
```
experimental-features = nix-command flakes
```

## Development Environment

To enter a development shell with all required dependencies:

```bash
# Using flakes 
nix develop
```

This will provide a shell with:
- Go compiler and tools (Go 1.22)
- Protocol Buffers and GRPC tools
- Docker
- Make and other build utilities

The development shell will automatically vendor dependencies if needed.

## Building with Nix Flakes

### Build the Binary

```bash
# Build just the binary
nix build

# The binary will be in ./result/bin/stellar_live_source_datalake
```

### Build a Docker Image

```bash
# Build the Docker image
nix build .#docker

# Load the image into Docker
docker load < result
```

## Running

### Run the Binary Directly

```bash
# Run using nix run
nix run

# Or run the built binary
./result/bin/stellar_live_source_datalake
```

### Running the Docker Container

```bash
# After loading the image
docker run -p 50052:50052 -p 8088:8088 \
  -v $HOME/.config/gcloud:/root/.config/gcloud:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS=/root/.config/gcloud/application_default_credentials.json \
  -e STORAGE_TYPE=GCS \
  -e BUCKET_NAME="obsrvr-stellar-ledger-data-testnet-data/landing/ledgers" \
  -e LEDGERS_PER_FILE=1 \
  -e FILES_PER_PARTITION=64000 \
  -e ENABLE_FLOWCTL=true \
  -e FLOWCTL_ENDPOINT=host.docker.internal:8080 \
  stellar-live-source-datalake:latest
```

For Linux hosts, use one of these approaches:

```bash
# Option 1: Use the host IP address
docker run -p 50052:50052 -p 8088:8088 \
  -v $HOME/.config/gcloud:/root/.config/gcloud:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS=/root/.config/gcloud/application_default_credentials.json \
  -e STORAGE_TYPE=GCS \
  -e BUCKET_NAME="obsrvr-stellar-ledger-data-testnet-data/landing/ledgers" \
  -e LEDGERS_PER_FILE=1 \
  -e FILES_PER_PARTITION=64000 \
  -e ENABLE_FLOWCTL=true \
  -e FLOWCTL_ENDPOINT=172.17.0.1:8080 \
  stellar-live-source-datalake:latest

# Option 2: Use host networking (simpler)
docker run --network=host \
  -v $HOME/.config/gcloud:/root/.config/gcloud:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS=/root/.config/gcloud/application_default_credentials.json \
  -e STORAGE_TYPE=GCS \
  -e BUCKET_NAME="obsrvr-stellar-ledger-data-testnet-data/landing/ledgers" \
  -e LEDGERS_PER_FILE=1 \
  -e FILES_PER_PARTITION=64000 \
  -e ENABLE_FLOWCTL=true \
  -e FLOWCTL_ENDPOINT=localhost:8080 \
  stellar-live-source-datalake:latest
```

## Customizing the Build

Edit the `flake.nix` file to modify:
- Go version
- Dependencies
- Docker image configuration
- Development shell tools

## CI Integration

For CI pipelines, you can use Nix flakes to ensure consistent builds:

```yaml
# Example GitHub Actions step
- name: Install Nix
  uses: cachix/install-nix-action@v22
  with:
    extra_nix_config: |
      experimental-features = nix-command flakes
    
- name: Build with Nix flakes
  run: nix build

- name: Build Docker image with Nix flakes
  run: nix build .#docker
```

## Troubleshooting

### Updating Dependencies

If you need to update the flake inputs:

```bash
nix flake update
```

### Cleaning Up

To clean up Nix store and remove unused packages:

```bash
nix-collect-garbage -d
```

### Debug Build Issues

If you encounter build issues, you can get more detailed information:

```bash
nix build --show-trace
```

### Non-Flake Alternative

For environments where flakes aren't available, you can still use traditional Nix with the provided `shell.nix` file:

```bash
nix-shell
```