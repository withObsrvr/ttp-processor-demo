# Nix Integration for Stellar Live Source Datalake

This project includes Nix flake support for development and building. Nix provides a reliable, reproducible build environment that works the same way on any machine.

## Prerequisites

- Install Nix: https://nixos.org/download.html
- Enable flakes (if not already enabled):

Add to `~/.config/nix/nix.conf` or `/etc/nix/nix.conf`:
```
experimental-features = nix-command flakes
```

## Development Environment

To enter a development shell with all required dependencies:

```bash
# Using flakes (recommended)
nix develop

# Using traditional nix-shell (fallback)
nix-shell
```

This will provide a shell with:
- Go compiler and tools
- Protocol Buffers and GRPC tools
- Docker
- Make and other build utilities

## Building

### Build the Binary

```bash
# Using Nix build system
nix build

# The binary will be in ./result/bin/stellar_live_source_datalake
```

### Build a Docker Image

```bash
# Build the Docker image using Nix
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

### Run the Docker Container

```bash
# After loading the image
docker run -p 50052:50052 -p 8088:8088 \
  -v $HOME/.config/gcloud:/.config/gcloud:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS=/application_default_credentials.json \
  -e STORAGE_TYPE=GCS \
  -e BUCKET_NAME=your-bucket-name \
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

For CI pipelines, you can use Nix to ensure consistent builds:

```yaml
# Example GitHub Actions step
- name: Build with Nix
  run: nix build

- name: Build Docker image
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