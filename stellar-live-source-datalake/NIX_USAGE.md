# Building with Nix

This project uses Nix flakes for reproducible builds. Nix provides a declarative and reliable way to build the application and its dependencies.

## Prerequisites

1. Install Nix:
   ```bash
   curl -L https://nixos.org/nix/install | sh
   ```

2. Enable flakes by adding to `~/.config/nix/nix.conf` or `/etc/nix/nix.conf`:
   ```
   experimental-features = nix-command flakes
   ```

## Using the Makefile

The Makefile provides convenient targets for Nix-based builds:

| Command | Description |
|---------|-------------|
| `make nix-build` | Build binary with Nix |
| `make nix-docker` | Build Docker image with Nix |
| `make nix-docker-load` | Load the Nix-built Docker image |
| `make docker-build` | Build Docker image from Nix-built binary |
| `make docker-run` | Run the Docker container |
| `make nix-shell` | Enter Nix development shell |
| `make nix-run` | Run the application with Nix |
| `make vendor` | Vendor Go dependencies for offline builds |
| `make ci-build` | Run full CI build process |

## Development Environment

To enter a development shell with all required dependencies:

```bash
# Using the Makefile
make nix-shell

# Or directly with Nix
nix develop
```

This provides a shell with:
- Go compiler and tools
- Protocol Buffers and gRPC tools
- Docker CLI
- Make and other build utilities

The development shell automatically vendors Go dependencies on first use to improve build reliability.

## Building the Application

### Build the Binary

```bash
# Using the Makefile
make nix-build

# Or directly with Nix
nix build

# The binary will be in ./result/bin/stellar_live_source_datalake
```

### Run the Application

```bash
# Using the Makefile
make nix-run

# Or directly with Nix
nix run

# Or run the built binary directly
./result/bin/stellar_live_source_datalake
```

## Docker Image

### Option 1: Build with Nix + Docker (Recommended)

```bash
# Use the Makefile for a 2-step process:
# 1. Build binary with Nix
# 2. Build Docker image with the binary
make docker-build
```

### Option 2: Pure Nix Docker Image

```bash
# Build a Docker image entirely with Nix
make nix-docker

# Load the image into Docker
make nix-docker-load
```

## Running the Docker Container

```bash
# Using the Makefile (with defaults)
make docker-run

# Or with custom parameters
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

## Handling Network Connectivity Issues

If you encounter network connectivity issues with Go dependencies:

1. Vendor dependencies when online:
   ```bash
   make vendor
   ```

2. Then use the offline build mode:
   ```bash
   make build-server-offline
   ```

## Go Workspace Mode Issues

This project is configured to disable Go workspace mode (`GOWORK=off`) for all operations because:

1. It resolves issues with missing modules in workspaces
2. It ensures consistent builds regardless of workspace configuration
3. It allows vendoring dependencies without workspace conflicts

If you see errors like:

```
go: 'go mod vendor' cannot be run in workspace mode
```

The Makefile targets automatically handle this by setting `GOWORK=off` for all Go commands. If you're running Go commands directly, you can:

```bash
# For any Go command
GOWORK=off go <command>

# Example: Vendor dependencies
GOWORK=off go mod vendor
```

## CI Integration

For CI pipelines, use the `ci-build` target in your workflow:

```yaml
# Example GitHub Actions workflow
name: Build and Test

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Install Nix
        uses: cachix/install-nix-action@v22
        with:
          extra_nix_config: |
            experimental-features = nix-command flakes
      
      - name: Build with Nix
        run: make ci-build
```

## Troubleshooting

### Common Issues

1. **Missing flakes support**:
   ```
   error: git tree '/path/to/project' is dirty
   ```
   Solution: Enable flakes in Nix configuration

2. **Go dependency failures**:
   ```
   go: downloading ... connection refused
   ```
   Solution: 
   ```bash
   make vendor
   ```
   Then rebuild using `make build-server-offline`

3. **Nix build errors**:
   ```
   error: attribute 'xxx' missing
   ```
   Solution: Check for syntax errors in flake.nix

### Advanced Options

#### Updating Dependencies

Update flake inputs:
```bash
nix flake update
```

#### Cleaning Up

Clean up Nix store:
```bash
nix-collect-garbage -d
```

#### Debugging Build Issues

Get detailed build logs:
```bash
nix build --show-trace
```