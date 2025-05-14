# Stellar Live Source - flowctl Integration

This document describes how to run the Stellar Live Source service with flowctl control plane integration.

## Prerequisites

1. Make sure the flowctl control plane server is running:
   ```bash
   cd /path/to/flowctl
   make build
   ./bin/flowctl server --port 8080
   ```

2. Build the Stellar Live Source service:
   ```bash
   cd /path/to/stellar-live-source
   make build
   ```

## Running with flowctl Integration

To enable flowctl integration, set the `ENABLE_FLOWCTL` environment variable to `true` when starting the service:

```bash
# Required environment variables
export RPC_ENDPOINT=https://your-soroban-rpc-endpoint

# flowctl-related environment variables
export ENABLE_FLOWCTL=true
export FLOWCTL_ENDPOINT=localhost:8080  # Address of the flowctl control plane
export FLOWCTL_HEARTBEAT_INTERVAL=10s   # Optional: Heartbeat interval (default: 10s)
export STELLAR_NETWORK=testnet          # Optional: Network name (default: testnet)

# Start the service
./stellar_live_source_server
```

## Configuration Options

The flowctl integration can be configured using the following environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `ENABLE_FLOWCTL` | Enable flowctl integration | `false` |
| `FLOWCTL_ENDPOINT` | Address of the flowctl control plane | `localhost:8080` |
| `FLOWCTL_HEARTBEAT_INTERVAL` | Interval for sending heartbeats | `10s` |
| `STELLAR_NETWORK` | Stellar network name | `testnet` |
| `HEALTH_PORT` | Port for the health check server | `8088` |

## Verifying the Integration

After starting the service with flowctl integration enabled, you can verify that it registered correctly by running:

```bash
cd /path/to/flowctl
./bin/flowctl list
```

This should show your stellar-live-source service in the list of registered services.

## Integration with Pipeline

To include the service in a pipeline, create a pipeline YAML file:

```yaml
apiVersion: flowctl/v1
kind: Pipeline
metadata:
  name: stellar-token-pipeline
spec:
  description: Pipeline for processing Stellar token transfers
  sources:
    - id: stellar-live-source
      type: source
      image: ghcr.io/withobsrvr/stellar-live-source:latest
      config:
        network: testnet
  # Add processors and sinks as needed
```

Apply the pipeline configuration:

```bash
cd /path/to/flowctl
./bin/flowctl apply -f your-pipeline.yaml
```

## How It Works

The integration consists of:

1. **FlowctlController**: A component that manages the connection to the flowctl control plane
2. **Registration**: The service registers itself with its capabilities (output event types, etc.)
3. **Heartbeats**: Regular heartbeats are sent to the control plane with service metrics
4. **Graceful Shutdown**: The controller is stopped when the service shuts down

The integration is optional and won't prevent the service from running if the control plane is unavailable.

## Health Check Server

The service includes a health check server that exposes a `/health` endpoint on the configured health port (default: 8088). This endpoint provides detailed metrics about the service's operation and is used by the flowctl control plane to monitor the service's health.

## Metrics Reporting

The following metrics are reported to the flowctl control plane:

- `success_count`: Number of successful operations
- `error_count`: Number of errors
- `total_processed`: Total number of events processed
- `total_bytes_processed`: Total bytes processed
- `last_successful_sequence`: Last successfully processed sequence number