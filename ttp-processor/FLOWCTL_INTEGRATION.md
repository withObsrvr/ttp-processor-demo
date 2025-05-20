# TTP Processor - flowctl Integration

This document describes how to run the Token Transfer Processor (TTP) service with flowctl control plane integration.

## Prerequisites

1. Make sure the flowctl control plane server is running:
   ```bash
   cd /path/to/flowctl
   make build
   ./bin/flowctl server --port 8080
   ```

2. Build the TTP Processor service:
   ```bash
   cd /path/to/ttp-processor
   make build
   ```

## Running with flowctl Integration

To enable flowctl integration, set the `ENABLE_FLOWCTL` environment variable to `true` when starting the service:

```bash
# Required environment variables
export NETWORK_PASSPHRASE="Public Global Stellar Network ; September 2015"
export SOURCE_SERVICE_ADDRESS=localhost:50052  # Address of the Stellar Live Source

# flowctl-related environment variables
export ENABLE_FLOWCTL=true
export FLOWCTL_ENDPOINT=localhost:8080  # Address of the flowctl control plane
export FLOWCTL_HEARTBEAT_INTERVAL=10s   # Optional: Heartbeat interval (default: 10s)
export STELLAR_NETWORK=testnet          # Optional: Network name (default: testnet)
export HEALTH_PORT=8088                 # Optional: Health check port (default: 8088)

# Start the service
./ttp_processor_server
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

This should show your ttp-processor service in the list of registered services.

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
  processors:
    - id: token-transfer-processor
      type: processor
      image: ghcr.io/withobsrvr/ttp-processor:latest
      config:
        network_passphrase: "Public Global Stellar Network ; September 2015"
  # Add sinks as needed
```

Apply the pipeline configuration:

```bash
cd /path/to/flowctl
./bin/flowctl apply -f your-pipeline.yaml
```

## How It Works

The integration consists of:

1. **FlowctlController**: A component that manages the connection to the flowctl control plane
2. **Registration**: The service registers itself with its capabilities (input/output event types, etc.)
3. **Metrics Tracking**: The service tracks processing metrics like success/error counts and event counts
4. **Health Check Server**: Exposes a health endpoint for monitoring
5. **Heartbeats**: Regular heartbeats are sent to the control plane with service metrics
6. **Graceful Shutdown**: The controller is stopped when the service shuts down

The integration is optional and won't prevent the service from running if the control plane is unavailable.

## Health Check Server

The service includes a health check server that exposes a `/health` endpoint on the configured health port (default: 8088). This endpoint provides detailed metrics about the service's operation and is used by the flowctl control plane to monitor the service's health.

## Metrics Reporting

The following metrics are reported to the flowctl control plane:

- `success_count`: Number of successful ledger processing operations
- `error_count`: Number of errors encountered
- `total_processed`: Total number of ledgers processed
- `total_events_emitted`: Total number of token transfer events emitted
- `last_processed_ledger`: Last successfully processed ledger sequence
- `processing_latency_ms`: Average processing latency in milliseconds