# Node.js Consumer App - flowctl Integration

This document describes how to run the Node.js Consumer Application with flowctl control plane integration.

## Prerequisites

1. Make sure the flowctl control plane server is running:
   ```bash
   cd /path/to/flowctl
   make build
   ./bin/flowctl server --port 8080
   ```

2. Install the Node.js Consumer App dependencies:
   ```bash
   cd /path/to/consumer_app/node
   npm install
   ```

## Running with flowctl Integration

To enable flowctl integration, set the `ENABLE_FLOWCTL` environment variable to `true` when starting the application:

```bash
# Required environment variables
export TTP_SERVICE_ADDRESS=localhost:50051  # Address of the TTP processor service

# flowctl-related environment variables
export ENABLE_FLOWCTL=true
export FLOWCTL_ENDPOINT=localhost:8080  # Address of the flowctl control plane
export FLOWCTL_HEARTBEAT_INTERVAL=10000  # Optional: Heartbeat interval in ms (default: 10000)
export STELLAR_NETWORK=testnet          # Optional: Network name (default: testnet)
export HEALTH_PORT=8088                 # Optional: Health check port (default: 8088)

# Start the application with a range of ledgers
npm run start -- 1000 2000
# This will process events for ledgers 1000 to 2000
```

For continuous monitoring (no end ledger):

```bash
npm run start -- 1000 0
# This will process events from ledger 1000 onwards
```

## Configuration Options

The flowctl integration can be configured using the following environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `ENABLE_FLOWCTL` | Enable flowctl integration | `false` |
| `FLOWCTL_ENDPOINT` | Address of the flowctl control plane | `localhost:8080` |
| `FLOWCTL_HEARTBEAT_INTERVAL` | Interval for sending heartbeats (ms) | `10000` |
| `STELLAR_NETWORK` | Stellar network name | `testnet` |
| `HEALTH_PORT` | Port for the health check server | `8088` |
| `TTP_SERVICE_ADDRESS` | Address of the TTP processor | `localhost:50051` |

## Verifying the Integration

After starting the application with flowctl integration enabled, you can verify that it registered correctly by running:

```bash
cd /path/to/flowctl
./bin/flowctl list
```

This should show your Node.js consumer application in the list of registered services.

## Integration with Pipeline

To include the consumer in a pipeline, create a pipeline YAML file:

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
  consumers:
    - id: node-consumer
      type: consumer
      image: ghcr.io/withobsrvr/node-consumer:latest
      config:
        start_ledger: 1000  # Optional: Start processing from this ledger
```

Apply the pipeline configuration:

```bash
cd /path/to/flowctl
./bin/flowctl apply -f your-pipeline.yaml
```

## Health and Metrics

The application exposes health and metrics information via the following endpoints:

- **Health Check**: `http://localhost:8088/health` - Overall service health with metrics
- **Metrics**: `http://localhost:8088/metrics` - Detailed metrics information

## Metrics Reporting

The following metrics are reported to the flowctl control plane:

- `success_count`: Number of successfully processed events
- `error_count`: Number of errors encountered
- `total_processed`: Total number of processing operations
- `total_events_received`: Total number of events received
- `last_processed_ledger`: Last successfully processed ledger sequence
- `processing_latency_ms`: Average processing latency in milliseconds
- `uptime_ms`: Service uptime in milliseconds

## How It Works

The integration consists of:

1. **FlowctlClient**: A TypeScript class that manages the connection to the flowctl control plane
2. **Metrics Tracking**: The service tracks processing metrics like success/error counts and event counts
3. **Health Server**: Exposes a health endpoint for monitoring
4. **Heartbeats**: Regular heartbeats are sent to the control plane with service metrics
5. **Graceful Shutdown**: The client is stopped when the application shuts down

The integration is optional and won't prevent the application from running if the control plane is unavailable.

## Development Notes

### Modifying the Integration

If you need to modify the flowctl integration, the relevant files are:

- `src/flowctl-client.ts` - The main flowctl client implementation
- `src/metrics.ts` - Metrics tracking implementation
- `src/health-server.ts` - Health/metrics HTTP server
- `src/index.ts` - Main application with flowctl initialization

### Adding Custom Metrics

To add custom metrics, modify the `metrics.ts` file to include additional metrics fields and update the `toJSON()` method to include them in the reported metrics.

### Testing the Integration

You can test the integration without a running flowctl server by setting `ENABLE_FLOWCTL=true` - the application will create a simulated service ID and continue operation.