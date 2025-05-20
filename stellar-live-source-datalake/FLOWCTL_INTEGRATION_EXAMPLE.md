# Flowctl Integration Example

This example demonstrates how to integrate a service with the flowctl control plane. This is a simplified implementation that shows the key concepts without requiring the actual flowctl proto dependencies.

## Running the Example

```bash
cd /path/to/stellar-live-source-datalake
go run flowctl_example.go
```

This will:
1. Register a service with the flowctl control plane
2. Send heartbeats every 10 seconds
3. Log all interactions for educational purposes

## How to Integrate with Your Service

To integrate your service with the flowctl control plane:

1. **Service Registration**:
   - Register your service with the control plane on startup
   - Provide information about capabilities (input/output event types)
   - Define health endpoint and monitoring details
   - Example shown in `server/flowctl.go`

2. **Periodic Heartbeats**:
   - Send heartbeats to the control plane at regular intervals
   - Include health status and metrics
   - Allows the control plane to detect service failures

3. **Graceful Shutdown**:
   - Stop heartbeats and close connections on service shutdown

## Integration with Other Microservices

For ttp-processor:
1. Implement a similar controller that specifies `"PROCESSOR"` as the ServiceType
2. Include both input and output event types
3. Register with the control plane on startup

For consumer_app:
1. Implement a controller that specifies `"SINK"` as the ServiceType
2. Include input event types
3. Register with the control plane on startup

## Environment Variables

Configure the integration using these environment variables:

- `FLOWCTL_ENDPOINT`: The control plane endpoint (default: http://localhost:8080)
- `FLOWCTL_HEARTBEAT_INTERVAL`: Interval for sending heartbeats (default: 10s)
- `ENABLE_FLOWCTL`: Enable flowctl integration (set to "true" to enable)
- `STELLAR_NETWORK`: The Stellar network to connect to (default: testnet)

## Notes on Implementation

The example uses a simplified HTTP-based API instead of the gRPC API for demonstration purposes. In a production environment, you would use the gRPC API defined in the flowctl proto files.

To use the actual gRPC API:
1. Import the flowctl proto package
2. Use the ControlPlaneClient to register and send heartbeats
3. Handle reconnections and failures appropriately