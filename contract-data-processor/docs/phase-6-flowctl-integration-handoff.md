# Phase 6: Flowctl Integration - Developer Handoff Document

## Overview

This document summarizes the flowctl integration implementation for the contract data processor. Flowctl serves as the control plane for service registration, monitoring, and coordination across the distributed system.

## What Was Implemented

### 1. Flowctl Controller (`go/server/flowctl_integration.go`)

A complete flowctl integration controller that:
- Connects to the flowctl control plane via gRPC
- Registers the service with comprehensive metadata
- Sends periodic heartbeats with processing metrics
- Reports service health and performance statistics
- Handles graceful shutdown with status updates

**Key Features:**
- Thread-safe metrics collection and reporting
- Automatic reconnection handling
- Configurable heartbeat intervals
- Service status detection based on error rates

### 2. Hybrid Server Integration

Updated `hybrid_server.go` to:
- Create and manage the flowctl controller
- Start flowctl integration during server startup
- Pass the controller to the processing coordinator
- Stop flowctl cleanly during shutdown

```go
// Start flowctl integration
if err := s.flowctl.Start(s.ctx); err != nil {
    s.logger.Error().Err(err).Msg("Failed to start flowctl integration")
    // Don't fail startup if flowctl is unavailable
}
```

### 3. Processing Coordinator Updates

Modified `processing_coordinator.go` to:
- Accept a flowctl controller reference
- Report metrics after each ledger processing
- Update Prometheus metrics alongside flowctl metrics
- Report errors to flowctl for status tracking

```go
// Report to flowctl if enabled
if pc.flowctl != nil && pc.flowctl.IsEnabled() {
    pc.flowctl.UpdateProcessingMetrics(metrics)
}
```

### 4. Prometheus Metrics Export

Created `prometheus_metrics.go` with comprehensive metrics:
- Contract processing metrics (processed, skipped, batches, bytes)
- Processing duration histogram with exponential buckets
- Flowctl integration metrics (registration status, heartbeats)
- Session and connection tracking
- Arrow Flight streaming metrics

### 5. Configuration Updates

Extended configuration to support flowctl:
```go
type Config struct {
    // Flowctl integration
    FlowctlEnabled           bool
    FlowctlEndpoint          string
    FlowctlHeartbeatInterval time.Duration
    ServiceName              string
    ServiceVersion           string
}
```

Environment variables:
- `FLOWCTL_ENABLED`: Enable/disable integration
- `FLOWCTL_ENDPOINT`: Control plane address
- `FLOWCTL_HEARTBEAT_INTERVAL`: Heartbeat frequency
- `SERVICE_NAME`: Service identifier
- `SERVICE_VERSION`: Version string

### 6. PostgreSQL Consumer Integration

Updated the PostgreSQL consumer to:
- Include flowctl configuration options
- Support the same environment variables
- Enable monitoring of consumer services

### 7. Test Script

Created `test-flowctl-integration.sh` that:
- Verifies processor health
- Checks flowctl connectivity
- Lists registered services
- Monitors heartbeat activity
- Provides troubleshooting guidance

## Architecture Integration

```
┌─────────────────────┐     ┌──────────────────┐
│  Contract Data      │────▶│     Flowctl      │
│    Processor        │     │  Control Plane   │
│                     │     │                  │
│ ┌─────────────────┐ │     │ ┌──────────────┐ │
│ │ Flowctl         │◀┼─────┼▶│ Service      │ │
│ │ Controller      │ │     │ │ Registry     │ │
│ └─────────────────┘ │     │ └──────────────┘ │
│                     │     │                  │
│ ┌─────────────────┐ │     │ ┌──────────────┐ │
│ │ Processing      │ │     │ │ Metrics      │ │
│ │ Coordinator     │ │     │ │ Aggregator   │ │
│ └─────────────────┘ │     │ └──────────────┘ │
└─────────────────────┘     └──────────────────┘
           │                          ▲
           │                          │
           └──────Heartbeats──────────┘
              (metrics, status)
```

## Metrics Reported to Flowctl

The following metrics are sent with each heartbeat:

1. **Processing Metrics:**
   - `contracts_processed`: Total contract data entries processed
   - `entries_skipped`: Entries filtered out
   - `batches_created`: Arrow batches created
   - `bytes_processed`: Total data volume

2. **Performance Metrics:**
   - `processing_rate`: Current processing rate
   - `contracts_per_second`: Average throughput
   - `bytes_per_second`: Data throughput

3. **Status Metrics:**
   - `current_ledger`: Latest ledger processed
   - `error_count`: Total processing errors
   - `error_rate`: Calculated error percentage

## Service Registration Details

The service registers with flowctl providing:

- **Service Type**: `PROCESSOR`
- **Input Event Types**: 
  - `raw_ledger_service.RawLedgerChunk`
  - `stellar.LedgerCloseMeta`
- **Output Event Types**:
  - `contract_data.ArrowBatch`
  - `stellar.ContractDataEntry`
- **Health Endpoint**: HTTP endpoint for health checks
- **Metadata**: Configuration details, addresses, and filter information

## Prometheus Metrics

Available at `http://localhost:8089/metrics`:

```
# Contract processing
contract_data_contracts_processed_total
contract_data_entries_skipped_total
contract_data_batches_created_total
contract_data_bytes_processed_total
contract_data_processing_errors_total
contract_data_processing_duration_seconds
contract_data_current_ledger

# Flowctl integration
contract_data_flowctl_registered
contract_data_flowctl_heartbeats_sent_total
contract_data_flowctl_heartbeat_errors_total

# Sessions and connections
contract_data_active_sessions
contract_data_flight_clients_connected
contract_data_flight_records_streamed_total
```

## Testing the Integration

1. **Start flowctl control plane:**
   ```bash
   # Assuming flowctl is running on localhost:8080
   flowctl serve
   ```

2. **Start the processor with flowctl enabled:**
   ```bash
   export FLOWCTL_ENABLED=true
   export FLOWCTL_ENDPOINT=localhost:8080
   ./contract-data-processor
   ```

3. **Run the test script:**
   ```bash
   ./scripts/test-flowctl-integration.sh
   ```

4. **Monitor metrics:**
   ```bash
   # Prometheus metrics
   curl http://localhost:8089/metrics | grep contract_data_

   # Health check
   curl http://localhost:8089/health
   ```

## Error Handling

The flowctl integration is designed to be resilient:

1. **Connection Failures**: Service continues operating if flowctl is unavailable
2. **Registration Failures**: Logged but don't prevent service startup
3. **Heartbeat Failures**: Logged and counted but don't affect processing
4. **Graceful Degradation**: Service functions normally without flowctl

## Production Considerations

1. **High Availability**: Flowctl should run in HA mode for production
2. **Network Partitions**: Service continues processing during network issues
3. **Metric Retention**: Configure flowctl metric retention policies
4. **Alert Rules**: Set up alerts based on reported metrics
5. **Service Discovery**: Use flowctl for dynamic service discovery

## Configuration Example

```yaml
# Full configuration with flowctl enabled
flowctl:
  enabled: true
  endpoint: "flowctl.service.consul:8080"
  heartbeat_interval: "10s"
  service_name: "contract-data-processor"
  service_version: "1.0.0"

processor:
  grpc_address: ":8815"
  flight_address: ":8816"
  health_port: 8089
  network_passphrase: "Test SDF Network ; September 2015"
```

## Next Steps

With flowctl integration complete, the contract data processor now has:

1. ✅ Hybrid architecture (gRPC control + Arrow Flight data)
2. ✅ High-performance data processing with stellar/go
3. ✅ PostgreSQL consumer with bulk inserts
4. ✅ Service registration and monitoring
5. ✅ Comprehensive metrics and health checks
6. ✅ Production-ready error handling

The service is now fully integrated with the control plane and ready for deployment in a distributed environment.

## Troubleshooting

Common issues and solutions:

1. **Service not appearing in flowctl:**
   - Check `FLOWCTL_ENABLED=true`
   - Verify flowctl is running and accessible
   - Check logs for registration errors

2. **No heartbeats being sent:**
   - Verify the service registered successfully
   - Check network connectivity to flowctl
   - Look for heartbeat errors in logs

3. **Metrics not updating:**
   - Ensure data is being processed
   - Check the processing coordinator is updating metrics
   - Verify Prometheus endpoint is accessible

4. **High error rates reported:**
   - Check data source connectivity
   - Verify stellar/go processor compatibility
   - Review processing logs for specific errors