# Flow Control Plane Integration Guide for Ledger-to-JSON-RPC Processor

This document describes how to integrate the Ledger-to-JSON-RPC processor with the Flow control plane for deployment, monitoring, and management.

## Overview

The Flow control plane provides the following capabilities:
- Service discovery and registration
- Health monitoring and metrics aggregation
- Deployment and lifecycle management
- Configuration management

## Registration

The Ledger-to-JSON-RPC processor automatically registers with the Flow control plane when `ENABLE_FLOWCTL=true`.

During registration, it provides the following information:
- Service type: `PROCESSOR`
- Input event types: `raw_ledger_service.RawLedger`
- Output event types: `ledger_jsonrpc_service.LedgerJsonRpcEvent`
- Health endpoint: `http://localhost:{HEALTH_PORT}/health`
- Metadata:
  - Network: (from `STELLAR_NETWORK` environment variable)
  - Processor type: `ledger_jsonrpc`

## Heartbeats and Metrics

The processor sends heartbeats to the Flow control plane every 10 seconds (configurable via `FLOWCTL_HEARTBEAT_INTERVAL`). Each heartbeat includes the following metrics:

- `success_count`: Number of successfully processed ledgers
- `error_count`: Number of errors encountered
- `total_processed`: Total number of ledgers processed
- `total_events_emitted`: Total number of JSON-RPC events emitted
- `last_processed_ledger`: Sequence number of the last processed ledger
- `processing_latency_ms`: Processing latency in milliseconds

## Flow Bindings

To define the Ledger-to-JSON-RPC processor in a Flow pipeline, use the following YAML:

```yaml
bindings:
  inlet:
    name: ledger_envelopes
    type: avro#ledger_close
  outlet:
    name: jsonrpc_results
    type: json
```

## Configuration

The following environment variables control Flow control plane integration:

| Variable | Description | Default |
|----------|-------------|---------|
| `ENABLE_FLOWCTL` | Enable integration with flowctl | false |
| `FLOWCTL_ENDPOINT` | The endpoint for the Flow control plane | localhost:8080 |
| `FLOWCTL_HEARTBEAT_INTERVAL` | Interval for sending heartbeats | 10s |

## Example Flow Pipeline

Here's an example Flow pipeline configuration that includes the Ledger-to-JSON-RPC processor:

```yaml
name: stellar-ledger-jsonrpc
version: 1.0.0
description: Stellar ledger to JSON-RPC conversion pipeline

processors:
  - name: raw-ledger-source
    image: obsrvr/stellar-live-source-datalake:latest
    config:
      NETWORK: testnet
      HISTORY_ARCHIVE_URLS: https://history.stellar.org/prd/core-testnet/core_testnet_001
      START_LEDGER: 10000
    bindings:
      outlets:
        - name: raw_ledgers
          type: proto#raw_ledger
  
  - name: ledger-jsonrpc
    image: obsrvr/ledger-jsonrpc:1.0
    config:
      NETWORK_PASSPHRASE: "Test SDF Network ; September 2015"
      RETENTION_WINDOW_DAYS: 7
      ENABLE_JSON_XDR: true
      ENABLE_FLOWCTL: true
    bindings:
      inlets:
        - name: ledger_envelopes
          type: proto#raw_ledger
          source: raw-ledger-source.raw_ledgers
      outlets:
        - name: jsonrpc_results
          type: json
  
  - name: json-rpc-proxy
    image: obsrvr/json-rpc-proxy:latest
    config:
      AUTH_REQUIRED: false
      RATE_LIMIT: 100
    bindings:
      inlets:
        - name: jsonrpc_responses
          type: json
          source: ledger-jsonrpc.jsonrpc_results
```

## Monitoring in the Flow Dashboard

Once registered, the Ledger-to-JSON-RPC processor will appear in the Flow dashboard with:

1. Basic service information (name, ID, type)
2. Health status (healthy/unhealthy)
3. Metrics (graphed over time)
4. Configuration
5. Logs (if log collection is enabled)

## Troubleshooting

If the processor fails to register with the Flow control plane:

1. Check the logs for any errors during registration
2. Verify that the Flow control plane is running and accessible
3. Check network connectivity to the Flow control plane endpoint
4. Verify that the service has the necessary permissions to register

The processor will continue to function even if Flow control plane integration fails, but monitoring and management capabilities will be limited.