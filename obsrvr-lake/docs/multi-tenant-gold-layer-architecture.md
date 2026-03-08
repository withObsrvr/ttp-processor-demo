# Multi-Tenant Gold Layer Streaming Architecture

> Design Document for extending obsrvr-lake with tenant-specific gold layers and custom sink pipelines.

## Executive Summary

This document describes the architecture for adding a gRPC streaming layer to obsrvr-lake that enables tenant-specific gold layers and custom sink pipelines built on top of the shared bronze/silver layers.

**Problem**: The current obsrvr-lake architecture provides shared bronze and silver layers via batch processing and a query API. There is no mechanism for tenants to:
- Define custom transformation logic (gold layers)
- Stream processed data to their own infrastructure
- Subscribe to real-time events with custom filtering

**Solution**: Add a gRPC streaming fan-out service (`silver-event-streamer`) that bridges the database-coordinated silver layer to tenant-specific pipelines orchestrated by flowctl.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Components](#components)
3. [Proto Definitions](#proto-definitions)
4. [Tenant Pipeline Examples](#tenant-pipeline-examples)
5. [Security Considerations](#security-considerations)
6. [Tenant Self-Service API](#tenant-self-service-api)
7. [Integration Points](#integration-points)
8. [Deployment Architecture](#deployment-architecture)
9. [Implementation Phases](#implementation-phases)
10. [Verification](#verification)
11. [Open Questions](#open-questions)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SHARED LAYERS (existing)                            │
│                                                                             │
│  stellar-live-source-datalake                                               │
│           │                                                                 │
│           ▼ gRPC (RawLedgerService)                                         │
│  stellar-postgres-ingester → PostgreSQL stellar_hot (Bronze)                │
│           │                                                                 │
│           ▼ polling                                                         │
│  silver-realtime-transformer → PostgreSQL silver_hot (Silver)               │
│           │                                                                 │
│           ▼ flush                                                           │
│  postgres-ducklake-flusher / silver-cold-flusher → DuckLake (Cold)          │
│                                                                             │
└────────────────────────────────────┬────────────────────────────────────────┘
                                     │
                    ┌────────────────▼────────────────┐
                    │      silver-event-streamer      │
                    │      (NEW gRPC Service)         │
                    │                                 │
                    │  • Subscribes to silver_hot     │
                    │  • Tenant subscription registry │
                    │  • gRPC streaming fan-out       │
                    │  • Backpressure management      │
                    └────────────────┬────────────────┘
                                     │
            ┌────────────────────────┼────────────────────────┐
            │ gRPC                   │ gRPC                   │ gRPC
            ▼                        ▼                        ▼
  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
  │ Tenant A Pipeline │    │ Tenant B Pipeline │    │ Tenant C Pipeline │
  │ (flowctl)         │    │ (flowctl)         │    │ (flowctl)         │
  │                   │    │                   │    │                   │
  │ ┌───────────────┐ │    │ ┌───────────────┐ │    │ ┌───────────────┐ │
  │ │Gold Transform │ │    │ │Gold Transform │ │    │ │Gold Transform │ │
  │ │(USDC only)    │ │    │ │(large txns)   │ │    │ │(contract X)   │ │
  │ └───────┬───────┘ │    │ └───────┬───────┘ │    │ └───────┬───────┘ │
  │         ▼         │    │         ▼         │    │         ▼         │
  │ ┌───────────────┐ │    │ ┌───────────────┐ │    │ ┌───────────────┐ │
  │ │ S3 Sink       │ │    │ │ Postgres Sink │ │    │ │ Webhook Sink  │ │
  │ │ (their bucket)│ │    │ │ (their DB)    │ │    │ │ (their API)   │ │
  │ └───────────────┘ │    │ └───────────────┘ │    │ └───────────────┘ │
  └──────────────────┘    └──────────────────┘    └──────────────────┘
```

### Data Flow

1. **Shared Bronze/Silver**: Existing obsrvr-lake services ingest and transform Stellar data into PostgreSQL hot buffers
2. **Fan-out Point**: `silver-event-streamer` subscribes to silver layer changes and streams to registered tenants
3. **Tenant Pipelines**: Each tenant runs an isolated flowctl pipeline with custom gold transformations
4. **Custom Sinks**: Tenant pipelines write to tenant-owned infrastructure (S3, databases, webhooks, etc.)

---

## Components

### 1. silver-event-streamer (New Service)

**Purpose**: Bridge between database-coordinated silver layer and gRPC streaming for tenants.

**Location**: `obsrvr-lake/silver-event-streamer/`

**Responsibilities**:
- Subscribe to silver_hot changes (via PostgreSQL LISTEN/NOTIFY or logical replication)
- Maintain tenant subscription registry
- Fan-out events to subscribed tenant pipelines via gRPC streams
- Handle backpressure per tenant
- Track delivery acknowledgments
- Enforce tenant rate limits and quotas

**Data Source Options**:

| Method | Latency | Complexity | Reliability |
|--------|---------|------------|-------------|
| PostgreSQL NOTIFY | ~10ms | Low | At-most-once |
| Logical Replication (pgoutput) | ~10ms | Medium | At-least-once |
| Polling with cursor | 100ms-1s | Low | At-least-once |

**Recommendation**: Start with NOTIFY for simplicity, migrate to logical replication if delivery guarantees become critical.

### 2. Silver Source Adapter

**Purpose**: Connects flowctl pipelines to the silver-event-streamer.

**Location**: `obsrvr-lake/adapters/silver-source-adapter/`

This adapter implements flowctl's `LedgerSource` gRPC interface while consuming from `SilverEventStreamer`:

```go
// Implements flowctl source.LedgerSource
type SilverSourceAdapter struct {
    streamerClient silver.SilverEventStreamerClient
    tenantID       string
    eventTypes     []string
    filter         *silver.SilverEventFilter
}

func (a *SilverSourceAdapter) StreamRawLedgers(
    req *source.SourceRequest,
    stream source.LedgerSource_StreamRawLedgersServer,
) error {
    // Subscribe to silver streamer
    subReq := &silver.SubscribeRequest{
        TenantId:   a.tenantID,
        EventTypes: a.eventTypes,
        Filter:     a.filter,
    }

    silverStream, err := a.streamerClient.Subscribe(ctx, subReq)
    if err != nil {
        return err
    }

    // Forward events as flowctl Events
    for {
        event, err := silverStream.Recv()
        if err != nil {
            return err
        }

        // Convert to flowctl Event format
        flowctlEvent := &flowctl.Event{
            EventType:     event.EventType,
            Payload:       event.PayloadJson,
            SchemaVersion: 1,
            Timestamp:     event.LedgerCloseTime,
        }

        if err := stream.Send(flowctlEvent); err != nil {
            return err
        }
    }
}
```

### 3. Common Gold Processors (Reusable)

Build a library of reusable gold-layer processors:

| Processor | Purpose | Input | Output |
|-----------|---------|-------|--------|
| `amount-filter` | Filter by amount range | Any with amount | Same |
| `asset-filter` | Filter by asset code/issuer | TokenTransfer | TokenTransfer |
| `contract-filter` | Filter by contract ID | ContractEvent | ContractEvent |
| `address-filter` | Filter by account address | Any with addresses | Same |
| `time-window-aggregator` | Aggregate over time windows | Any | Aggregate |
| `dedup` | Deduplicate by key | Any | Same |
| `enricher-coingecko` | Add USD prices | TokenTransfer | EnrichedTransfer |
| `enricher-account-names` | Add known account labels | Any with addresses | Same |

### 4. Common Sink Adapters

| Sink | Destination | Config |
|------|-------------|--------|
| `s3-sink` | AWS S3 / S3-compatible | bucket, prefix, format (json/parquet) |
| `gcs-sink` | Google Cloud Storage | bucket, prefix, format |
| `postgres-sink` | PostgreSQL | connection string, table, upsert mode |
| `webhook-sink` | HTTP POST | url, headers, batch size |
| `nats-sink` | NATS JetStream | url, stream, subject |
| `kafka-sink` | Kafka | brokers, topic |
| `bigquery-sink` | BigQuery | project, dataset, table |

---

## Proto Definitions

### silver_streamer.proto

```protobuf
syntax = "proto3";

package obsrvr.silver;

import "google/protobuf/timestamp.proto";

option go_package = "github.com/withObsrvr/obsrvr-lake/silver-event-streamer/gen/silver_streamer";

// Silver event streamer service - fan-out from silver layer to tenant pipelines
service SilverEventStreamer {
  // Subscribe to silver layer events with optional filtering
  rpc Subscribe(SubscribeRequest) returns (stream SilverEvent);

  // List available event types
  rpc ListEventTypes(ListEventTypesRequest) returns (ListEventTypesResponse);

  // Health and metrics
  rpc GetStats(GetStatsRequest) returns (StreamerStats);
}

message SubscribeRequest {
  string tenant_id = 1;
  string api_key = 2;  // For authentication

  // Filter by event types (empty = all)
  repeated string event_types = 3;

  // Filter by ledger range (0 = latest)
  uint32 start_ledger = 4;

  // Optional filters
  SilverEventFilter filter = 5;

  // Backpressure settings
  int32 max_inflight = 6;
}

message SilverEventFilter {
  // Filter token transfers by asset
  repeated string asset_codes = 1;

  // Filter by contract addresses
  repeated string contract_ids = 2;

  // Filter by account addresses
  repeated string accounts = 3;

  // Minimum amount (stroops)
  int64 min_amount = 4;
}

message SilverEvent {
  string event_id = 1;
  string event_type = 2;  // "token_transfer", "contract_event", "operation", etc.
  uint32 ledger_sequence = 3;
  google.protobuf.Timestamp ledger_close_time = 4;

  // Event payload as JSON (allows flexible schema evolution)
  bytes payload_json = 5;

  // Or typed payloads for common types
  oneof typed_payload {
    TokenTransferEvent token_transfer = 10;
    ContractEventData contract_event = 11;
    EnrichedOperation operation = 12;
  }
}

message TokenTransferEvent {
  string from_address = 1;
  string to_address = 2;
  string asset_code = 3;
  string asset_issuer = 4;
  string amount = 5;  // String to avoid precision loss
  string transfer_type = 6;  // "payment", "mint", "burn", "clawback"
  string transaction_hash = 7;
}

message ContractEventData {
  string contract_id = 1;
  repeated string topics = 2;
  bytes data = 3;
  string transaction_hash = 4;
}

message EnrichedOperation {
  int64 operation_id = 1;
  string type = 2;
  string source_account = 3;
  bytes details_json = 4;
  string transaction_hash = 5;
}

message ListEventTypesRequest {}

message ListEventTypesResponse {
  repeated EventTypeInfo event_types = 1;
}

message EventTypeInfo {
  string name = 1;
  string description = 2;
  string proto_type = 3;  // Fully qualified protobuf type name
}

message GetStatsRequest {}

message StreamerStats {
  int64 total_events_streamed = 1;
  int32 active_subscriptions = 2;
  uint32 latest_ledger = 3;
  map<string, int64> events_by_type = 4;
  map<string, TenantStats> tenant_stats = 5;
}

message TenantStats {
  string tenant_id = 1;
  int64 events_delivered = 2;
  int64 events_pending = 3;
  uint32 last_acked_ledger = 4;
}
```

---

## Tenant Pipeline Examples

### Example 1: USDC Analytics to S3

**File**: `pipelines/tenant-a-usdc-pipeline.yaml`

```yaml
apiVersion: flowctl/v1
kind: Pipeline
metadata:
  name: tenant-a-usdc-analytics
  labels:
    tenant: tenant-a
    tier: gold
spec:
  driver: docker

  sources:
    - id: silver-source
      image: obsrvr/silver-source-adapter:latest
      env:
        SILVER_STREAMER_ENDPOINT: "silver-event-streamer:50051"
        TENANT_ID: "tenant-a"
        EVENT_TYPES: "token_transfer"
        FILTER_ASSET_CODES: "USDC"
      ports:
        - "50052:50051"
      health_endpoint: "/health"

  processors:
    - id: usdc-enricher
      image: obsrvr/usdc-enricher:latest
      inputs: ["silver-source"]
      input_event_types: ["TokenTransferEvent"]
      output_event_types: ["EnrichedUSDCTransfer"]
      env:
        COINGECKO_API_KEY: "${TENANT_A_COINGECKO_KEY}"

    - id: aggregator
      image: obsrvr/time-window-aggregator:latest
      inputs: ["usdc-enricher"]
      input_event_types: ["EnrichedUSDCTransfer"]
      output_event_types: ["USDCAggregate"]
      env:
        WINDOW_SIZE: "1h"
        GROUP_BY: "from_address,to_address"

  sinks:
    - id: tenant-s3-sink
      image: obsrvr/s3-sink:latest
      inputs: ["aggregator"]
      env:
        AWS_REGION: "us-east-1"
        S3_BUCKET: "${TENANT_A_S3_BUCKET}"
        S3_PREFIX: "gold/usdc-aggregates/"
        AWS_ACCESS_KEY_ID: "${TENANT_A_AWS_KEY}"
        AWS_SECRET_ACCESS_KEY: "${TENANT_A_AWS_SECRET}"
        FILE_FORMAT: "parquet"
        PARTITION_BY: "date"
```

### Example 2: Large Transaction Alerts to Webhook

**File**: `pipelines/tenant-b-whale-alerts.yaml`

```yaml
apiVersion: flowctl/v1
kind: Pipeline
metadata:
  name: tenant-b-whale-alerts
  labels:
    tenant: tenant-b
    tier: gold
spec:
  driver: docker

  sources:
    - id: silver-source
      image: obsrvr/silver-source-adapter:latest
      env:
        SILVER_STREAMER_ENDPOINT: "silver-event-streamer:50051"
        TENANT_ID: "tenant-b"
        EVENT_TYPES: "token_transfer"
        FILTER_MIN_AMOUNT: "10000000000"  # 1000 XLM in stroops

  processors:
    - id: whale-classifier
      image: obsrvr/whale-classifier:latest
      inputs: ["silver-source"]
      env:
        WHALE_THRESHOLD_XLM: "10000"
        WHALE_THRESHOLD_USDC: "100000"

  sinks:
    - id: webhook-sink
      image: obsrvr/webhook-sink:latest
      inputs: ["whale-classifier"]
      env:
        WEBHOOK_URL: "${TENANT_B_WEBHOOK_URL}"
        WEBHOOK_SECRET: "${TENANT_B_WEBHOOK_SECRET}"
        BATCH_SIZE: "1"  # Immediate delivery
        RETRY_COUNT: "3"
```

### Example 3: Contract Events to PostgreSQL

**File**: `pipelines/tenant-c-contract-indexer.yaml`

```yaml
apiVersion: flowctl/v1
kind: Pipeline
metadata:
  name: tenant-c-contract-indexer
  labels:
    tenant: tenant-c
    tier: gold
spec:
  driver: docker

  sources:
    - id: silver-source
      image: obsrvr/silver-source-adapter:latest
      env:
        SILVER_STREAMER_ENDPOINT: "silver-event-streamer:50051"
        TENANT_ID: "tenant-c"
        EVENT_TYPES: "contract_event"
        FILTER_CONTRACT_IDS: "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

  processors:
    - id: event-decoder
      image: obsrvr/soroban-event-decoder:latest
      inputs: ["silver-source"]
      env:
        CONTRACT_ABI_URL: "${TENANT_C_ABI_URL}"

  sinks:
    - id: postgres-sink
      image: obsrvr/postgres-sink:latest
      inputs: ["event-decoder"]
      env:
        POSTGRES_DSN: "${TENANT_C_POSTGRES_DSN}"
        TABLE_NAME: "decoded_events"
        UPSERT_MODE: "true"
        CONFLICT_COLUMNS: "event_id"
```

---

## Security Considerations

### Authentication & Authorization

#### API Key Management

```
┌─────────────────────────────────────────────────────────────────┐
│                    API Key Lifecycle                            │
│                                                                 │
│  Tenant Portal                                                  │
│       │                                                         │
│       ▼                                                         │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────┐ │
│  │ Generate    │───▶│ Store Hash  │───▶│ Return Key (once)   │ │
│  │ API Key     │    │ in DB       │    │ to Tenant           │ │
│  └─────────────┘    └─────────────┘    └─────────────────────┘ │
│                                                                 │
│  gRPC Request                                                   │
│       │                                                         │
│       ▼                                                         │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────┐ │
│  │ Extract Key │───▶│ Hash & Lookup│───▶│ Validate Scopes    │ │
│  │ from Request│    │ in DB       │    │ & Rate Limits       │ │
│  └─────────────┘    └─────────────┘    └─────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

**Key Properties**:
- Keys are hashed (Argon2id) before storage - never stored in plaintext
- Keys have configurable expiration dates
- Keys have scoped permissions (event types, rate limits)
- Keys can be rotated without downtime (multiple active keys per tenant)

#### Scopes and Permissions

```yaml
# Example tenant permission model
tenant:
  id: "tenant-a"
  tier: "professional"  # free, starter, professional, enterprise

  permissions:
    event_types:
      - "token_transfer"
      - "contract_event"
    max_subscriptions: 5
    max_events_per_second: 1000
    max_inflight: 100
    historical_replay: true
    custom_processors: true

  restrictions:
    blocked_contracts: []  # Specific contracts tenant cannot access
    ip_allowlist: ["10.0.0.0/8", "192.168.1.0/24"]
```

### Data Isolation

#### Tenant Pipeline Isolation

| Isolation Level | Description | Trade-offs |
|----------------|-------------|------------|
| **Process** | Each tenant runs in separate OS process | High isolation, higher resource usage |
| **Container** | Each tenant runs in separate container | Good isolation, moderate overhead |
| **Namespace** | Shared process with logical separation | Lower isolation, efficient resources |

**Recommendation**: Container isolation (Docker/Nomad) for production, with resource limits:

```hcl
# Nomad job with tenant isolation
job "tenant-a-pipeline" {
  group "pipeline" {
    task "processor" {
      driver = "docker"

      resources {
        cpu    = 500   # MHz limit
        memory = 512   # MB limit
      }

      config {
        image = "obsrvr/tenant-a-gold:latest"

        # Network isolation
        network_mode = "bridge"

        # Read-only filesystem
        readonly_rootfs = true

        # Drop capabilities
        cap_drop = ["ALL"]
        cap_add  = ["NET_BIND_SERVICE"]
      }
    }
  }
}
```

#### Secrets Management

**Never expose**:
- Tenant sink credentials to the platform
- Platform database credentials to tenants
- Other tenants' API keys or data

**Vault Integration**:
```
┌──────────────────────────────────────────────────────────────┐
│                    Secrets Flow                              │
│                                                              │
│  Tenant Self-Service Portal                                  │
│           │                                                  │
│           ▼                                                  │
│  ┌─────────────────┐                                         │
│  │ Tenant provides │                                         │
│  │ sink credentials│                                         │
│  └────────┬────────┘                                         │
│           │                                                  │
│           ▼                                                  │
│  ┌─────────────────┐    ┌─────────────────┐                  │
│  │ Store in Vault  │───▶│ tenant-a/sinks/ │                  │
│  │ (tenant path)   │    │ s3-credentials  │                  │
│  └─────────────────┘    └─────────────────┘                  │
│                                                              │
│  Pipeline Runtime                                            │
│           │                                                  │
│           ▼                                                  │
│  ┌─────────────────┐    ┌─────────────────┐                  │
│  │ Pipeline reads  │───▶│ Vault agent     │                  │
│  │ from Vault      │    │ injects secrets │                  │
│  └─────────────────┘    └─────────────────┘                  │
└──────────────────────────────────────────────────────────────┘
```

### Network Security

#### gRPC Security

```protobuf
// All gRPC connections should use TLS
// silver-event-streamer configuration:

server:
  tls:
    enabled: true
    cert_file: "/etc/certs/server.crt"
    key_file: "/etc/certs/server.key"
    ca_file: "/etc/certs/ca.crt"  # For mTLS
    client_auth: "require_and_verify"  # mTLS for tenant connections
```

#### Network Policies (Kubernetes/Nomad)

```yaml
# Only allow tenant pipelines to connect to silver-event-streamer
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: tenant-pipeline-egress
spec:
  podSelector:
    matchLabels:
      component: tenant-pipeline
  policyTypes:
    - Egress
  egress:
    - to:
        - podSelector:
            matchLabels:
              component: silver-event-streamer
      ports:
        - protocol: TCP
          port: 50051
    # Allow tenant-specific sink destinations
    - to:
        - ipBlock:
            cidr: 0.0.0.0/0  # Tenant sinks (external)
      ports:
        - protocol: TCP
          port: 443
```

### Audit Logging

All tenant actions should be logged:

```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "tenant_id": "tenant-a",
  "action": "subscribe",
  "resource": "silver-event-streamer",
  "details": {
    "event_types": ["token_transfer"],
    "filters": {"asset_codes": ["USDC"]},
    "source_ip": "10.0.1.50"
  },
  "result": "success"
}
```

**Logged Events**:
- API key creation/rotation/revocation
- Subscription start/stop
- Pipeline deployment/update/deletion
- Rate limit violations
- Authentication failures

### Rate Limiting

```go
// Per-tenant rate limiting
type TenantRateLimiter struct {
    limits map[string]*rate.Limiter
    mu     sync.RWMutex
}

func (r *TenantRateLimiter) Allow(tenantID string) bool {
    r.mu.RLock()
    limiter, exists := r.limits[tenantID]
    r.mu.RUnlock()

    if !exists {
        return false
    }

    return limiter.Allow()
}

// Configuration per tier
var tierLimits = map[string]rate.Limit{
    "free":         rate.Limit(10),    // 10 events/sec
    "starter":      rate.Limit(100),   // 100 events/sec
    "professional": rate.Limit(1000),  // 1000 events/sec
    "enterprise":   rate.Limit(10000), // 10000 events/sec
}
```

---

## Tenant Self-Service API

### REST API Design

**Base URL**: `https://api.obsrvr.com/v1`

#### Authentication

All API requests require a management API key in the header:
```
Authorization: Bearer <management-api-key>
```

#### Endpoints

##### Tenant Management

```
POST   /tenants                    # Create tenant (admin only)
GET    /tenants/{tenant_id}        # Get tenant details
PATCH  /tenants/{tenant_id}        # Update tenant settings
DELETE /tenants/{tenant_id}        # Delete tenant (admin only)
```

##### API Keys

```
POST   /tenants/{tenant_id}/api-keys           # Create API key
GET    /tenants/{tenant_id}/api-keys           # List API keys
DELETE /tenants/{tenant_id}/api-keys/{key_id}  # Revoke API key
POST   /tenants/{tenant_id}/api-keys/{key_id}/rotate  # Rotate key
```

##### Subscriptions

```
POST   /tenants/{tenant_id}/subscriptions      # Create subscription
GET    /tenants/{tenant_id}/subscriptions      # List subscriptions
GET    /tenants/{tenant_id}/subscriptions/{id} # Get subscription details
PATCH  /tenants/{tenant_id}/subscriptions/{id} # Update subscription
DELETE /tenants/{tenant_id}/subscriptions/{id} # Delete subscription
```

##### Pipelines

```
POST   /tenants/{tenant_id}/pipelines          # Deploy pipeline
GET    /tenants/{tenant_id}/pipelines          # List pipelines
GET    /tenants/{tenant_id}/pipelines/{id}     # Get pipeline details
PATCH  /tenants/{tenant_id}/pipelines/{id}     # Update pipeline
DELETE /tenants/{tenant_id}/pipelines/{id}     # Delete pipeline
POST   /tenants/{tenant_id}/pipelines/{id}/restart  # Restart pipeline
GET    /tenants/{tenant_id}/pipelines/{id}/logs     # Get pipeline logs
```

##### Sinks

```
POST   /tenants/{tenant_id}/sinks              # Register sink
GET    /tenants/{tenant_id}/sinks              # List sinks
GET    /tenants/{tenant_id}/sinks/{id}         # Get sink details
PATCH  /tenants/{tenant_id}/sinks/{id}         # Update sink
DELETE /tenants/{tenant_id}/sinks/{id}         # Delete sink
POST   /tenants/{tenant_id}/sinks/{id}/test    # Test sink connectivity
```

##### Metrics & Usage

```
GET    /tenants/{tenant_id}/usage              # Get usage summary
GET    /tenants/{tenant_id}/usage/events       # Get event counts
GET    /tenants/{tenant_id}/usage/bandwidth    # Get bandwidth usage
GET    /tenants/{tenant_id}/metrics            # Get real-time metrics
```

### API Request/Response Examples

#### Create Subscription

**Request**:
```http
POST /v1/tenants/tenant-a/subscriptions
Content-Type: application/json
Authorization: Bearer mgmt_xxx

{
  "name": "usdc-transfers",
  "event_types": ["token_transfer"],
  "filter": {
    "asset_codes": ["USDC"],
    "min_amount": 1000000
  },
  "start_ledger": 0,
  "max_inflight": 100
}
```

**Response**:
```json
{
  "id": "sub_abc123",
  "tenant_id": "tenant-a",
  "name": "usdc-transfers",
  "status": "active",
  "event_types": ["token_transfer"],
  "filter": {
    "asset_codes": ["USDC"],
    "min_amount": 1000000
  },
  "created_at": "2024-01-15T10:30:00Z",
  "stats": {
    "events_delivered": 0,
    "last_event_at": null
  }
}
```

#### Deploy Pipeline

**Request**:
```http
POST /v1/tenants/tenant-a/pipelines
Content-Type: application/json
Authorization: Bearer mgmt_xxx

{
  "name": "usdc-to-s3",
  "subscription_id": "sub_abc123",
  "processors": [
    {
      "id": "enricher",
      "type": "usdc-enricher",
      "config": {}
    }
  ],
  "sink": {
    "type": "s3",
    "config": {
      "bucket": "tenant-a-data",
      "prefix": "gold/usdc/",
      "format": "parquet"
    },
    "credentials_ref": "sink_creds_xyz"
  }
}
```

**Response**:
```json
{
  "id": "pipe_def456",
  "tenant_id": "tenant-a",
  "name": "usdc-to-s3",
  "status": "deploying",
  "subscription_id": "sub_abc123",
  "processors": [...],
  "sink": {...},
  "created_at": "2024-01-15T10:35:00Z",
  "deployed_at": null,
  "health": {
    "status": "pending",
    "last_check": null
  }
}
```

#### Get Usage

**Request**:
```http
GET /v1/tenants/tenant-a/usage?start=2024-01-01&end=2024-01-31
Authorization: Bearer mgmt_xxx
```

**Response**:
```json
{
  "tenant_id": "tenant-a",
  "period": {
    "start": "2024-01-01T00:00:00Z",
    "end": "2024-01-31T23:59:59Z"
  },
  "usage": {
    "events_delivered": 15234567,
    "events_by_type": {
      "token_transfer": 12000000,
      "contract_event": 3234567
    },
    "bandwidth_bytes": 4523456789,
    "active_subscriptions": 3,
    "active_pipelines": 2
  },
  "billing": {
    "tier": "professional",
    "included_events": 10000000,
    "overage_events": 5234567,
    "estimated_cost_usd": 52.35
  }
}
```

### OpenAPI Specification

A full OpenAPI 3.0 specification should be maintained at:
`obsrvr-lake/api/openapi/tenant-api.yaml`

### SDK Support

Provide client SDKs for common languages:

```python
# Python SDK example
from obsrvr import TenantClient

client = TenantClient(
    api_key="mgmt_xxx",
    tenant_id="tenant-a"
)

# Create subscription
sub = client.subscriptions.create(
    name="usdc-transfers",
    event_types=["token_transfer"],
    filter={"asset_codes": ["USDC"]}
)

# Deploy pipeline
pipeline = client.pipelines.create(
    name="usdc-to-s3",
    subscription_id=sub.id,
    sink={
        "type": "s3",
        "bucket": "my-bucket"
    }
)

# Monitor usage
usage = client.usage.get(
    start="2024-01-01",
    end="2024-01-31"
)
```

---

## Integration Points

### PostgreSQL Changes (silver_hot)

Add NOTIFY triggers for real-time streaming:

```sql
-- Add notification trigger to silver tables
CREATE OR REPLACE FUNCTION notify_silver_change() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify(
    'silver_events',
    json_build_object(
      'table', TG_TABLE_NAME,
      'operation', TG_OP,
      'ledger_sequence', NEW.ledger_sequence,
      'id', NEW.id
    )::text
  );
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply to key silver tables
CREATE TRIGGER notify_token_transfers
  AFTER INSERT ON token_transfers_raw
  FOR EACH ROW EXECUTE FUNCTION notify_silver_change();

CREATE TRIGGER notify_enriched_operations
  AFTER INSERT ON enriched_history_operations
  FOR EACH ROW EXECUTE FUNCTION notify_silver_change();

CREATE TRIGGER notify_contract_events
  AFTER INSERT ON soroban_history_operations
  FOR EACH ROW EXECUTE FUNCTION notify_silver_change();
```

### Tenant Registry Schema

```sql
-- Tenant configuration
CREATE TABLE tenants (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  tier TEXT NOT NULL DEFAULT 'free',
  enabled BOOLEAN DEFAULT true,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),

  -- Limits based on tier
  max_subscriptions INT DEFAULT 1,
  max_events_per_second INT DEFAULT 10,
  max_pipelines INT DEFAULT 1,

  -- Metadata
  metadata JSONB DEFAULT '{}'
);

-- API keys
CREATE TABLE tenant_api_keys (
  id TEXT PRIMARY KEY,
  tenant_id TEXT REFERENCES tenants(id) ON DELETE CASCADE,
  key_hash TEXT NOT NULL,  -- Argon2id hash
  name TEXT,
  scopes TEXT[] DEFAULT '{}',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  expires_at TIMESTAMPTZ,
  last_used_at TIMESTAMPTZ,
  revoked_at TIMESTAMPTZ
);

CREATE INDEX idx_api_keys_tenant ON tenant_api_keys(tenant_id);
CREATE INDEX idx_api_keys_hash ON tenant_api_keys(key_hash);

-- Subscriptions
CREATE TABLE tenant_subscriptions (
  id TEXT PRIMARY KEY,
  tenant_id TEXT REFERENCES tenants(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  status TEXT DEFAULT 'active',  -- active, paused, deleted
  event_types TEXT[] NOT NULL,
  filter_config JSONB DEFAULT '{}',
  start_ledger INT DEFAULT 0,
  max_inflight INT DEFAULT 100,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),

  -- Stats
  events_delivered BIGINT DEFAULT 0,
  last_event_at TIMESTAMPTZ,
  last_ledger_sequence INT
);

CREATE INDEX idx_subscriptions_tenant ON tenant_subscriptions(tenant_id);

-- Pipelines
CREATE TABLE tenant_pipelines (
  id TEXT PRIMARY KEY,
  tenant_id TEXT REFERENCES tenants(id) ON DELETE CASCADE,
  subscription_id TEXT REFERENCES tenant_subscriptions(id),
  name TEXT NOT NULL,
  status TEXT DEFAULT 'pending',  -- pending, deploying, running, stopped, failed
  pipeline_yaml TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  deployed_at TIMESTAMPTZ,
  stopped_at TIMESTAMPTZ,

  -- Health
  health_status TEXT DEFAULT 'unknown',
  last_health_check TIMESTAMPTZ,
  error_message TEXT
);

CREATE INDEX idx_pipelines_tenant ON tenant_pipelines(tenant_id);

-- Sink credentials (references to Vault paths)
CREATE TABLE tenant_sink_credentials (
  id TEXT PRIMARY KEY,
  tenant_id TEXT REFERENCES tenants(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  sink_type TEXT NOT NULL,  -- s3, postgres, webhook, etc.
  vault_path TEXT NOT NULL,  -- Path in Vault where credentials are stored
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Usage tracking
CREATE TABLE tenant_usage (
  id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT REFERENCES tenants(id),
  period_start TIMESTAMPTZ NOT NULL,
  period_end TIMESTAMPTZ NOT NULL,
  events_delivered BIGINT DEFAULT 0,
  bytes_transferred BIGINT DEFAULT 0,
  events_by_type JSONB DEFAULT '{}',
  UNIQUE(tenant_id, period_start)
);

CREATE INDEX idx_usage_tenant_period ON tenant_usage(tenant_id, period_start);

-- Audit log
CREATE TABLE tenant_audit_log (
  id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT,
  action TEXT NOT NULL,
  resource_type TEXT,
  resource_id TEXT,
  actor TEXT,  -- API key ID or 'system'
  details JSONB,
  source_ip INET,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_audit_tenant ON tenant_audit_log(tenant_id, created_at);
```

---

## Deployment Architecture

### Option A: Single-Tenant Pipelines (Simpler)

Each tenant gets isolated flowctl process:

```
┌─────────────────────────────────────────────────────────┐
│  silver-event-streamer (single instance)                │
│  Port: 50051                                            │
└─────────────────────┬───────────────────────────────────┘
                      │
     ┌────────────────┼────────────────┐
     ▼                ▼                ▼
┌──────────┐    ┌──────────┐    ┌──────────┐
│flowctl   │    │flowctl   │    │flowctl   │
│tenant-a  │    │tenant-b  │    │tenant-c  │
│(container)│   │(container)│   │(container)│
└──────────┘    └──────────┘    └──────────┘
```

### Option B: Multi-Tenant Orchestration (Scalable)

Single flowctl control plane with tenant namespacing:

```
┌─────────────────────────────────────────────────────────┐
│  flowctl control plane                                   │
│  • Tenant isolation via namespaces                      │
│  • Shared processor pool                                │
│  • Resource quotas per tenant                           │
└─────────────────────────────────────────────────────────┘
```

### Nomad Job: silver-event-streamer

```hcl
job "silver-event-streamer" {
  datacenters = ["dc1"]
  type = "service"

  group "streamer" {
    count = 1

    network {
      port "grpc" { static = 50051 }
      port "health" { static = 8097 }
      port "api" { static = 8098 }
    }

    task "streamer" {
      driver = "docker"

      config {
        image = "obsrvr/silver-event-streamer:latest"
        ports = ["grpc", "health", "api"]
      }

      template {
        data = <<EOF
SILVER_HOT_DSN={{ with secret "database/creds/silver-streamer" }}{{ .Data.connection_url }}{{ end }}
GRPC_PORT=50051
HEALTH_PORT=8097
API_PORT=8098
TLS_CERT_FILE=/secrets/server.crt
TLS_KEY_FILE=/secrets/server.key
EOF
        destination = "secrets/env"
        env = true
      }

      vault {
        policies = ["silver-streamer"]
      }

      service {
        name = "silver-event-streamer"
        port = "grpc"

        check {
          type     = "http"
          port     = "health"
          path     = "/health"
          interval = "10s"
          timeout  = "2s"
        }
      }

      resources {
        cpu    = 1000
        memory = 1024
      }
    }
  }
}
```

### Nomad Job: Tenant Pipeline Template

```hcl
job "tenant-[[.tenant_id]]-pipeline" {
  datacenters = ["dc1"]
  type = "service"

  meta {
    tenant_id = "[[.tenant_id]]"
    pipeline_id = "[[.pipeline_id]]"
  }

  group "pipeline" {
    count = 1

    network {
      port "health" {}
    }

    task "flowctl" {
      driver = "docker"

      config {
        image = "obsrvr/flowctl:latest"
        args = ["run", "-f", "/local/pipeline.yaml"]
        ports = ["health"]
      }

      template {
        data = <<EOF
[[.pipeline_yaml]]
EOF
        destination = "local/pipeline.yaml"
      }

      vault {
        policies = ["tenant-[[.tenant_id]]"]
      }

      resources {
        cpu    = [[.cpu_limit]]
        memory = [[.memory_limit]]
      }

      service {
        name = "tenant-[[.tenant_id]]-pipeline"
        port = "health"

        check {
          type     = "http"
          path     = "/health"
          interval = "30s"
          timeout  = "5s"
        }

        meta {
          tenant_id = "[[.tenant_id]]"
          pipeline_id = "[[.pipeline_id]]"
        }
      }
    }
  }
}
```

---

## Implementation Phases

### Phase 1: Core Streaming Service (1 week appetite)

**Must Have**:
- silver-event-streamer service with basic Subscribe RPC
- PostgreSQL NOTIFY integration for token_transfers_raw
- Single-tenant proof of concept
- Health endpoint

**Nice to Have**:
- Multiple event types
- Basic filtering

**Could Have**:
- Metrics/stats endpoint

**Done**: Single tenant can subscribe and receive token transfer events in real-time.

### Phase 2: flowctl Integration (1 week appetite)

**Must Have**:
- silver-source-adapter for flowctl
- Example tenant pipeline YAML
- S3 sink working end-to-end

**Nice to Have**:
- Postgres sink
- Webhook sink

**Could Have**:
- Multi-tenant isolation

**Done**: Tenant pipeline deploys via flowctl and writes to S3.

### Phase 3: Self-Service API (1 week appetite)

**Must Have**:
- Tenant registry (PostgreSQL schema)
- API key management endpoints
- Subscription CRUD endpoints

**Nice to Have**:
- Pipeline deployment endpoints
- Usage tracking

**Could Have**:
- Billing integration

**Done**: Tenant can self-service create subscriptions via REST API.

### Phase 4: Security & Production Hardening (1 week appetite)

**Must Have**:
- mTLS for gRPC connections
- Vault integration for secrets
- Rate limiting per tenant
- Audit logging

**Nice to Have**:
- Network policies
- Container security hardening

**Could Have**:
- IP allowlisting

**Done**: Production-ready with security controls.

### Phase 5: Processor Library (1 week appetite)

**Must Have**:
- 3-4 common filter processors
- 2 enrichment processors
- Documentation

**Nice to Have**:
- Time-window aggregator
- Additional sinks (BigQuery, Kafka)

**Could Have**:
- Custom processor SDK

**Done**: Tenants have reusable processors for common use cases.

---

## Verification

### End-to-End Test

1. Start silver-event-streamer connected to silver_hot
2. Create tenant via API
3. Create subscription via API
4. Deploy tenant pipeline via flowctl
5. Insert test data into silver_hot
6. Verify events flow through pipeline to sink
7. Check tenant stats and metrics via API

### Load Test

1. Simulate 1000 events/second through silver layer
2. Run 3 concurrent tenant pipelines
3. Verify no event loss
4. Measure end-to-end latency (target: <500ms)
5. Verify rate limiting works correctly

### Security Test

1. Attempt to access another tenant's subscription (should fail)
2. Attempt to exceed rate limits (should throttle)
3. Verify API key rotation doesn't drop events
4. Verify audit logs capture all actions

### Failure Scenarios

1. Tenant pipeline crashes → verify reconnection and resume
2. silver-event-streamer restarts → verify tenant pipelines reconnect
3. Sink unavailable → verify backpressure propagates
4. API key expired → verify clean disconnection

---

## File Structure

```
obsrvr-lake/
├── silver-event-streamer/
│   ├── go/
│   │   ├── main.go
│   │   ├── server/
│   │   │   ├── streamer.go         # gRPC server implementation
│   │   │   ├── subscriber.go       # Subscription management
│   │   │   └── auth.go             # API key validation
│   │   ├── pg/
│   │   │   ├── listener.go         # PostgreSQL NOTIFY listener
│   │   │   └── queries.go          # Event queries
│   │   ├── api/
│   │   │   ├── router.go           # REST API routes
│   │   │   ├── tenants.go          # Tenant handlers
│   │   │   ├── subscriptions.go    # Subscription handlers
│   │   │   └── pipelines.go        # Pipeline handlers
│   │   └── gen/                    # Generated protobuf code
│   ├── Makefile
│   ├── Dockerfile
│   └── nomad/
│       └── silver-event-streamer.nomad
├── adapters/
│   └── silver-source-adapter/
│       ├── go/
│       │   └── main.go             # flowctl source adapter
│       ├── Makefile
│       └── Dockerfile
├── processors/
│   ├── amount-filter/
│   ├── asset-filter/
│   ├── contract-filter/
│   ├── time-window-aggregator/
│   └── usdc-enricher/
├── sinks/
│   ├── s3-sink/
│   ├── postgres-sink/
│   ├── webhook-sink/
│   └── bigquery-sink/
├── protos/
│   └── silver_streamer.proto
├── api/
│   └── openapi/
│       └── tenant-api.yaml
├── pipelines/
│   └── examples/
│       ├── tenant-usdc-s3.yaml
│       ├── tenant-whale-alerts.yaml
│       └── tenant-contract-indexer.yaml
├── migrations/
│   └── tenant_registry/
│       ├── 001_create_tenants.sql
│       ├── 002_create_api_keys.sql
│       ├── 003_create_subscriptions.sql
│       └── 004_create_pipelines.sql
└── docs/
    └── multi-tenant-gold-layer-architecture.md  # This document
```

---

## Open Questions

1. **Delivery guarantees**: At-least-once vs exactly-once?
   - Recommendation: At-least-once with idempotent sinks

2. **Historical replay**: Should tenants be able to replay from a past ledger?
   - Recommendation: Yes, query silver_hot/cold with start_ledger parameter

3. **Schema evolution**: How to handle proto changes?
   - Recommendation: JSON payload with schema_version field, deprecation policy

4. **Tenant isolation level**: Process-level or namespace-level?
   - Recommendation: Start with container isolation, migrate to namespace if needed

5. **Pricing model**: Per-event, per-GB, or tier-based?
   - Recommendation: Tier-based with overage charges per 1M events

6. **SLA commitments**: What latency/uptime guarantees?
   - Recommendation: 99.9% uptime, <1s p99 latency for professional tier

---

## References

- [flowctl documentation](/home/tillman/Documents/flowctl/docs/)
- [nebu architecture decisions](/home/tillman/Documents/nebu/docs/ARCHITECTURE_DECISIONS.md)
- [obsrvr-lake CLAUDE.md](/home/tillman/Documents/ttp-processor-demo/obsrvr-lake/CLAUDE.md)
