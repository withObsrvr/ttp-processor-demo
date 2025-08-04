# Flowctl Apache Arrow Integration Plan

**Document Version**: 1.0  
**Date**: August 3, 2025  
**Status**: Planning Phase

## Executive Summary

This document outlines the integration plan for Apache Arrow support within the flowctl control plane architecture. Based on analysis of the existing flowctl codebase, the current architecture is well-suited for Apache Arrow integration with minimal structural changes required.

## Current Flowctl Architecture Analysis

### Control Plane Role
- **Service Orchestration Only**: Flowctl acts as a pure control plane that coordinates services but does NOT route data directly
- **Service Discovery**: Manages registration of sources, processors, and sinks with event type compatibility checking
- **Health Monitoring**: Tracks service health via heartbeats and provides monitoring/metrics aggregation
- **Pipeline Orchestration**: Starts services in dependency order and manages their lifecycle

### Data Flow Model
- **Direct Service-to-Service Communication**: Data flows directly between services via gRPC streaming, NOT through flowctl
- **Event-Driven Architecture**: Uses typed protobuf events (`Event` message with `event_type`, `payload`, `schema_version`)
- **Type Compatibility**: Control plane validates that processor input/output event types match when connecting services

### Current Service Types
1. **SOURCE** (`SERVICE_TYPE_SOURCE`): Produces events, no inputs
2. **PROCESSOR** (`SERVICE_TYPE_PROCESSOR`): Transforms events, requires inputs and produces outputs  
3. **SINK** (`SERVICE_TYPE_SINK`): Consumes events, requires inputs

### Key Architecture Benefits for Arrow Integration
- **Clean Separation**: Control plane handles coordination, services handle data transformation
- **Extensible Type System**: Event types can accommodate new formats like Arrow
- **No Data Routing**: Since flowctl doesn't route data directly, integration focuses on orchestration rather than data handling

## Apache Arrow Integration Strategy

### Phase 1: Core Arrow Support (Minimal Changes)

#### 1.1 Event Type Extensions

Extend the existing `Event` message in `common.proto`:

```protobuf
message Event {
  string event_type = 1;     // NEW Arrow types: "arrow_data.ArrowBatch", "arrow_data.ParquetFile"
  bytes payload = 2;         // Arrow IPC format bytes or Parquet file data
  uint32 schema_version = 3;
  google.protobuf.Timestamp timestamp = 4;
  map<string, string> metadata = 5;
  
  // NEW: Arrow-specific metadata
  ArrowMetadata arrow_metadata = 6;
}

message ArrowMetadata {
  string schema_fingerprint = 1;  // Arrow schema hash for compatibility
  int64 num_rows = 2;             // Number of rows in the batch
  repeated string column_names = 3; // Column names for quick inspection
  map<string, string> custom_metadata = 4; // Custom Arrow metadata
  string compression_codec = 5;    // Compression used (zstd, lz4, etc.)
}
```

#### 1.2 Arrow Event Types

Define standard Arrow event types:

```yaml
# Standard Arrow Event Types
arrow_data.ArrowBatch:          # Arrow IPC batch format
  - Columnar data in Arrow IPC format
  - Optimized for streaming and analytics
  - Schema embedded in metadata

arrow_data.ParquetFile:         # Parquet file format  
  - Columnar data in Parquet format
  - Optimized for storage and batch processing
  - Schema embedded in file

arrow_data.StellarLedgerBatch:  # Stellar-specific Arrow batches
  - Stellar ledger data in native Arrow format
  - Optimized schemas for TTP events and transactions
  - Protocol 23 compatible

arrow_data.TTPEventBatch:       # TTP events in Arrow format
  - Token transfer events in columnar format
  - Analytics-optimized field layout
  - High-frequency streaming support
```

#### 1.3 Service Registration Updates

No changes to service types needed. Arrow processors register as existing types:

```go
// Arrow data source (like stellar-arrow-source)
serviceInfo := &flowctlpb.ServiceInfo{
    ServiceType:      flowctlpb.ServiceType_SERVICE_TYPE_SOURCE,
    OutputEventTypes: []string{
        "arrow_data.StellarLedgerBatch",
        "arrow_data.TTPEventBatch",
    },
    HealthEndpoint:   healthEndpoint,
    MaxInflight:      100,
    Metadata: map[string]string{
        "data_format":   "apache_arrow",
        "arrow_version": "17.0.0",
        "network":       "testnet",
        "schema_evolution": "strict",
    },
}

// Arrow processor
serviceInfo := &flowctlpb.ServiceInfo{
    ServiceType:      flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
    InputEventTypes:  []string{"raw_ledger_service.RawLedger"},
    OutputEventTypes: []string{"arrow_data.ArrowBatch"},
    HealthEndpoint:   healthEndpoint,
    MaxInflight:      50,
    Metadata: map[string]string{
        "processor_type": "format_conversion",
        "input_format":   "protobuf",
        "output_format":  "apache_arrow",
        "conversion_mode": "native",
    },
}
```

### Phase 2: Schema Management Enhancement

#### 2.1 Schema Registry Integration

Extend `ServiceInfo` to include schema information:

```protobuf
message ServiceInfo {
  string service_id = 1;
  ServiceType service_type = 2;
  repeated string input_event_types = 3;
  repeated string output_event_types = 4;
  string health_endpoint = 5;
  int32 max_inflight = 6;
  map<string, string> metadata = 7;
  
  // NEW: Schema registry information
  repeated SchemaInfo supported_schemas = 8;
}

message SchemaInfo {
  string event_type = 1;           // e.g., "arrow_data.ArrowBatch"
  string schema_format = 2;        // "arrow_ipc", "parquet"
  string schema_fingerprint = 3;   // Arrow schema hash
  bytes schema_definition = 4;     // Serialized Arrow schema
  uint32 version = 5;              // Schema version number
  repeated string compatible_versions = 6; // Backward compatible versions
}
```

#### 2.2 Schema Management Methods

Add new RPC methods to the `ControlPlane` service:

```protobuf
service ControlPlane {
  // Existing methods...
  rpc Register(ServiceInfo) returns (ServiceAck);
  rpc Heartbeat(ServiceHeartbeat) returns (HeartbeatAck);
  
  // NEW: Schema management
  rpc RegisterSchema(SchemaRegistration) returns (SchemaAck);
  rpc GetSchema(SchemaRequest) returns (SchemaResponse);
  rpc ListCompatibleServices(CompatibilityRequest) returns (ServiceList);
  rpc ValidateSchemaCompatibility(CompatibilityRequest) returns (CompatibilityResponse);
}

message SchemaRegistration {
  string service_id = 1;
  SchemaInfo schema_info = 2;
}

message SchemaRequest {
  string event_type = 1;
  string schema_fingerprint = 2; // Optional: specific version
}

message CompatibilityRequest {
  string source_event_type = 1;
  string target_event_type = 2;
  string source_schema_fingerprint = 3;
  string target_schema_fingerprint = 4;
}
```

#### 2.3 Schema Compatibility Logic

Implement Arrow schema compatibility checking:

```go
// Schema compatibility levels
type CompatibilityLevel int32

const (
    COMPATIBILITY_NONE CompatibilityLevel = 0      // No compatibility
    COMPATIBILITY_BACKWARD CompatibilityLevel = 1  // Backward compatible
    COMPATIBILITY_FORWARD CompatibilityLevel = 2   // Forward compatible  
    COMPATIBILITY_FULL CompatibilityLevel = 3      // Fully compatible
)

// Arrow schema compatibility rules:
// - Column additions (nullable) = BACKWARD compatible
// - Column removals = FORWARD compatible  
// - Column type widening (int32->int64) = BACKWARD compatible
// - Column type narrowing = FORWARD compatible
// - Column reordering = FULL compatible (Arrow handles by name)
```

### Phase 3: Enhanced Monitoring and Observability

#### 3.1 Arrow-Specific Metrics

Extend heartbeat metrics for Arrow data processing:

```go
// Enhanced metrics in ServiceHeartbeat
map<string, float64> metrics = 3; // Add Arrow-specific metrics:

// Data throughput metrics
"arrow_batches_per_second": 45.2,
"arrow_rows_per_second": 15420.0,
"arrow_bytes_per_second": 2048576.0,

// Data quality metrics
"arrow_compression_ratio": 0.73,
"arrow_null_percentage": 0.02,
"arrow_schema_evolution_events": 2.0,

// Performance metrics
"arrow_serialization_time_ms": 12.5,
"arrow_deserialization_time_ms": 8.3,
"arrow_memory_usage_bytes": 134217728.0,

// Schema and compatibility metrics
"arrow_schema_validation_errors": 0.0,
"arrow_compatibility_check_failures": 0.0,
"arrow_batch_size_avg": 10000.0,
```

#### 3.2 Pipeline Configuration Extensions

Update pipeline YAML schema to support Arrow-specific configuration:

```yaml
# Pipeline configuration with Arrow support
spec:
  sources:
    - id: stellar-arrow-source
      image: ghcr.io/withobsrvr/stellar-arrow-source:latest
      config:
        arrow:
          format: "ipc"           # "ipc" or "parquet"
          compression: "zstd"     # "none", "lz4", "zstd", "brotli"
          batch_size: 10000       # Target rows per batch
          schema_evolution: "strict"  # "none", "backward", "forward", "full"
        output_schemas:
          - event_type: "arrow_data.StellarLedgerBatch"
            schema_file: "./schemas/stellar_ledger_v1.arrow"
          - event_type: "arrow_data.TTPEventBatch"  
            schema_file: "./schemas/ttp_events_v1.arrow"

  processors:
    - id: stellar-to-arrow-processor
      image: ghcr.io/withobsrvr/stellar-arrow-processor:latest
      inputs: [stellar-live-source]
      config:
        arrow:
          input_format: "protobuf"
          output_format: "arrow_ipc"
          conversion_mode: "native"    # "native", "hybrid", "compatibility"
          memory_optimization: true
        schema_mapping:
          "raw_ledger_service.RawLedger": "arrow_data.StellarLedgerBatch"
        performance:
          parallel_conversion: true
          batch_accumulation_timeout: "100ms"
```

#### 3.3 Monitoring Dashboard Extensions

Add Arrow-specific monitoring capabilities:

```yaml
# Arrow monitoring dashboard components
arrow_metrics:
  data_flow:
    - arrow_throughput_chart       # Real-time data flow rates
    - arrow_batch_size_histogram   # Distribution of batch sizes
    - arrow_compression_efficiency # Compression ratios over time
    
  schema_management:
    - schema_evolution_timeline    # Schema changes over time
    - compatibility_matrix         # Service compatibility grid
    - schema_validation_errors     # Schema validation failures
    
  performance:
    - arrow_processing_latency     # End-to-end processing times
    - memory_usage_tracking        # Arrow memory allocations
    - serialization_performance    # Ser/deser timing metrics

  alerts:
    - schema_compatibility_failures
    - arrow_processing_errors
    - memory_usage_threshold_exceeded
    - throughput_degradation
```

## Implementation Phases and Timeline

### Phase 1: Core Arrow Support (4-6 weeks)
**Deliverables:**
- Extended protobuf definitions with Arrow metadata
- Arrow event type registry
- Basic service registration for Arrow processors
- stellar-arrow-source flowctl integration

**Dependencies:**
- Complete stellar-arrow-source Phase 1 implementation
- Flowctl protobuf updates
- Basic integration testing

### Phase 2: Schema Management (6-8 weeks)
**Deliverables:**
- Schema registry implementation in control plane
- Schema compatibility checking logic
- Schema evolution tracking
- Pipeline configuration updates

**Dependencies:**
- Phase 1 completion
- Arrow schema fingerprinting algorithm
- Backward compatibility testing

### Phase 3: Enhanced Monitoring (4-6 weeks)
**Deliverables:**
- Arrow-specific metrics collection
- Monitoring dashboard updates
- Performance optimization recommendations
- Production deployment guidelines

**Dependencies:**
- Phase 2 completion
- Monitoring infrastructure updates
- Performance benchmarking

## Risk Assessment and Mitigation

### Technical Risks

**Risk**: Schema compatibility issues between Arrow versions
- **Mitigation**: Implement strict schema fingerprinting and compatibility matrices
- **Contingency**: Fallback to format conversion layers

**Risk**: Performance degradation with Arrow serialization overhead
- **Mitigation**: Extensive benchmarking and optimization during Phase 1
- **Contingency**: Hybrid mode supporting both Arrow and protobuf simultaneously

**Risk**: Memory usage increases with Arrow columnar format
- **Mitigation**: Implement Arrow memory pooling and batch size optimization
- **Contingency**: Configurable memory limits and batch size controls

### Operational Risks

**Risk**: Complexity increase in pipeline configuration
- **Mitigation**: Comprehensive documentation and configuration validation
- **Contingency**: Automated configuration generation tools

**Risk**: Monitoring complexity with additional metrics
- **Mitigation**: Gradual rollout of monitoring features with clear documentation
- **Contingency**: Optional monitoring features that can be disabled

## Success Criteria

### Phase 1 Success Criteria
- [ ] stellar-arrow-source successfully registers with flowctl
- [ ] Arrow event types flow through existing pipeline infrastructure
- [ ] Basic metrics collection for Arrow data processing
- [ ] No performance regression in existing protobuf pipelines

### Phase 2 Success Criteria
- [ ] Schema registry handles Arrow schema evolution
- [ ] Automatic compatibility checking between Arrow services
- [ ] Pipeline configuration supports Arrow-specific settings
- [ ] Schema validation prevents incompatible service connections

### Phase 3 Success Criteria
- [ ] Comprehensive Arrow monitoring dashboard
- [ ] Performance optimization recommendations based on metrics
- [ ] Production-ready Arrow pipeline deployment
- [ ] Documentation and training materials complete

## Migration Strategy

### Existing Pipeline Compatibility
- **No Breaking Changes**: All existing protobuf-based services continue to work unchanged
- **Gradual Migration**: Services can be migrated to Arrow incrementally
- **Hybrid Support**: Pipelines can mix Arrow and protobuf services during transition

### Service Migration Path
1. **Phase 1**: New Arrow services register alongside existing services
2. **Phase 2**: Existing services add Arrow output options while maintaining protobuf
3. **Phase 3**: Pure Arrow pipelines for new deployments, legacy support maintained

### Rollback Strategy
- **Service Level**: Individual services can rollback to protobuf output
- **Pipeline Level**: Entire pipelines can disable Arrow processing
- **Control Plane**: Arrow features can be disabled via feature flags

## Conclusion

The flowctl architecture's clean separation of concerns makes Apache Arrow integration straightforward and non-disruptive. The proposed phased approach allows for gradual adoption while maintaining full backward compatibility with existing protobuf-based services.

Key benefits of this integration:
- **Performance**: Native columnar processing for analytics workloads
- **Compatibility**: Seamless integration with existing flowctl architecture  
- **Scalability**: Improved throughput for high-volume data processing
- **Future-Proofing**: Schema evolution and compatibility management

The implementation plan provides a clear path from basic Arrow support to advanced schema management and monitoring, enabling the ttp-processor-demo project to leverage Apache Arrow's performance benefits while maintaining operational excellence.