# Product Requirements Document: Apache Arrow Integration for ttp-processor-demo

## Executive Summary

### Vision
Transform ttp-processor-demo from a traditional row-based gRPC streaming system to a high-performance columnar data processing pipeline using Apache Arrow, achieving 10x processing speed improvements and 80% memory reduction while maintaining backward compatibility.

### Strategic Context
- **Current State**: ttp-processor-demo processes Stellar TTP events using traditional protobuf/gRPC streaming with row-based data processing
- **Future State**: obsrvr-stellar-components represents the target architecture with full Apache Arrow integration
- **Migration Goal**: Add Apache Arrow capabilities to ttp-processor-demo as an evolutionary step toward the obsrvr-stellar-components architecture

## Problem Statement

### Current Performance Limitations
1. **Processing Bottlenecks**: XDR unmarshaling and row-by-row event processing limits throughput to ~50-100 ledgers/sec
2. **Memory Inefficiency**: Multiple data copies during protobuf serialization/deserialization consume excessive memory
3. **Limited Analytics**: Row-based format prevents efficient batch analytics and aggregations
4. **Storage Overhead**: No native Parquet support for efficient data lake storage

### Market Opportunity
- **10x Performance Improvement**: Apache Arrow's columnar format with SIMD optimizations
- **Analytics Enablement**: Direct integration with DataFusion, DuckDB, and analytics tools
- **Storage Efficiency**: Native Parquet output for data lake integration
- **Cost Reduction**: Reduced compute and storage costs through efficiency gains

## Solution Architecture

### High-Level Design

```
Current: Stellar Data → stellar-live-source → ttp-processor → Consumer Apps
                        (XDR → Protobuf)      (Row Processing)   (gRPC)

Proposed: Stellar Data → stellar-arrow-source → ttp-arrow-processor → arrow-analytics-sink
                         (XDR → Arrow)          (Columnar Compute)    (Parquet/JSON/WS)
```

### Implementation Strategy: Parallel Development
1. **Maintain Current Architecture**: Keep existing gRPC/protobuf services operational
2. **Add Arrow Services**: Implement parallel Arrow-based services
3. **Gradual Migration**: Allow consumers to choose between traditional and Arrow endpoints
4. **Performance Validation**: Benchmark and validate Arrow performance gains

## Detailed Requirements

### 1. Core Arrow Integration Components

#### 1.1 Stellar Arrow Source Service
**Requirement**: Replace `stellar-live-source` with Arrow-native data streaming

**Implementation Details**:
- **Port**: 8815 (Arrow Flight), 8088 (Health)
- **Input Sources**: RPC endpoints, data lake storage (GCS, S3, FS)
- **Output Format**: Arrow Record Batches via Arrow Flight protocol
- **Schema**: Versioned Arrow schema for Stellar ledger data

**Success Criteria**:
- Process 200+ ledgers/sec (4x current performance)
- Stream Arrow Record Batches with zero-copy efficiency
- Support both bounded and unbounded streams
- Maintain schema versioning and evolution

#### 1.2 TTP Arrow Processor Service  
**Requirement**: Replace `ttp-processor` with Arrow-native TTP event extraction

**Implementation Details**:
- **Port**: 8816 (Arrow Flight), 8088 (Health)
- **Processing**: Vectorized TTP event extraction using Arrow compute
- **Input**: Arrow Record Batches from stellar-arrow-source
- **Output**: Arrow Record Batches with TTP events

**Success Criteria**:
- Process 1000+ TTP events/sec (10x current performance)
- Support complex filtering and aggregation via Arrow compute
- Implement event type-specific optimizations
- Maintain all current TTP event types and fields

#### 1.3 Arrow Analytics Sink Service
**Requirement**: Multi-format output service supporting Parquet, JSON, and real-time streaming

**Implementation Details**:
- **Ports**: 8817 (Arrow Flight), 8080 (WebSocket), 8081 (REST), 8088 (Health)
- **Output Formats**:
  - **Parquet**: Columnar storage with compression (Snappy, Zstd, Gzip)
  - **JSON**: REST API and file output
  - **WebSocket**: Real-time streaming for live dashboards
  - **CSV**: Legacy compatibility
- **Partitioning**: Date, hour, asset_code, event_type partitions

**Success Criteria**:
- Write 5000+ events/sec to Parquet
- Stream 10000+ events/sec via WebSocket
- Support configurable partitioning and compression
- Implement data retention and lifecycle management

### 2. Schema and Data Models

#### 2.1 Arrow Schema Design
**Requirement**: Define comprehensive Arrow schemas for Stellar data and TTP events

**Schemas Required**:
1. **StellarLedgerSchema**: Ledger metadata, transactions, operations
2. **TTPEventSchema**: Token transfer events with all current fields
3. **ContractDataSchema**: Contract invocation and event data

**Schema Features**:
- **Versioning**: SemVer-compatible schema evolution
- **Metadata**: Field descriptions, indexing hints, validation rules
- **Type Optimization**: Fixed-size binary for hashes, timestamp precision, nullable fields
- **Compatibility**: Migration paths between schema versions

#### 2.2 Data Type Mapping
**Requirement**: Map existing protobuf types to optimal Arrow types

| Current Type | Arrow Type | Optimization |
|--------------|------------|-------------|
| uint32 | Uint32 | Direct mapping |
| bytes (hash) | FixedSizeBinary(32) | Memory alignment |
| string | UTF8 | Dictionary encoding for repeated values |
| timestamp | Timestamp(Microsecond, UTC) | Temporal analytics |
| repeated fields | List<T> | Nested processing |

### 3. Integration Points

#### 3.1 Backward Compatibility
**Requirement**: Maintain existing gRPC/protobuf endpoints during transition

**Implementation**:
- Run both Arrow and protobuf services in parallel
- Provide configuration flag to enable Arrow mode
- Implement conversion layer from Arrow to protobuf for legacy consumers
- Support gradual consumer migration

#### 3.2 Consumer Migration Path
**Requirement**: Enable consumers to migrate at their own pace

**Migration Options**:
1. **Arrow Native**: Direct Arrow Flight consumption
2. **Arrow-to-Protobuf**: Server-side conversion for legacy clients
3. **Hybrid**: Support both endpoints simultaneously

### 4. Performance Requirements

#### 4.1 Throughput Targets
- **Source Service**: 200+ ledgers/sec (vs current 50-100)
- **Processor Service**: 1000+ TTP events/sec (vs current 100-200)
- **Sink Service**: 5000+ events/sec Parquet write, 10000+ events/sec streaming

#### 4.2 Memory Efficiency
- **Target**: 80% memory reduction through zero-copy operations
- **Implementation**: Arrow record sharing between services
- **Monitoring**: Memory usage tracking and alerts

#### 4.3 Latency Requirements
- **End-to-end**: <100ms from ledger ingestion to consumer delivery
- **Processing**: <10ms per ledger for TTP extraction
- **Network**: <5ms for Arrow Flight transfers

### 5. Development Phases

#### Phase 1: Foundation (4 weeks)
**Deliverables**:
- Arrow schema definitions for Stellar ledger and TTP events
- Basic stellar-arrow-source with RPC ingestion
- Arrow Flight server implementation
- Integration with existing stellar-live-source data

**Success Criteria**:
- Successfully stream Arrow Record Batches
- Validate schema compatibility with existing data
- Achieve basic performance parity with current system

#### Phase 2: Core Processing (6 weeks)
**Deliverables**:
- ttp-arrow-processor with vectorized TTP extraction
- Arrow compute integration for filtering and transformations
- Comprehensive testing with real Stellar data
- Performance benchmarking vs current system

**Success Criteria**:
- Achieve 10x processing performance improvement
- Support all current TTP event types
- Pass all existing integration tests

#### Phase 3: Analytics and Output (4 weeks)
**Deliverables**:
- arrow-analytics-sink with Parquet output
- WebSocket streaming for real-time consumers
- Data partitioning and compression options
- Consumer migration tools and documentation

**Success Criteria**:
- Write Parquet files compatible with analytics tools
- Support real-time streaming at target throughput
- Provide seamless migration path for existing consumers

#### Phase 4: Production Readiness (3 weeks)
**Deliverables**:
- Production monitoring and alerting
- Performance optimization and tuning
- Security hardening and authentication
- Comprehensive documentation and deployment guides

**Success Criteria**:
- Ready for production deployment
- Complete monitoring and observability
- Security audit compliance

## Implementation Strategy

### Development Approach
1. **Parallel Development**: Build Arrow services alongside existing services
2. **Incremental Migration**: Start with development environments, gradually move to production
3. **Feature Parity**: Ensure Arrow implementation supports all current capabilities
4. **Performance Validation**: Continuous benchmarking against current system

### Technology Stack
- **Language**: Go 1.23+ (consistent with current codebase)
- **Arrow Library**: Apache Arrow Go v17.0.0+
- **Transport**: Arrow Flight over gRPC
- **Storage**: Native Parquet with compression
- **Monitoring**: Prometheus metrics (consistent with current system)

### Risk Mitigation
1. **Compatibility Risk**: Maintain parallel services during transition
2. **Performance Risk**: Continuous benchmarking and optimization
3. **Complexity Risk**: Phased implementation with clear rollback plans
4. **Learning Curve**: Team training on Arrow concepts and implementation

## Success Metrics

### Performance KPIs
- **Processing Throughput**: 10x improvement (target: 1000+ events/sec)
- **Memory Usage**: 80% reduction in memory consumption
- **End-to-End Latency**: <100ms from ingestion to delivery
- **Storage Efficiency**: 50%+ reduction in storage size via Parquet compression

### Operational KPIs
- **Service Availability**: 99.9% uptime
- **Error Rate**: <0.1% processing errors
- **Consumer Adoption**: 80% of consumers migrated within 6 months
- **Developer Satisfaction**: Improved development experience metrics

### Business Impact
- **Infrastructure Costs**: 50% reduction in compute and storage costs
- **Analytics Capabilities**: Enable real-time analytics and reporting
- **Time to Market**: Faster feature development with Arrow ecosystem tools
- **Competitive Advantage**: Industry-leading performance for Stellar data processing

## Resource Requirements

### Development Team
- **Go Developers**: 2-3 senior developers with gRPC experience
- **Data Engineer**: 1 developer with Arrow and analytics experience
- **DevOps Engineer**: 1 engineer for deployment and monitoring
- **QA Engineer**: 1 engineer for testing and validation

### Infrastructure
- **Development**: Additional compute resources for parallel services
- **Testing**: Performance testing environment matching production
- **Monitoring**: Enhanced monitoring for Arrow-specific metrics

### Timeline
- **Total Duration**: 17 weeks (4.25 months)
- **Milestone Reviews**: Weekly progress reviews with stakeholders
- **Go/No-Go Decisions**: After each phase completion

## Conclusion

Apache Arrow integration represents a transformative opportunity for ttp-processor-demo to achieve significant performance improvements while enabling advanced analytics capabilities. The phased approach ensures minimal risk while delivering substantial benefits.

**Key Benefits**:
- 10x processing performance improvement
- 80% memory usage reduction
- Native Parquet output for data lake integration
- Real-time analytics capabilities
- Backward compatibility during migration

**Recommendation**: Proceed with implementation following the proposed phased approach, beginning with Phase 1 foundation work.