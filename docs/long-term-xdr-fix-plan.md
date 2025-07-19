# Long-Term XDR Decode Limits Fix - Implementation Plan

## Executive Summary

The TTP processor demo currently works perfectly for ledger ranges below 501100 but fails on higher ranges due to XDR decode limits in the withObsrvr packages. This document outlines a comprehensive plan to implement a long-term solution that enables processing of all Protocol 23 ledger data, including large Soroban contract structures.

## Problem Analysis

### Current State
- ✅ **Working**: Ledger ranges 409907-409948 (and likely up to ~501099)
- ❌ **Failing**: Ledger ranges 501100+ with error: `xdr:DecodeString: data exceeds max slice limit - read: '345221'`
- 🔍 **Root Cause**: withObsrvr packages use outdated XDR decode limits (64KB default vs 10MB+ needed for Protocol 23)

### Affected Components
```
stellar-live-source-datalake/
├── github.com/withObsrvr/stellar-ledgerbackend@v0.0.0-20241220092445-b96fa5b9c924
├── github.com/withObsrvr/stellar-datastore@v0.0.0-20250207023055-4074500adc35
└── github.com/withObsrvr/stellar-cdp@v0.0.0-20241220082310-1a8c717a9c8f
```

### Technical Details
- **Error Location**: XDR decode operations in ledger buffer processing
- **Data Size**: 345,221 bytes (345KB) - typical for large Soroban contracts
- **Current Limit**: 64KB (default XDR decode limit)
- **Required Limit**: 10MB+ (Protocol 23 Soroban contract data)

## Solution Options Analysis

### Option A: Replace withObsrvr Packages with Official stellar-go
**Pros:**
- Always up-to-date with latest stellar-go improvements
- Official support and maintenance
- Better Protocol 23 compatibility
- Simplified dependency management

**Cons:**
- Requires significant refactoring of business logic
- May lose custom enterprise features (circuit breakers, metrics)
- Potential API breaking changes
- Higher implementation complexity

**Effort:** High (4-6 weeks)
**Risk:** Medium-High

### Option B: Fork and Update withObsrvr Packages
**Pros:**
- Minimal code changes to existing application
- Preserves all custom features
- Full control over update timing
- Maintains current architecture

**Cons:**
- Creates maintenance burden
- Need to keep fork updated with upstream changes
- Potential licensing considerations
- Limited to fixing known issues

**Effort:** Medium (2-3 weeks)
**Risk:** Medium

### Option C: Hybrid Approach (Recommended)
**Pros:**
- Best of both worlds: official stellar-go for XDR + custom business logic
- Gradual migration path
- Preserves existing enterprise features
- Allows selective component replacement

**Cons:**
- Temporary complexity during migration
- Need to maintain compatibility layers
- Requires careful dependency management

**Effort:** Medium-High (3-4 weeks)
**Risk:** Low-Medium

## Recommended Implementation Plan (Option C)

### Phase 1: XDR Layer Replacement (Week 1-2)
**Goal**: Replace XDR decode operations with stellar-go implementations

#### Step 1.1: Create XDR Compatibility Layer
```go
// pkg/xdr/decoder.go
package xdr

import (
    stellarxdr "github.com/stellar/go/xdr"
    xdr3 "github.com/stellar/go-xdr/xdr3"
    "github.com/stellar/go/support/compressxdr"
)

// LargeDataDecoder handles Protocol 23 large data structures
type LargeDataDecoder struct {
    options xdr3.DecodeOptions
}

func NewLargeDataDecoder() *LargeDataDecoder {
    return &LargeDataDecoder{
        options: xdr3.DecodeOptions{
            MaxInputLen: 10 * 1024 * 1024, // 10MB for Protocol 23
            MaxDepth:    xdr3.DecodeDefaultMaxDepth,
        },
    }
}

func (d *LargeDataDecoder) DecodeLedgerCloseMeta(data []byte) (*stellarxdr.LedgerCloseMeta, error) {
    // Implementation using stellar-go XDR with custom options
}
```

#### Step 1.2: Update Ledger Buffer Processing
```go
// Replace withObsrvr ledger buffer with stellar-go based implementation
func (lb *ledgerBuffer) getFromLedgerQueue(ctx context.Context) (stellarxdr.LedgerCloseMetaBatch, error) {
    // Use new LargeDataDecoder instead of withObsrvr implementation
    decoder := NewLargeDataDecoder()
    return decoder.DecodeLedgerCloseMeta(compressedBinary)
}
```

#### Step 1.3: Update Buffered Meta Pipe Reader
```go
// Replace withObsrvr buffered reader with stellar-go based implementation
func (b *bufferedLedgerMetaReader) readLedgerMetaFromPipe() (*stellarxdr.LedgerCloseMeta, error) {
    // Use stellar-go XDR decoder with custom options
    decoder := xdr3.NewDecoderWithOptions(b.r, b.decodeOptions)
    return decoder.Decode(&xlcm)
}
```

### Phase 2: Storage Backend Integration (Week 2-3)
**Goal**: Integrate stellar-go storage capabilities while preserving custom features

#### Step 2.1: Create Storage Abstraction Layer
```go
// pkg/storage/interface.go
type StorageBackend interface {
    GetLedgerCloseMeta(sequence uint32) (*stellarxdr.LedgerCloseMeta, error)
    StreamLedgerCloseMeta(start, end uint32) (<-chan *stellarxdr.LedgerCloseMeta, error)
    GetLedgerRange() (uint32, uint32, error)
}

// Implementations:
// - StellarStorageBackend (using stellar-go)
// - HybridStorageBackend (using stellar-go XDR + custom features)
```

#### Step 2.2: Implement Hybrid Storage Backend
```go
// pkg/storage/hybrid.go
type HybridStorageBackend struct {
    stellarBackend   historyarchive.Archive
    customFeatures   *EnterpriseFeatures
    xdrDecoder      *LargeDataDecoder
}

func (h *HybridStorageBackend) GetLedgerCloseMeta(sequence uint32) (*stellarxdr.LedgerCloseMeta, error) {
    // Use stellar-go for data retrieval
    // Apply custom enterprise features (circuit breaker, metrics, etc.)
    // Use LargeDataDecoder for XDR operations
}
```

### Phase 3: Service Layer Migration (Week 3-4)
**Goal**: Update service layer to use new storage backend

#### Step 3.1: Update Raw Ledger Service
```go
// server/server.go
func (s *RawLedgerServer) StreamRawLedgers(req *pb.StreamLedgersRequest, stream pb.RawLedgerService_StreamRawLedgersServer) error {
    // Replace CDP ApplyLedgerMetadata with direct storage backend usage
    backend := storage.NewHybridStorageBackend(config)
    
    for sequence := req.StartLedger; sequence <= endLedger; sequence++ {
        lcm, err := backend.GetLedgerCloseMeta(sequence)
        if err != nil {
            return err
        }
        
        // Apply enterprise features
        s.applyEnterpriseFeatures(lcm)
        
        // Send to stream
        if err := s.sendLedgerToStream(lcm, stream); err != nil {
            return err
        }
    }
}
```

#### Step 3.2: Preserve Enterprise Features
```go
// pkg/enterprise/features.go
type EnterpriseFeatures struct {
    CircuitBreaker *CircuitBreaker
    Metrics        *EnterpriseMetrics
    RateLimiter    *RateLimiter
}

func (ef *EnterpriseFeatures) ApplyToLedger(lcm *stellarxdr.LedgerCloseMeta) error {
    // Apply circuit breaker logic
    // Update metrics
    // Apply rate limiting
    // Add enterprise monitoring
}
```

### Phase 4: Testing and Validation (Week 4)
**Goal**: Comprehensive testing of the new implementation

#### Test Categories:
1. **Unit Tests**: XDR decode operations with large data
2. **Integration Tests**: End-to-end ledger processing
3. **Performance Tests**: Throughput and latency validation
4. **Compatibility Tests**: Backward compatibility with existing APIs
5. **Load Tests**: High-volume ledger processing

#### Test Data:
- **Small Data**: Ledger ranges 409907-409948 (known working)
- **Large Data**: Ledger ranges 501100+ (currently failing)
- **Protocol 23 Data**: Mixed version transaction metadata
- **Soroban Data**: Large contract invocations and state changes

## Implementation Details

### Dependencies Update Strategy
```yaml
# Current dependencies
github.com/withObsrvr/stellar-ledgerbackend: v0.0.0-20241220092445-b96fa5b9c924
github.com/withObsrvr/stellar-datastore: v0.0.0-20250207023055-4074500adc35
github.com/withObsrvr/stellar-cdp: v0.0.0-20241220082310-1a8c717a9c8f

# Target dependencies
github.com/stellar/go: v0.0.0-20250718065357-be943c360056 (latest Protocol 23)
# Custom packages for enterprise features
github.com/withObsrvr/stellar-enterprise: v1.0.0 (new package)
```

### Configuration Changes
```yaml
# New configuration options
xdr:
  max_input_len: 10485760  # 10MB
  max_depth: 200
  enable_protocol_23: true

storage:
  backend: "hybrid"  # stellar-go + enterprise features
  buffer_size: 1000
  retry_strategy: "exponential"

enterprise:
  circuit_breaker:
    failure_threshold: 5
    reset_timeout: 30s
  metrics:
    enabled: true
    export_interval: 10s
```

### Migration Strategy
1. **Backward Compatibility**: Maintain existing APIs during transition
2. **Feature Flags**: Enable new implementation via environment variables
3. **Gradual Rollout**: Start with non-critical ledger ranges
4. **Monitoring**: Enhanced metrics during migration
5. **Rollback Plan**: Quick revert to current implementation if issues arise

## Risk Analysis and Mitigation

### Technical Risks
| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| XDR decode performance regression | Medium | High | Comprehensive benchmarking |
| Memory usage increase | Low | Medium | Memory profiling and optimization |
| API breaking changes | Low | High | Comprehensive compatibility testing |
| New stellar-go bugs | Low | Medium | Thorough integration testing |

### Business Risks
| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Extended downtime during migration | Low | High | Phased rollout with rollback plan |
| Loss of enterprise features | Low | High | Preserve features in hybrid approach |
| Delayed timeline | Medium | Medium | Buffer time in schedule |

## Success Criteria

### Technical Success Metrics
- ✅ Process ledger ranges 501100+ without XDR decode errors
- ✅ Maintain processing throughput within 5% of current performance
- ✅ Memory usage increase < 20%
- ✅ All existing APIs remain functional
- ✅ Enterprise features preserved (circuit breaker, metrics, rate limiting)

### Business Success Metrics
- ✅ Zero data loss during migration
- ✅ < 1 hour total downtime
- ✅ All Protocol 23 features supported
- ✅ Soroban contract data processing capability

## Timeline and Milestones

### Week 1: Foundation
- [ ] Create XDR compatibility layer
- [ ] Implement LargeDataDecoder
- [ ] Update ledger buffer processing
- [ ] Unit tests for XDR operations

### Week 2: Storage Integration
- [ ] Create storage abstraction layer
- [ ] Implement hybrid storage backend
- [ ] Update buffered meta pipe reader
- [ ] Integration tests

### Week 3: Service Migration
- [ ] Update raw ledger service
- [ ] Preserve enterprise features
- [ ] Update configuration system
- [ ] End-to-end testing

### Week 4: Validation
- [ ] Comprehensive testing
- [ ] Performance validation
- [ ] Documentation updates
- [ ] Deployment preparation

## Deployment Strategy

### Pre-Deployment
1. **Staging Environment**: Full testing with production data
2. **Performance Baseline**: Establish current performance metrics
3. **Rollback Plan**: Prepare quick revert procedures
4. **Monitoring**: Enhanced observability during deployment

### Deployment Phases
1. **Phase 1**: Deploy to development environment
2. **Phase 2**: Deploy to staging with full feature testing
3. **Phase 3**: Deploy to production with feature flags
4. **Phase 4**: Enable new features for subset of ledger ranges
5. **Phase 5**: Full rollout after validation

### Post-Deployment
1. **Monitoring**: 24/7 monitoring for first 48 hours
2. **Performance Validation**: Compare against baseline metrics
3. **User Acceptance**: Validate all use cases work as expected
4. **Documentation**: Update all relevant documentation

## Long-Term Maintenance

### Ongoing Responsibilities
1. **Dependency Updates**: Keep stellar-go dependency current
2. **Protocol Updates**: Monitor and implement new Stellar protocol changes
3. **Performance Monitoring**: Continuous performance optimization
4. **Security Updates**: Regular security patch application

### Future Considerations
1. **Protocol 24 Preparation**: Ensure architecture supports future protocols
2. **Soroban Evolution**: Stay current with Soroban smart contract improvements
3. **Enterprise Features**: Continuous improvement of monitoring and reliability
4. **Scalability**: Plan for increased data volumes and processing requirements

## Conclusion

This implementation plan provides a comprehensive approach to fixing the XDR decode limits issue while preserving all existing functionality and enterprise features. The hybrid approach (Option C) offers the best balance of risk, effort, and long-term maintainability.

The phased implementation ensures minimal disruption to current operations while systematically addressing the root cause of the issue. With proper execution, this plan will enable the TTP processor demo to handle all Protocol 23 ledger ranges, including the largest Soroban contract data structures.

**Expected Outcome**: A robust, enterprise-grade stellar ledger processing system capable of handling current and future Protocol 23 requirements with 99.99% uptime guarantee.