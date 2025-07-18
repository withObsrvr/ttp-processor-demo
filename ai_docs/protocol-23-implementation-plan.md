# Protocol 23 Implementation Plan

## Overview

This document outlines the implementation plan for upgrading the TTP processor to support Stellar Protocol 23 changes. The upgrade introduces unified events streams, new event ordering, and enhanced token transfer processing capabilities.

## Key Protocol 23 Changes

### 1. Unified Events Stream Support
- **Feature**: SEP-0041 Soroban Token Interface Events
- **Impact**: All Stellar asset movements can now emit events using the Stellar Asset Contract (SAC) event format
- **Benefit**: Unified event tracking across all token movements on the network

### 2. Event Ordering Changes
- **New Order**: 
  1. Fee events from all transactions (first)
  2. Operation events for each transaction
  3. Fee refund events (last)
- **Impact**: Changes the chronological order of events in ledger processing

### 3. Transaction Metadata Version 4
- **Feature**: Support for unified events in transaction metadata
- **Impact**: Enhanced metadata processing capabilities

### 4. Multiplexed Account Support
- **Feature**: M-account support in Soroban and SAC
- **Impact**: Enables exchange crypto-deposits of Soroban tokens

## Implementation Timeline

### Phase 1: Development Environment Setup (Week 1)
- [ ] Set up development branch for Protocol 23 testing
- [ ] Update local development environment with Protocol 23 dependencies
- [ ] Create feature branch: `feature/protocol-23-upgrade`

### Phase 2: Core Implementation (Week 2)
- [ ] Update Go dependencies to Protocol 23 branch
- [ ] Implement unified events stream support
- [ ] Add configuration options for unified events
- [ ] Update server constructors and initialization

### Phase 3: Testing & Validation (Week 3)
- [ ] Test with Protocol 23 testnet
- [ ] Validate event ordering changes
- [ ] Performance testing with unified events
- [ ] Backward compatibility testing

### Phase 4: Documentation & Deployment Prep (Week 4)
- [ ] Update documentation and configuration guides
- [ ] Create deployment scripts for Protocol 23
- [ ] Prepare rollback procedures
- [ ] Final integration testing

## Technical Implementation Details

### 1. Dependency Updates

```bash
# Update go.mod to use Protocol 23 branch
cd ttp-processor/go
go get github.com/stellar/go@protocol-23
go mod tidy
```

### 2. Server Configuration Updates

#### main.go Changes
```go
// Add unified events configuration
enableUnifiedEvents := os.Getenv("ENABLE_UNIFIED_EVENTS")
var eventServer *server.EventServer

if strings.ToLower(enableUnifiedEvents) == "true" {
    eventServer, err = server.NewEventServerWithUnified(networkPassphrase, sourceServiceAddr)
} else {
    eventServer, err = server.NewEventServer(networkPassphrase, sourceServiceAddr)
}
```

#### server/server.go Changes
```go
// Add new constructor for unified events
func NewEventServerWithUnified(passphrase string, sourceServiceAddr string) (*EventServer, error) {
    // ... existing logger and connection setup ...
    
    processor := token_transfer.NewEventsProcessorForUnifiedEvents(passphrase)
    
    // ... rest of existing constructor logic ...
}
```

### 3. Environment Variables

New environment variables to add:
- `ENABLE_UNIFIED_EVENTS=true/false` - Enable unified events processing
- `PROTOCOL_VERSION=23` - Track protocol version for debugging

## Testing Strategy

### 1. Unit Testing
- [ ] Test both unified and traditional event processing modes
- [ ] Verify event ordering matches Protocol 23 specifications
- [ ] Test edge cases with multiplexed accounts

### 2. Integration Testing
- [ ] Test against Protocol 23 testnet
- [ ] Validate with real Stellar testnet data
- [ ] Test consumer compatibility (Node.js, Go WASM, Rust WASM)

### 3. Performance Testing
- [ ] Benchmark unified events vs traditional processing
- [ ] Test processing latency with new event ordering
- [ ] Validate memory usage with enhanced metadata

### 4. Backward Compatibility Testing
- [ ] Ensure existing consumers continue to work
- [ ] Test with Protocol 22 and earlier data
- [ ] Validate graceful fallback behavior

## Deployment Strategy

### 1. Staging Deployment
- Deploy to staging environment with Protocol 23 testnet
- Run comprehensive tests for 48 hours
- Validate all consumer applications work correctly

### 2. Production Preparation
- Create deployment scripts with feature flags
- Prepare rollback procedures
- Set up monitoring for Protocol 23 specific metrics

### 3. Production Deployment
- Deploy with unified events disabled initially
- Monitor system health and performance
- Gradually enable unified events for testing
- Full rollout after validation

## Configuration Management

### Development Configuration
```bash
# .env.development
ENABLE_UNIFIED_EVENTS=true
PROTOCOL_VERSION=23
STELLAR_NETWORK=testnet
```

### Production Configuration
```bash
# .env.production
ENABLE_UNIFIED_EVENTS=false  # Start disabled
PROTOCOL_VERSION=23
STELLAR_NETWORK=pubnet
```

## Monitoring & Observability

### New Metrics to Track
- Unified events processing rate
- Event ordering validation metrics
- Protocol 23 specific error rates
- Processing latency differences

### Health Checks
- Add Protocol 23 readiness checks
- Monitor event stream health
- Track unified events availability

## Risk Mitigation

### 1. Feature Flags
- Use `ENABLE_UNIFIED_EVENTS` flag for gradual rollout
- Implement circuit breaker for unified events
- Maintain fallback to traditional processing

### 2. Rollback Plan
- Keep Protocol 22 compatible version ready
- Prepare database rollback scripts if needed
- Document rollback procedures

### 3. Monitoring
- Set up alerts for Protocol 23 specific errors
- Monitor processing latency changes
- Track consumer application health

## Dependencies & Prerequisites

### 1. Infrastructure
- Stellar Protocol 23 testnet access
- Updated development environments
- CI/CD pipeline updates for Protocol 23

### 2. External Services
- Stellar testnet endpoint supporting Protocol 23
- Updated stellar-live-source with Protocol 23 support
- Consumer applications tested with Protocol 23

### 3. Team Preparation
- Team training on Protocol 23 changes
- Documentation review and updates
- Testing procedure validation

## Success Criteria

### 1. Technical Criteria
- [ ] All existing functionality works with Protocol 23
- [ ] Unified events processing implemented and tested
- [ ] Event ordering follows Protocol 23 specifications
- [ ] Performance metrics within acceptable ranges

### 2. Quality Criteria
- [ ] 100% backward compatibility maintained
- [ ] Zero data loss during transition
- [ ] All consumer applications continue to function
- [ ] Comprehensive test coverage for new features

### 3. Operational Criteria
- [ ] Monitoring and alerting in place
- [ ] Rollback procedures tested and documented
- [ ] Team trained on Protocol 23 operations
- [ ] Documentation updated and accurate

## Next Steps

1. **Immediate Actions**
   - Review and approve implementation plan
   - Set up development environment with Protocol 23
   - Create feature branch and begin implementation

2. **Week 1 Deliverables**
   - Protocol 23 development environment ready
   - Initial code changes implemented
   - Basic testing framework established

3. **Communication Plan**
   - Weekly progress updates to stakeholders
   - Technical review sessions with team
   - Consumer application teams notified of changes

## Questions & Considerations

1. **Timing**: Should we wait for Protocol 23 to be merged to master, or proceed with branch-based development?
2. **Consumer Impact**: Do we need to coordinate with consumer application teams?
3. **Data Migration**: Is any historical data migration required for unified events?
4. **Performance**: What are the acceptable performance thresholds for the new processing model?

---

*This document will be updated as implementation progresses and new requirements are identified.*