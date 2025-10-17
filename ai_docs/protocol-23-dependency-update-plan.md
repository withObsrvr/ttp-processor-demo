# Protocol 23 Dependency Update Implementation Plan

**Date:** October 11, 2025
**Status:** Protocol 23 released to mainnet on September 3, 2025
**Current Project State:** Using pre-Protocol 23 dependencies
**Priority:** HIGH - Over 1 month behind stable release

## Executive Summary

This implementation plan addresses the critical need to update all services in the ttp-processor-demo project to Protocol 23. The Stellar network upgraded to Protocol 23 on September 3, 2025, introducing breaking changes to the RPC API, XDR schemas, and core data structures. This project is currently using pre-Protocol 23 dependencies from January to July 2025, which may cause compatibility issues with mainnet services.

**Critical Issues:**
1. ❌ stellar-rpc dependencies stuck on `v0.9.6` (January 30, 2025) - Latest is `v23.0.4` (October 1, 2025)
2. ❌ stellar/go dependencies using unstable commits - Latest stable is `v23.0.0` (August 14, 2025)
3. ⚠️  Uncommitted Protocol 23 work in stellar-live-source (cache.go, config.go, metrics.go)
4. ⚠️  Multiple services with inconsistent stellar/go versions

**Impact:**
- Potential RPC API incompatibility with mainnet services
- Missing support for Protocol 23 features (dual BucketList, hot archives)
- Technical debt accumulation
- Risk of runtime errors with Protocol 23 ledgers

## Current Dependency State Analysis

### Service-by-Service Breakdown

#### 1. **stellar-live-source** (RPC-based ledger source)
```
Current:
  github.com/stellar/go v0.0.0-20250908183029-d588c917d979 (Sep 8 commit)
  github.com/stellar/stellar-rpc v0.9.6-0.20250130160539-be7702aa01ba (Jan 30)

Target:
  github.com/stellar/go v23.0.0 or later
  github.com/stellar/stellar-rpc v23.0.4

Status: ⚠️ Partially updated, has uncommitted Protocol 23 work
Files: server/server.go, server/cache.go, server/config.go, server/metrics.go (modified)
```

#### 2. **stellar-live-source-datalake** (Archive-based ledger source)
```
Current:
  github.com/stellar/go v0.0.0-20250716214416-01d16bf8185f (July 16)
  github.com/stellar/stellar-rpc v0.9.6-0.20250130160539-be7702aa01ba (Jan 30)

Target:
  github.com/stellar/go v23.0.0 or later
  github.com/stellar/stellar-rpc v23.0.4

Status: ❌ Pre-Protocol 23
Note: Has implementation plan in ai_docs/protocol-23-upgrade-implementation-plan.md
```

#### 3. **ttp-processor** (Token Transfer Protocol processor)
```
Current:
  github.com/stellar/go v0.0.0-20250716214416-01d16bf8185f (July 16)
  github.com/stellar/stellar-rpc v0.9.6-0.20250130160539-be7702aa01ba (Jan 30)

Target:
  github.com/stellar/go v23.0.0 or later
  github.com/stellar/stellar-rpc v23.0.4

Status: ❌ Pre-Protocol 23
Dependencies: Depends on stellar-live-source (local replace)
```

#### 4. **contract-invocation-processor** (Contract invocation processor)
```
Current:
  github.com/stellar/go v0.0.0-20250716214416-01d16bf8185f (July 16)
  github.com/stellar/stellar-rpc v0.9.6-0.20250130160539-be7702aa01ba (Jan 30)

Target:
  github.com/stellar/go v23.0.0 or later
  github.com/stellar/stellar-rpc v23.0.4

Status: ❌ Pre-Protocol 23
Replace directive: Uses local stellar-live-source
```

#### 5. **contract-data-processor** (Contract data processor)
```
Current:
  github.com/stellar/go v0.0.0-20250716214416-01d16bf8185f (July 16)
  github.com/stellar/stellar-rpc v0.9.6-0.20250130160539-be7702aa01ba (Jan 30)

Target:
  github.com/stellar/go v23.0.0 or later
  github.com/stellar/stellar-rpc v23.0.4

Status: ❌ Pre-Protocol 23
```

#### 6. **stellar-arrow-source** (Apache Arrow data source)
```
Current:
  github.com/stellar/go v0.0.0-20250716214416-01d16bf8185f (July 16)

Target:
  github.com/stellar/go v23.0.0 or later

Status: ❌ Pre-Protocol 23
Note: Doesn't use stellar-rpc directly
```

#### 7. **cli_tool** (Command-line interface tool)
```
Current:
  github.com/stellar/go v0.0.0-20250322075617-5ccaef04a539 (Mar 22)
  github.com/stellar/stellar-rpc v0.9.6-0.20250130160539-be7702aa01ba (Jan 30)

Target:
  github.com/stellar/go v23.0.0 or later
  github.com/stellar/stellar-rpc v23.0.4

Status: ❌ Pre-Protocol 23 (oldest stellar/go version)
```

### Dependency Version Summary

| Service | Current stellar/go | Current stellar-rpc | Update Priority |
|---------|-------------------|---------------------|-----------------|
| stellar-live-source | Sep 8, 2025 | Jan 30, 2025 | HIGH - Has uncommitted work |
| stellar-live-source-datalake | July 16, 2025 | Jan 30, 2025 | HIGH - Foundation service |
| ttp-processor | July 16, 2025 | Jan 30, 2025 | HIGH - Core processor |
| contract-invocation-processor | July 16, 2025 | Jan 30, 2025 | MEDIUM |
| contract-data-processor | July 16, 2025 | Jan 30, 2025 | MEDIUM |
| stellar-arrow-source | July 16, 2025 | N/A | MEDIUM |
| cli_tool | Mar 22, 2025 | Jan 30, 2025 | LOW |

## Protocol 23 Breaking Changes Review

### 1. stellar-rpc v23.0.0+ Breaking Changes

#### API Changes
- ✅ **Removed `getLedgerEntry` endpoint** - Use `getLedger` instead
- ✅ **Removed `pagingToken` from `getEvents`** - Use cursor-based pagination
- ✅ **Diagnostic events removed from `getEvents` stream** - No longer included
- ✅ **New wildcard `"**"` for event topics** - More flexible filtering

#### XDR Changes
- ✅ **TransactionMetaV4 support** - New transaction metadata version
- ✅ **LedgerCloseMetaV2 support** - New ledger close metadata version
- ✅ **Non-root authorization** - `simulateTransaction` supports non-root auth

#### Impact on Services
- **stellar-live-source**: Needs RPC client updates for new API
- **ttp-processor**: May need to handle new metadata versions
- **contract-*-processor**: Event filtering changes affect event processors

### 2. stellar/go v23.0.0+ Changes (Protocol 23 Features)

#### CAP-62: Dual BucketList Architecture
```
Old: Single BucketList for all state
New: Dual BucketList
  - Live State BucketList (classic + active Soroban)
  - Hot Archive BucketList (evicted PERSISTENT entries)

XDR Changes:
  - New HotArchiveBucketEntry type
  - History Archive State includes hotArchiveBuckets
  - bucketListHash = SHA256(liveStateBucketListHash, hotArchiveHash)
```

**Impact:**
- All ledger processors need to handle hot archive buckets
- Storage schema updates for hot archive data
- Hash calculations must account for dual structure

#### CAP-66: Resource Accounting Changes
```
Old: Single resource accounting
New: Separated read resources
  - In-memory entries
  - Disk entries
  - Automatic restoration of archived entries

XDR Changes:
  - SorobanResourcesExtV0 with archivedSorobanEntries
  - evictedTemporaryLedgerKeys → evictedKeys
  - Removed evictedPersistentLedgerEntries
```

**Impact:**
- Fee calculation logic changes
- Contract size calculations differ
- Resource metering updates

#### CAP-63: Parallel Transaction Execution
```
New: Transaction sets support parallel execution
  - Conflict verification in footprints
  - Transaction entry limits
  - Efficient construction
```

**Impact:**
- Transaction processing order considerations
- Metadata structure changes

## Implementation Strategy

### Phase 1: Foundation Updates (Week 1)

#### Step 1.1: Update stellar-live-source (Day 1-2)

This is the foundation service that other processors depend on.

**Tasks:**
1. Review uncommitted changes in stellar-live-source
   ```bash
   cd stellar-live-source/go
   git diff server/server.go server/cache.go server/config.go server/metrics.go
   ```

2. Update dependencies
   ```bash
   cd stellar-live-source/go
   go get github.com/stellar/go@v23.0.0
   go get github.com/stellar/stellar-rpc@v23.0.4
   go mod tidy
   ```

3. Review and commit Protocol 23 enhancements
   - cache.go: Historical ledger caching
   - config.go: Configuration management
   - metrics.go: Data source metrics

4. Update server.go for Protocol 23 compatibility
   - Handle TransactionMetaV4
   - Handle LedgerCloseMetaV2
   - Update RPC client calls for removed endpoints

5. Build and test
   ```bash
   make build
   # Test against Protocol 23 testnet
   export RPC_ENDPOINT="https://soroban-testnet.stellar.org"
   export NETWORK_PASSPHRASE="Test SDF Network ; September 2015"
   make run
   ```

**Acceptance Criteria:**
- ✅ Builds without errors
- ✅ Connects to Protocol 23 RPC endpoints
- ✅ Streams ledgers successfully
- ✅ Health check passes
- ✅ No regression in functionality

#### Step 1.2: Update stellar-live-source-datalake (Day 2-3)

**Tasks:**
1. Update dependencies
   ```bash
   cd stellar-live-source-datalake/go
   go get github.com/stellar/go@v23.0.0
   go get github.com/stellar/stellar-rpc@v23.0.4
   go mod tidy
   ```

2. Implement Protocol 23 XDR handling
   - Add hot archive bucket support
   - Handle LedgerCloseMetaV2
   - Update dual BucketList processing

3. Review implementation plan
   - Check ai_docs/protocol-23-upgrade-implementation-plan.md
   - Implement critical sections

4. Test with archive data
   ```bash
   export BACKEND_TYPE=ARCHIVE
   export ARCHIVE_STORAGE_TYPE=GCS  # or S3
   export HISTORY_ARCHIVE_URLS="https://history.stellar.org/prd/core-live/core_live_001"
   make build && make run
   ```

**Acceptance Criteria:**
- ✅ Handles Protocol 23 ledgers from archives
- ✅ Supports dual BucketList structure
- ✅ Hot archive buckets processed correctly
- ✅ No data loss or corruption

### Phase 2: Core Processors (Week 2)

#### Step 2.1: Update ttp-processor (Day 4-5)

**Tasks:**
1. Update dependencies
   ```bash
   cd ttp-processor/go
   go get github.com/stellar/go@v23.0.0
   go mod tidy
   # Note: stellar-live-source is local replace, no update needed
   ```

2. Update TTP event extraction for Protocol 23
   - Handle new transaction metadata versions
   - Update resource accounting parsing
   - Test with Protocol 23 ledgers

3. Test end-to-end
   ```bash
   # Start stellar-live-source
   cd stellar-live-source/go && make run &

   # Start ttp-processor
   cd ttp-processor/go
   export LIVE_SOURCE_ENDPOINT="localhost:50052"
   make run

   # Test with consumer
   cd consumer_app/node
   npm start -- 100000 100010
   ```

**Acceptance Criteria:**
- ✅ Processes Protocol 23 ledgers correctly
- ✅ TTP events extracted accurately
- ✅ No errors with new metadata versions
- ✅ Consumer apps receive events

#### Step 2.2: Update contract-invocation-processor (Day 5-6)

**Tasks:**
1. Update dependencies
   ```bash
   cd contract-invocation-processor/go
   go get github.com/stellar/go@v23.0.0
   go mod tidy
   ```

2. Update contract invocation parsing
   - Handle parallel transaction execution
   - Update event filtering for new wildcard patterns
   - Test with Protocol 23 contract invocations

3. Integration testing
   ```bash
   # Start full pipeline
   stellar-live-source → contract-invocation-processor → consumer
   ```

**Acceptance Criteria:**
- ✅ Extracts contract invocations from Protocol 23 ledgers
- ✅ Handles parallel transactions correctly
- ✅ Event filtering works with new patterns

#### Step 2.3: Update contract-data-processor (Day 6-7)

**Tasks:**
1. Update dependencies
   ```bash
   cd contract-data-processor/go
   go get github.com/stellar/go@v23.0.0
   go mod tidy
   ```

2. Update contract data extraction
   - Handle hot archive entries
   - Support automatic restoration
   - Update resource accounting

3. Test with PostgreSQL consumer
   ```bash
   cd contract-data-processor/consumer/postgresql
   # Test full pipeline with database
   ```

**Acceptance Criteria:**
- ✅ Processes contract data from Protocol 23 ledgers
- ✅ Handles archived and restored entries
- ✅ PostgreSQL consumer works correctly

### Phase 3: Supporting Services (Week 3)

#### Step 3.1: Update stellar-arrow-source (Day 8)

**Tasks:**
1. Update dependencies
   ```bash
   cd stellar-arrow-source/go
   go get github.com/stellar/go@v23.0.0
   go mod tidy
   ```

2. Update Apache Arrow schema for Protocol 23
   - Add hot archive bucket fields
   - Update resource accounting fields
   - Test Parquet generation

3. Test with parquet consumer
   ```bash
   cd stellar-arrow-source/go
   make build
   ./stellar-arrow-source &

   cd consumer_app/parquet
   ./parquet-consumer localhost:8815 ./data 100000 100100
   ```

**Acceptance Criteria:**
- ✅ Arrow streams include Protocol 23 fields
- ✅ Parquet files generated correctly
- ✅ Consumers can read new schema

#### Step 3.2: Update cli_tool (Day 9)

**Tasks:**
1. Update dependencies
   ```bash
   cd cli_tool/go
   go get github.com/stellar/go@v23.0.0
   go get github.com/stellar/stellar-rpc@v23.0.4
   go mod tidy
   ```

2. Update CLI commands for Protocol 23
   - Update help text for removed endpoints
   - Add support for new features
   - Test all commands

**Acceptance Criteria:**
- ✅ CLI works with Protocol 23 services
- ✅ All commands functional
- ✅ Help text accurate

### Phase 4: Testing and Validation (Week 4)

#### Step 4.1: Integration Testing (Day 10-11)

**Test Scenarios:**

1. **End-to-End Pipeline Test**
   ```bash
   # Full pipeline from RPC to consumers
   stellar-live-source → ttp-processor → node consumer
   stellar-live-source → contract-invocation-processor → consumer
   stellar-live-source → contract-data-processor → postgresql
   ```

2. **Archive Processing Test**
   ```bash
   # Historical data from archives
   stellar-live-source-datalake (GCS) → processors → consumers
   stellar-live-source-datalake (S3) → processors → consumers
   ```

3. **Protocol 23 Feature Test**
   - Process ledgers with hot archive entries
   - Process ledgers with parallel transactions
   - Process ledgers with new metadata versions
   - Verify resource accounting calculations

4. **Arrow/Parquet Pipeline Test**
   ```bash
   stellar-arrow-source → parquet-consumer → data analysis
   ```

**Test Ledgers:**
- Testnet: Ledgers after Protocol 23 activation (July 17, 2025)
- Mainnet: Ledgers after Protocol 23 activation (September 3, 2025)
- Focus on ledgers with:
  - Contract invocations
  - State archival/restoration
  - Parallel transactions

#### Step 4.2: Performance Testing (Day 12)

**Benchmarks:**

1. **Throughput Test**
   - Measure ledgers/second for each processor
   - Compare to pre-Protocol 23 baselines
   - Identify performance regressions

2. **Latency Test**
   - End-to-end latency from RPC to consumer
   - Hot archive access latency
   - Cache hit/miss rates

3. **Resource Usage Test**
   - Memory usage with dual BucketList
   - CPU usage with parallel transactions
   - Network bandwidth

**Acceptance Criteria:**
- ✅ No significant performance regression (< 10% slower)
- ✅ Memory usage within acceptable limits
- ✅ Cache effectiveness > 80% for historical data

#### Step 4.3: Backwards Compatibility Testing (Day 13)

**Test Scenarios:**

1. **Pre-Protocol 23 Ledgers**
   - Process ledgers before Protocol 23 activation
   - Verify no errors or data loss
   - Ensure smooth transition at activation ledger

2. **Mixed Ledger Processing**
   - Stream from pre-Protocol 23 to post-Protocol 23
   - Verify protocol detection and handling
   - Test all processors with mixed data

**Acceptance Criteria:**
- ✅ Pre-Protocol 23 ledgers process correctly
- ✅ No errors at protocol transition point
- ✅ All processors handle both versions

### Phase 5: Documentation and Deployment (Week 5)

#### Step 5.1: Update Documentation (Day 14)

**Tasks:**

1. Update README files
   - stellar-live-source/README.md
   - stellar-live-source-datalake/README.md
   - ttp-processor/README.md
   - All processor README files
   - Root CLAUDE.md

2. Update configuration documentation
   - Environment variable changes
   - New Protocol 23 options
   - Migration guides

3. Create migration guide
   - Step-by-step upgrade procedure
   - Breaking changes summary
   - Rollback procedures

4. Update API documentation
   - gRPC interface changes
   - Protobuf schema updates
   - Consumer app changes

#### Step 5.2: Prepare Deployment (Day 15)

**Pre-Deployment Checklist:**

- [ ] All services build successfully
- [ ] All tests pass
- [ ] Integration tests pass
- [ ] Performance benchmarks met
- [ ] Documentation updated
- [ ] Migration guide created
- [ ] Rollback procedure documented

**Deployment Strategy:**

1. **Development Environment**
   - Deploy to dev environment
   - Run full test suite
   - Monitor for 24 hours

2. **Staging Environment**
   - Deploy to staging
   - Run production-like load tests
   - Monitor for 48 hours

3. **Production Deployment**
   - **Option A: Blue-Green Deployment**
     - Deploy new version alongside old
     - Switch traffic gradually
     - Quick rollback if needed

   - **Option B: Rolling Update**
     - Update services one at a time
     - Foundation services first (stellar-live-source)
     - Processors second
     - Consumers last

**Monitoring During Deployment:**
- Health check endpoints
- Error rates
- Processing throughput
- Latency metrics
- Cache hit rates
- Memory/CPU usage

#### Step 5.3: Post-Deployment Validation (Day 16-17)

**Validation Tasks:**

1. **Functional Validation**
   - All services running
   - Data flowing end-to-end
   - No errors in logs
   - Consumers receiving data

2. **Performance Validation**
   - Throughput within expected range
   - Latency acceptable
   - Resource usage normal
   - Cache performing well

3. **Protocol 23 Feature Validation**
   - Hot archive entries processed
   - Parallel transactions handled
   - New metadata versions parsed
   - Resource accounting correct

**Success Criteria:**
- ✅ Zero downtime migration
- ✅ No data loss
- ✅ Performance within 10% of baseline
- ✅ All Protocol 23 features working
- ✅ No critical errors

## Risk Assessment and Mitigation

### Critical Risks

#### Risk 1: Breaking Changes Cause Service Failures
**Probability:** Medium
**Impact:** High

**Mitigation:**
- Comprehensive testing before deployment
- Blue-green deployment strategy
- Quick rollback procedure
- Feature flags for Protocol 23 features
- Gradual traffic shift

**Rollback Plan:**
```bash
# Revert to pre-Protocol 23 commits
cd stellar-live-source/go
git checkout <pre-update-commit>
go build -o ../stellar-live-source main.go
# Restart service
```

#### Risk 2: Data Loss During Migration
**Probability:** Low
**Impact:** Critical

**Mitigation:**
- No data modification during update (read-only services)
- Archive backups before deployment
- Test data integrity in staging
- Monitor data completeness metrics
- Checksum validation

**Detection:**
- Compare ledger counts pre/post migration
- Verify no gaps in processed sequences
- Check data integrity with known checksums

#### Risk 3: Performance Degradation
**Probability:** Medium
**Impact:** Medium

**Mitigation:**
- Performance benchmarking in staging
- Load testing before production
- Gradual rollout with monitoring
- Cache tuning for hot archive access
- Resource scaling if needed

**Monitoring:**
- Throughput: ledgers/second
- Latency: p50, p95, p99
- Error rate: errors/minute
- Resource usage: CPU, memory, network

### High Risks

#### Risk 4: Incompatible XDR Parsing
**Probability:** Medium
**Impact:** High

**Mitigation:**
- XDR version detection
- Graceful degradation for unknown versions
- Extensive unit tests for XDR parsing
- Fallback to raw bytes on parse errors

**Detection:**
- XDR parse error rates
- Unknown version warnings in logs
- Data validation failures

#### Risk 5: RPC API Incompatibility
**Probability:** Low
**Impact:** High

**Mitigation:**
- Test against official Protocol 23 RPC endpoints
- Handle removed endpoints gracefully
- Update to official RPC client libraries
- API version negotiation

**Detection:**
- RPC call failures
- "Method not found" errors
- Unexpected response formats

### Medium Risks

#### Risk 6: Consumer App Breakage
**Probability:** Medium
**Impact:** Medium

**Mitigation:**
- Backwards compatible protobuf changes
- Version consumer apps if needed
- Test all consumer implementations
- Gradual consumer updates

**Detection:**
- Consumer connection errors
- Protobuf deserialization failures
- Consumer app crashes

#### Risk 7: Dependency Conflicts
**Probability:** Low
**Impact:** Medium

**Mitigation:**
- Careful go.mod management
- Test all interdependencies
- Resolve conflicts incrementally
- Use go work sync if needed

**Detection:**
- Build failures
- Import errors
- Version mismatch warnings

## Success Metrics

### Functional Metrics
- ✅ All services build without errors
- ✅ All unit tests pass
- ✅ All integration tests pass
- ✅ Protocol 23 ledgers process correctly
- ✅ Pre-Protocol 23 ledgers still work
- ✅ Zero data loss during migration

### Performance Metrics
- ✅ Throughput: ≥ 90% of pre-update baseline
- ✅ Latency p99: ≤ 110% of pre-update baseline
- ✅ Error rate: ≤ 0.01%
- ✅ Cache hit rate: ≥ 80% for historical data
- ✅ Memory usage: ≤ 120% of pre-update baseline

### Operational Metrics
- ✅ Zero downtime deployment
- ✅ Deployment time: ≤ 2 hours
- ✅ Rollback time (if needed): ≤ 15 minutes
- ✅ Documentation complete and accurate
- ✅ Team trained on Protocol 23 changes

### Quality Metrics
- ✅ Code review approval for all changes
- ✅ Test coverage: ≥ 80% for new code
- ✅ No critical bugs in production
- ✅ All Protocol 23 features validated
- ✅ Clean git history with good commit messages

## Timeline Summary

| Phase | Duration | Days | Deliverables |
|-------|----------|------|--------------|
| Phase 1: Foundation | 3 days | 1-3 | stellar-live-source, stellar-live-source-datalake updated |
| Phase 2: Core Processors | 4 days | 4-7 | ttp-processor, contract processors updated |
| Phase 3: Supporting | 2 days | 8-9 | stellar-arrow-source, cli_tool updated |
| Phase 4: Testing | 4 days | 10-13 | All tests pass, performance validated |
| Phase 5: Deployment | 4 days | 14-17 | Documentation, deployment, validation |
| **Total** | **17 days** | **~3.5 weeks** | Full Protocol 23 migration complete |

## Quick Start Commands

### Update All Services (Automated)

Create a script to update all services:

```bash
#!/bin/bash
# update-protocol-23.sh

set -e

STELLAR_GO_VERSION="v23.0.0"
STELLAR_RPC_VERSION="v23.0.4"

# List of directories to update
SERVICES=(
    "stellar-live-source/go"
    "stellar-live-source-datalake/go"
    "ttp-processor/go"
    "contract-invocation-processor/go"
    "contract-data-processor/go"
    "stellar-arrow-source/go"
    "cli_tool/go"
)

echo "Starting Protocol 23 dependency updates..."

for service in "${SERVICES[@]}"; do
    echo ""
    echo "================================================"
    echo "Updating $service"
    echo "================================================"

    if [ -d "$service" ]; then
        cd "$service"

        # Update stellar/go
        echo "Updating github.com/stellar/go to $STELLAR_GO_VERSION"
        go get github.com/stellar/go@$STELLAR_GO_VERSION

        # Update stellar-rpc if present
        if grep -q "github.com/stellar/stellar-rpc" go.mod; then
            echo "Updating github.com/stellar/stellar-rpc to $STELLAR_RPC_VERSION"
            go get github.com/stellar/stellar-rpc@$STELLAR_RPC_VERSION
        fi

        # Tidy dependencies
        echo "Running go mod tidy"
        go mod tidy

        # Return to root
        cd - > /dev/null

        echo "✓ $service updated successfully"
    else
        echo "⚠ Warning: $service directory not found"
    fi
done

echo ""
echo "================================================"
echo "All services updated to Protocol 23!"
echo "================================================"
echo ""
echo "Next steps:"
echo "1. Review changes: git diff"
echo "2. Build all services"
echo "3. Run tests"
echo "4. Test integration"
```

### Build All Services

```bash
#!/bin/bash
# build-all.sh

SERVICES=(
    "stellar-live-source"
    "stellar-live-source-datalake"
    "ttp-processor"
    "contract-invocation-processor"
    "contract-data-processor"
    "stellar-arrow-source"
    "cli_tool"
)

for service in "${SERVICES[@]}"; do
    echo "Building $service..."
    cd "$service"
    make build || exit 1
    cd ..
    echo "✓ $service built successfully"
done

echo "All services built successfully!"
```

### Test All Services

```bash
#!/bin/bash
# test-all.sh

SERVICES=(
    "stellar-live-source/go"
    "stellar-live-source-datalake/go"
    "ttp-processor/go"
    "contract-invocation-processor/go"
    "contract-data-processor/go"
    "stellar-arrow-source/go"
)

for service in "${SERVICES[@]}"; do
    echo "Testing $service..."
    cd "$service"
    go test ./... || exit 1
    cd - > /dev/null
    echo "✓ $service tests passed"
done

echo "All tests passed!"
```

## Conclusion

This implementation plan provides a comprehensive roadmap for updating all services in the ttp-processor-demo project to Protocol 23. The phased approach ensures:

1. **Foundation First:** Update core data sources before processors
2. **Incremental Validation:** Test after each phase
3. **Risk Mitigation:** Multiple safety checks and rollback procedures
4. **Minimal Downtime:** Blue-green deployment strategy
5. **Complete Documentation:** Migration guides and updated docs

**Timeline:** 3.5 weeks from start to full production deployment
**Risk Level:** Medium (with comprehensive mitigation strategies)
**Expected Outcome:** Fully Protocol 23 compliant system with no data loss and minimal service disruption

## Next Steps

1. **Review and Approve Plan** - Get team sign-off
2. **Set Up Test Environment** - Prepare staging with Protocol 23 data
3. **Begin Phase 1** - Start with stellar-live-source updates
4. **Execute Plan** - Follow phases sequentially with testing gates
5. **Monitor Production** - Continuous monitoring post-deployment

## References

- [Stellar Protocol 23 Announcement](https://stellar.org/blog/developers/announcing-protocol-23)
- [Protocol 23 Upgrade Guide](https://stellar.org/blog/developers/protocol-23-upgrade-guide)
- [stellar-rpc v23.0.4 Release Notes](https://github.com/stellar/stellar-rpc/releases/tag/v23.0.4)
- [stellar/go v23.0.0 Release Notes](https://github.com/stellar/go/releases/tag/horizonclient%2Ftxnbuild%2Fv23.0.0)
- [CAP-62: Soroban Live State Prioritization](https://github.com/stellar/stellar-protocol/blob/master/core/cap-0062.md)
- [CAP-66: Soroban In-memory Read Resource](https://github.com/stellar/stellar-protocol/blob/master/core/cap-0066.md)
- [CAP-63: Parallelism-friendly Transaction Scheduling](https://github.com/stellar/stellar-protocol/blob/master/core/cap-0063.md)

---

**Document Version:** 1.0
**Last Updated:** October 11, 2025
**Author:** Implementation Team
**Status:** Ready for Review
