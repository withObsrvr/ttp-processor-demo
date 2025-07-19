# Rollback Strategy Analysis: stellar-live-source-datalake

## Executive Summary

Based on git history analysis, the original `stellar-live-source-datalake` implementation (commit `3de0840`) was **fundamentally sound** and working correctly. The current issues stem from **over-engineering during Protocol 23 migration** rather than architectural flaws in the original design.

**Recommendation: ROLLBACK to commit `3de0840` and apply targeted Protocol 23 updates**

## Original Working Architecture Analysis

### What Was Working (Commit 3de0840)

**Clean, Purpose-Built Architecture**:
```go
// Original approach - SIMPLE and EFFECTIVE
import (
    "github.com/stellar/go/xdr"
    cdp "github.com/withObsrvr/stellar-cdp"
    datastore "github.com/withObsrvr/stellar-datastore"
    ledgerbackend "github.com/withObsrvr/stellar-ledgerbackend"
)

// Used CDP (Custom Data Pipeline) framework
err := cdp.ApplyLedgerMetadata(
    ledgerRange,
    publisherConfig,
    ctx,
    func(lcm xdr.LedgerCloseMeta) error {
        // Simple callback - just stream the data
        rawBytes, err := lcm.MarshalBinary()
        // Send to gRPC stream
    }
)
```

**Key Strengths of Original Design**:

1. **Proper Abstraction**: Used `withObsrvr/stellar-cdp` framework designed specifically for this use case
2. **Complete Data**: `ApplyLedgerMetadata` provided complete `LedgerCloseMeta` structures
3. **Tested Framework**: CDP was battle-tested with the specific datalake format
4. **No Format Parsing**: Framework handled all Galexie datalake parsing complexity
5. **Simple Integration**: Clean callback pattern for streaming data

### What We Broke During Protocol 23 Migration

**Current Broken Architecture**:
```go
// What we changed to - OVER-ENGINEERED and BROKEN
import (
    "github.com/stellar/go/historyarchive"  // WRONG - doesn't match format
    "github.com/stellar/go/support/storage" // LOW-LEVEL - too complex
)

// Tried to reinvent the wheel
type StellarArchiveReader struct {
    archive        stellar.ArchiveInterface  // Incompatible with Galexie
    galexieReader *GalexieDatalakeReader    // Custom parsing
}
```

**Mistakes Made**:

1. **Abandoned Working Framework**: Threw away `stellar-cdp` that was handling everything correctly
2. **Mixed Incompatible Interfaces**: Tried to force Galexie format into standard `historyarchive`
3. **Manual XDR Parsing**: Attempted low-level parsing instead of using proven framework
4. **Over-Complicated**: Went from 1 working abstraction to 3 broken ones

## Git History Analysis

### Timeline of Changes

**Working Version** (3de0840 - "add datalake"):
- Used `withObsrvr/stellar-cdp` framework
- Clean, simple implementation
- Complete `LedgerCloseMeta` structures
- Proven to work with datalake format

**Protocol 23 Migration** (ac9f2a1 - "update to protocol 23"):
- Only changed `flake.nix` build configuration
- **No code changes yet** - still working

**Where It Broke** (b7d6601 - "add all changes"):
- **Abandoned CDP framework**
- **Introduced manual archive parsing**
- **Added complex custom readers**
- **Created hybrid architecture problems**

### What the Original Code Did Right

**1. Used Purpose-Built Framework**:
```go
// stellar-cdp was designed specifically for this:
// - Reading from Galexie datalake format
// - Handling different storage backends (GCS, S3, FS)
// - Providing complete LedgerCloseMeta structures
// - Protocol version handling
```

**2. Simple Configuration**:
```go
schema := datastore.DataStoreSchema{
    LedgersPerFile:    ledgersPerFile,
    FilesPerPartition: filesPerPartition,
}

dsConfig := datastore.DataStoreConfig{
    Type: "GCS", 
    Schema: schema, 
    Params: dsParams
}
```

**3. Clean Data Flow**:
```go
// Input: Storage configuration + ledger range
// Framework: CDP handles all parsing complexity
// Output: Complete LedgerCloseMeta via callback
// Result: Stream to gRPC client
```

## Framework Comparison Analysis

### withObsrvr/stellar-cdp vs Manual Implementation

| Feature | Original CDP Framework | Current Manual Approach |
|---------|----------------------|-------------------------|
| **Galexie Format Parsing** | ✅ Built-in, tested | ❌ Custom, incomplete |
| **Protocol Support** | ✅ Framework handles versions | ❌ Manual version management |
| **Complete Data** | ✅ Full LedgerCloseMeta | ❌ Missing transaction data |
| **Storage Backends** | ✅ GCS, S3, FS support | ❌ Custom per-backend code |
| **Error Handling** | ✅ Framework patterns | ❌ Inconsistent custom errors |
| **Maintenance** | ✅ Framework team updates | ❌ We maintain everything |
| **Testing** | ✅ Framework has test suite | ❌ We write all tests |

### CDP Framework Protocol 23 Support

**Key Question**: Does `withObsrvr/stellar-cdp` support Protocol 23?

Looking at the dependencies:
```go
github.com/withObsrvr/stellar-cdp v0.0.0-20241220082310-1a8c717a9c8f
```

**Analysis**: The CDP framework was specifically built to handle Galexie datalake format and would likely have been updated for Protocol 23. We should verify this rather than rebuilding everything.

## Rollback Strategy: Step-by-Step

### Phase 1: Rollback to Working Code (Immediate)

**1. Reset to Working Commit**:
```bash
cd /home/tillman/Documents/ttp-processor-demo
git checkout 3de0840 -- stellar-live-source-datalake/
```

**2. Update Dependencies for Protocol 23**:
```bash
cd stellar-live-source-datalake/go
go get github.com/withObsrvr/stellar-cdp@latest
go get github.com/stellar/go@protocol-23
go mod tidy
```

**3. Test Original Architecture**:
```bash
# Build with original approach
make build-server
# Test with known working ledger range
```

### Phase 2: Verify Protocol 23 Support (1-2 days)

**1. Check CDP Framework Updates**:
- Contact `withObsrvr` team about Protocol 23 support in CDP
- Check if framework was updated for Protocol 23 features
- Test with recent ledgers (post-Protocol 23 activation)

**2. Validate Complete Data**:
```bash
# Test that LedgerCloseMeta includes:
# - Transaction processing data
# - Protocol 23 eviction fields
# - Proper TransactionMeta V3 structures
```

**3. Compare with RPC Version**:
```bash
# Ensure datalake version produces identical output to stellar-live-source
```

### Phase 3: Targeted Updates (If Needed)

**Only if CDP framework lacks Protocol 23 support**:

**Option A: Framework Extension**:
```go
// Extend CDP callback to handle Protocol 23 features
func enhancedCallback(lcm xdr.LedgerCloseMeta) error {
    // Check if Protocol 23 fields are present
    if lcm.V == 1 && lcm.V1.EvictedTemporaryLedgerKeys != nil {
        // Protocol 23 support confirmed
    }
    
    // Continue with original streaming logic
    return originalStreamingLogic(lcm)
}
```

**Option B: CDP Framework Update**:
- Work with `withObsrvr` team to update CDP for Protocol 23
- Much smaller scope than complete rewrite

## Risk Assessment

### Rollback Risks (LOW)
- **Time Investment**: 1-2 days to rollback and test
- **Dependency Risk**: CDP framework might need updates
- **Integration Risk**: Need to verify Protocol 23 compatibility

### Continue Current Approach Risks (HIGH)
- **Time Investment**: 2-4 weeks to fix all current issues
- **Complexity Risk**: Multiple custom parsers and interfaces
- **Maintenance Risk**: We become responsible for all parsing logic
- **Protocol Risk**: Missing Protocol 23 features hard to implement correctly

## Recommendation

### ROLLBACK is the Right Strategy Because:

1. **Original Design Was Sound**: Clean, tested, working architecture
2. **Framework Benefits**: Purpose-built for exactly this use case
3. **Reduced Complexity**: Let framework handle parsing complexity
4. **Faster Resolution**: Days vs weeks to fix
5. **Better Maintenance**: Framework team handles updates

### Protocol 23 Strategy:

1. **First**: Rollback to working code
2. **Then**: Verify CDP framework Protocol 23 support
3. **If Needed**: Minimal targeted updates
4. **Never**: Rebuild parsing from scratch

## Implementation Plan

### Week 1: Immediate Rollback
- [x] Analyze git history (completed)
- [ ] Rollback to commit 3de0840
- [ ] Update dependencies for Protocol 23
- [ ] Test basic functionality

### Week 2: Protocol 23 Validation
- [ ] Contact withObsrvr team about CDP Protocol 23 support
- [ ] Test with recent ledgers
- [ ] Compare output with stellar-live-source (RPC version)
- [ ] Document any gaps

### Week 3: Targeted Fixes (If Needed)
- [ ] Implement minimal Protocol 23 updates
- [ ] Comprehensive testing
- [ ] Performance validation

## Conclusion

The original `stellar-live-source-datalake` implementation was **architecturally correct** and working. Our Protocol 23 migration went wrong by **abandoning the proven CDP framework** and attempting to rebuild parsing logic from scratch.

**The fastest, safest path forward is to rollback to the working version and apply targeted Protocol 23 updates rather than continuing with the current over-engineered approach.**

This strategy reduces risk, accelerates delivery, and leverages proven, tested code rather than custom implementations that duplicate existing functionality.

## Files to Rollback

1. `stellar-live-source-datalake/go/server/server.go` - Back to CDP framework usage
2. `stellar-live-source-datalake/go/go.mod` - Back to working dependencies  
3. **Remove**: `stellar-live-source-datalake/go/server/galexie_datalake_reader.go` - Custom parser
4. **Remove**: All custom archive interface code

The key insight: **Working code > Perfect code**. The original implementation solved the problem correctly.