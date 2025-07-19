# Stellar-Live-Source-Datalake Analysis & Protocol 23 Compliance

## Executive Summary

This analysis reveals that `stellar-live-source-datalake` has fundamental architectural issues that prevent Protocol 23 compliance. The implementation incorrectly attempts to use Stellar's `historyarchive` interface for a custom Galexie datalake format and creates incomplete `LedgerCloseMeta` structures missing critical Protocol 23 features.

## Critical Issues Identified

### 1. Fundamental Architecture Problem

**Issue**: Mixing incompatible archive formats

**Current Implementation**:
```go
// server.go lines 182-184 - WRONG APPROACH
ledgers, err := sar.archive.GetLedgers(sequence, sequence)
if err != nil {
    // Falls back to custom Galexie reader
    return sar.galexieReader.GetLedgerCloseMeta(sequence)
}
```

**Problem**: 
- Stellar's `historyarchive.GetLedgers()` expects standard checkpoint-based archives
- Galexie datalake uses completely different format (partition-based, zstd compressed)
- This hybrid approach leads to incomplete data structures

**Stellar Standard**:
```go
// From stellar/go/historyarchive/archive.go
func (a *Archive) GetLedgers(start, end uint32) ([]io.ReadCloser, error) {
    // Expects checkpoint structure: 64-ledger boundaries
    // Returns separate XDR streams for ledgers, transactions, results
}
```

### 2. Incomplete LedgerCloseMeta Construction

**Critical Gap**: Missing Protocol 23 eviction data

**Current Implementation** (`server.go:273-299`):
```go
lcm := &xdr.LedgerCloseMeta{
    V: 1,
    V1: &xdr.LedgerCloseMetaV1{
        LedgerHeader: ledger.Header,
        TxSet: xdr.GeneralizedTransactionSet{
            V: 1,
            V1TxSet: &xdr.TransactionSetV1{
                // Empty transaction set!
                Phases: []xdr.TransactionPhase{},
            },
        },
        TxProcessing: []xdr.TransactionResultMeta{}, // Empty!
        // Missing Protocol 23 fields entirely
    },
}
```

**Missing Protocol 23 Fields**:
```go
// Required for Protocol 23 (from stellar/go/xdr/ledger_close_meta.go)
type LedgerCloseMetaV1 struct {
    // ... existing fields ...
    
    // Protocol 23 additions:
    EvictedTemporaryLedgerKeys     []LedgerKey   `json:"evictedTemporaryLedgerKeys"`
    EvictedPersistentLedgerEntries []LedgerEntry `json:"evictedPersistentLedgerEntries"`
}
```

### 3. Wrong TransactionMeta Version Support

**Issue**: Protocol 23 requires TransactionMeta V3 for Soroban features

**Current Code**: Creates empty transaction processing arrays
**Required**: Must support TransactionMeta V3 structure

**From Stellar Go** (`xdr/transaction_meta.go`):
```go
const (
    TransactionMetaVersionV0 = 0
    TransactionMetaVersionV1 = 1
    TransactionMetaVersionV2 = 2
    TransactionMetaVersionV3 = 3  // Required for Protocol 23
)
```

### 4. Galexie Format Parsing Issues

**Current Implementation** (`galexie_datalake_reader.go:237-248`):
```go
// Hardcoded header skip - format-specific hack
if len(data) < 12 {
    return nil, fmt.Errorf("decompressed data too short: %d bytes", len(data))
}
xdrData := data[12:] // Skip custom 12-byte header

// Creates minimal structure
var lcm xdr.LedgerCloseMeta
lcm.V = 1
_, err = xdr.Unmarshal(dataReader, &lcm.V1)
```

**Problems**:
1. **Assumes only ledger header data** in galexie files
2. **Missing transaction and result data** that should be in complete ledger files
3. **No support for Protocol 23 eviction data**

## Comparison with Stellar Standards

### How Stellar Handles Ledger Data

**From `stellar/go/ingest/ledger_transaction_reader.go`**:
```go
type LedgerTransactionReader struct {
    ledgerReader     *LedgerReader
    transactionReader *TransactionReader
    resultReader      *ResultReader
}

// Proper way to build complete LedgerCloseMeta
func (reader *LedgerTransactionReader) Read() (xdr.LedgerCloseMeta, error) {
    // Combines all three data streams
    // Includes full transaction processing info
    // Supports all protocol versions including eviction
}
```

### Stellar's Archive Interface

**Correct Pattern** (`stellar/go/historyarchive/archive.go`):
```go
type ArchiveInterface interface {
    GetPathHAS(path string) (HistoryArchiveState, error)
    PutPathHAS(path string, has HistoryArchiveState) error
    GetLedgerHeader(ledgerSeq uint32) (xdr.LedgerHeader, error)
    // ... proper checkpoint-based methods
}
```

**Our Wrong Approach**: Trying to force Galexie format into this interface

## Protocol 23 Specific Requirements

### 1. Eviction Support
Protocol 23 introduced eviction of temporary and persistent entries:

```go
// Required in LedgerCloseMetaV1
EvictedTemporaryLedgerKeys     []LedgerKey
EvictedPersistentLedgerEntries []LedgerEntry
```

### 2. Enhanced TransactionMeta
```go
// TransactionMeta V3 includes extended operation results
type TransactionMetaV3 struct {
    Ext               ExtensionPoint
    TxChangesBefore   []LedgerEntryChange
    Operations        []OperationMeta
    TxChangesAfter    []LedgerEntryChange
    SorobanMeta       *SorobanTransactionMeta  // Protocol 23
    DiagnosticEvents  []DiagnosticEvent        // Protocol 23
}
```

### 3. Soroban State Changes
Protocol 23 includes Soroban (smart contract) state changes that must be tracked.

## Recommended Fix Strategy

### Phase 1: Separate Archive Implementations

**Create dedicated Galexie interface**:
```go
// New file: galexie_archive_interface.go
type GalexieArchiveInterface interface {
    GetLedgerCloseMeta(sequence uint32) (*xdr.LedgerCloseMeta, error)
    GetLedgerRange(start, end uint32) ([]*xdr.LedgerCloseMeta, error)
    Close() error
}

type GalexieDatalakeArchive struct {
    reader *GalexieDatalakeReader
}
```

**Remove historyarchive inheritance**:
```go
// server.go - REMOVE this hybrid approach
type StellarArchiveReader struct {
    archive stellar.ArchiveInterface  // REMOVE
    galexieReader *GalexieDatalakeReader  // Keep only this
}
```

### Phase 2: Complete Galexie Format Parsing

**Enhanced galexie_datalake_reader.go**:
```go
type GalexieDataStructure struct {
    Header      []byte  // 8-byte custom header
    Version     uint32  // 4-byte version
    LedgerData  *xdr.LedgerHeader
    TxSetData   *xdr.GeneralizedTransactionSet
    ResultsData []xdr.TransactionResultMeta
    EvictionData *ProtocolEvictionData  // Protocol 23
}

func (gdr *GalexieDatalakeReader) parseGalexieFormat(data []byte) (*GalexieDataStructure, error) {
    // Parse complete galexie structure, not just header
    // Extract all components needed for LedgerCloseMeta
}
```

### Phase 3: Protocol 23 Compliance

**Complete LedgerCloseMeta construction**:
```go
func (gdr *GalexieDatalakeReader) GetLedgerCloseMeta(sequence uint32) (*xdr.LedgerCloseMeta, error) {
    galexieData, err := gdr.parseGalexieFormat(rawData)
    if err != nil {
        return nil, err
    }
    
    lcm := &xdr.LedgerCloseMeta{
        V: 1,
        V1: &xdr.LedgerCloseMetaV1{
            LedgerHeader:       galexieData.LedgerData,
            TxSet:             *galexieData.TxSetData,
            TxProcessing:      galexieData.ResultsData,
            UpgradesProcessing: galexieData.UpgradeData,
            ScpInfo:           galexieData.ScpData,
            
            // Protocol 23 fields
            EvictedTemporaryLedgerKeys:     galexieData.EvictionData.TempKeys,
            EvictedPersistentLedgerEntries: galexieData.EvictionData.PersistentEntries,
        },
    }
    
    return lcm, nil
}
```

### Phase 4: Error Handling Alignment

**Use Stellar error patterns**:
```go
import (
    "github.com/stellar/go/ingest"
    "github.com/stellar/go/support/errors"
)

func (gdr *GalexieDatalakeReader) GetLedgerCloseMeta(sequence uint32) (*xdr.LedgerCloseMeta, error) {
    if notFound {
        return nil, ingest.ErrNotFound
    }
    
    if parseError {
        return nil, errors.Wrap(err, "failed to parse galexie ledger data")
    }
}
```

## File-by-File Changes Required

### 1. `server/server.go`
**Lines to Fix**:
- **Lines 182-184**: Remove `historyarchive.GetLedgers()` call
- **Lines 273-299**: Complete `convertToLedgerCloseMeta()` implementation
- **Lines 85-90**: Update `StellarArchiveReader` struct

**Key Changes**:
```go
// REMOVE hybrid archive approach
type StellarArchiveReader struct {
    galexieReader *GalexieDatalakeReader  // Only keep this
}

// FIX: Complete LedgerCloseMeta construction
func (sar *StellarArchiveReader) convertToLedgerCloseMeta(ledger stellar.Ledger) (*xdr.LedgerCloseMeta, error) {
    // Must parse complete galexie data, not just header
    return sar.galexieReader.GetCompleteLedgerCloseMeta(ledger.Sequence)
}
```

### 2. `server/galexie_datalake_reader.go`
**Major Rewrite Required**:

**Current Lines 237-248** (incomplete parsing):
```go
// WRONG: Only parses partial data
xdrData := data[12:]
var lcm xdr.LedgerCloseMeta
lcm.V = 1
_, err = xdr.Unmarshal(dataReader, &lcm.V1)
```

**Required Fix**:
```go
// RIGHT: Parse complete galexie structure
func (gdr *GalexieDatalakeReader) parseCompleteGalexieData(data []byte) (*xdr.LedgerCloseMeta, error) {
    // 1. Parse 8-byte header
    // 2. Parse version
    // 3. Parse ledger header
    // 4. Parse transaction set
    // 5. Parse transaction results
    // 6. Parse Protocol 23 eviction data
    // 7. Combine into complete LedgerCloseMeta
}
```

### 3. Create New Files

**`galexie_archive_interface.go`**:
- Define proper Galexie-specific interface
- Remove dependency on `historyarchive` patterns

**`protocol_23_support.go`**:
- Handle Protocol 23 specific features
- Eviction data structures
- TransactionMeta V3 support

## Testing Strategy

### 1. Protocol Version Testing
```go
func TestProtocol23Support(t *testing.T) {
    // Test with actual Protocol 23 ledgers
    // Verify eviction data is present
    // Check TransactionMeta V3 support
}
```

### 2. Galexie Format Validation
```go
func TestCompleteGalexieParsing(t *testing.T) {
    // Verify all galexie data components are extracted
    // Compare with stellar-live-source (RPC) output
    // Ensure identical LedgerCloseMeta structures
}
```

## Success Criteria

1. **Complete Data**: LedgerCloseMeta contains all transaction and result data
2. **Protocol 23 Support**: Eviction data properly extracted and included
3. **Format Independence**: No more hybrid historyarchive/galexie approach
4. **Stellar Compatibility**: Error handling and patterns match stellar/go standards
5. **Performance**: Efficient parsing of galexie format without memory leaks

## Implementation Priority

1. **High Priority**: Remove historyarchive hybrid approach (Phase 1)
2. **High Priority**: Complete galexie format parsing (Phase 2)
3. **Critical**: Add Protocol 23 eviction support (Phase 3)
4. **Medium Priority**: Align error handling patterns (Phase 4)

## Conclusion

The current `stellar-live-source-datalake` implementation has fundamental architectural flaws that prevent Protocol 23 compliance. The mix of incompatible archive formats and incomplete data structures must be addressed through a complete refactoring that properly parses the Galexie datalake format and constructs complete `LedgerCloseMeta` structures with Protocol 23 features.

The working `stellar-live-source` (RPC-based) should be used as the reference implementation for proper Protocol 23 support, while the datalake version requires significant architectural changes to achieve the same level of compliance.