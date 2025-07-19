# Stellar Archive Data Retrieval Fix Plan

## Executive Summary

The current stellar-live-source-datalake implementation has critical issues in how it accesses Stellar history archive data. While the replacement of withObsrvr packages with stellar-go was successful, the implementation incorrectly attempts to manually handle archive file access instead of using stellar-go's built-in archive methods. This document outlines a comprehensive fix plan to resolve the data retrieval issues.

## Problem Analysis

### Current Implementation Issues

#### 1. **Incorrect Checkpoint Calculation**
```go
// Current (WRONG):
checkpoint := sequence - (sequence % 64)
if checkpoint == 0 {
    checkpoint = 64
}
```

**Problems:**
- Stellar checkpoints occur at ledgers 63, 127, 191, etc. (not 64, 128, 192)
- Formula doesn't account for Stellar's actual checkpoint system
- First checkpoint is special (contains ledgers 1-63, only 63 ledgers)

**Correct Approach:**
```go
// Use stellar-go's CheckpointManager
checkpoint := archive.GetCheckpointManager().GetCheckpoint(sequence)
```

#### 2. **Wrong Archive Path Structure**
```go
// Current (WRONG):
xdrPath := fmt.Sprintf("ledger/%08x/ledger-%08x.xdr.gz", checkpoint, checkpoint)
```

**Problems:**
- Uses full 8-digit hex for directory structure
- Doesn't match Stellar's actual archive hierarchy
- Results in paths like `ledger/00007d00/ledger-00007d00.xdr.gz`

**Correct Structure:**
```
ledger/ww/xx/yy/ledger-wwxxyyzz.xdr.gz
```
Where `ww/xx/yy` are the first three bytes of the checkpoint number.

#### 3. **Direct File Access Instead of Archive Methods**
```go
// Current (WRONG):
reader, err := sar.storage.GetFile(xdrPath)
xdrData, err := io.ReadAll(reader)
var lcm xdr.LedgerCloseMeta
_, err = xdr.Unmarshal(bytes.NewReader(xdrData), &lcm)
```

**Problems:**
- Bypasses stellar-go's built-in archive handling
- Manual XDR decompression and parsing
- No checkpoint boundary management
- Inefficient for processing multiple ledgers

**Correct Approach:**
```go
// Use stellar-go's built-in methods
ledgers, err := archive.GetLedgers(startSeq, endSeq)
// OR for individual ledgers:
header, err := archive.GetLedgerHeader(sequence)
```

#### 4. **Incorrect Archive URL Format**
```go
// Current (WRONG):
archiveURL := fmt.Sprintf("%s://%s", storageType, bucketName)
// Results in: "GCS://obsrvr-stellar-ledger-data-testnet-data"
```

**Problems:**
- Doesn't include path to actual archive location
- Incorrect URL scheme format
- Missing archive-specific path structure

**Correct Format:**
```go
archiveURL := "gcs://obsrvr-stellar-ledger-data-testnet-data/landing/ledgers/testnet"
```

### Root Cause

The implementation **manually recreates functionality that stellar-go already provides**, leading to:
- Incorrect archive navigation
- Manual XDR processing errors
- Inefficient data access patterns
- EOF errors due to malformed requests

## Solution Architecture

### Principle: Use stellar-go's Built-in Archive Methods

Instead of manually implementing archive file access, leverage stellar-go's `historyarchive` package which:
- ✅ Handles checkpoint calculations correctly
- ✅ Manages archive path structures automatically
- ✅ Provides efficient XDR decompression
- ✅ Supports batch processing
- ✅ Includes proper error handling

### Implementation Strategy

#### Phase 1: Archive Connection Fix
Replace manual storage backend creation with proper archive URL connection:

```go
// Before:
storageBackend, err := storage.NewGCSBackend(ctx, bucketName, "", endpoint)
archive, err := historyarchive.Connect(archiveURL, options)

// After:
archiveURL := "gcs://obsrvr-stellar-ledger-data-testnet-data/landing/ledgers/testnet"
archive, err := historyarchive.Connect(archiveURL, historyarchive.ArchiveOptions{
    ConnectOptions: storage.ConnectOptions{Context: ctx},
})
```

#### Phase 2: Replace Manual File Access
Replace direct file reading with stellar-go archive methods:

```go
// Before:
func (sar *StellarArchiveReader) GetLedgerCloseMeta(sequence uint32) (*xdr.LedgerCloseMeta, error) {
    checkpoint := sequence - (sequence % 64)
    xdrPath := fmt.Sprintf("ledger/%08x/ledger-%08x.xdr.gz", checkpoint, checkpoint)
    reader, err := sar.storage.GetFile(xdrPath)
    // ... manual XDR processing
}

// After:
func (sar *StellarArchiveReader) GetLedgerCloseMeta(sequence uint32) (*xdr.LedgerCloseMeta, error) {
    ledgers, err := sar.archive.GetLedgers(sequence, sequence)
    if err != nil {
        return nil, fmt.Errorf("failed to get ledger %d: %w", sequence, err)
    }
    
    ledger, exists := ledgers[sequence]
    if !exists {
        return nil, fmt.Errorf("ledger %d not found in checkpoint", sequence)
    }
    
    return convertToLedgerCloseMeta(ledger), nil
}
```

#### Phase 3: Efficient Batch Processing
Implement checkpoint-aware batch processing:

```go
func (sar *StellarArchiveReader) StreamLedgers(startSeq, endSeq uint32, output chan<- *xdr.LedgerCloseMeta) error {
    defer close(output)
    
    checkpointManager := sar.archive.GetCheckpointManager()
    
    for seq := startSeq; seq <= endSeq; {
        // Process in checkpoint-sized chunks
        checkpointRange := checkpointManager.GetCheckpointRange(seq)
        endRange := min(checkpointRange.High, endSeq)
        
        ledgers, err := sar.archive.GetLedgers(seq, endRange)
        if err != nil {
            return fmt.Errorf("failed to get ledgers %d-%d: %w", seq, endRange, err)
        }
        
        for i := seq; i <= endRange; i++ {
            if ledger, exists := ledgers[i]; exists {
                lcm := convertToLedgerCloseMeta(ledger)
                select {
                case output <- lcm:
                case <-time.After(5 * time.Second):
                    return fmt.Errorf("timeout sending ledger %d", i)
                }
            }
        }
        
        seq = endRange + 1
    }
    
    return nil
}
```

## Implementation Plan

### Phase 1: Core Architecture Fix (Week 1)

#### Step 1.1: Update Archive Connection
- [ ] Fix archive URL format to include full path
- [ ] Remove manual storage backend creation
- [ ] Use `historyarchive.Connect()` directly
- [ ] Test connection with corrected URL

#### Step 1.2: Create Conversion Functions
- [ ] Implement `convertToLedgerCloseMeta()` function
- [ ] Handle different LedgerCloseMeta versions (V0, V1, V2)
- [ ] Preserve transaction and result data
- [ ] Add proper error handling

#### Step 1.3: Update Configuration
- [ ] Fix `.secrets` file with correct archive paths
- [ ] Update environment variable documentation
- [ ] Add archive-specific configuration options

### Phase 2: Method Replacement (Week 1-2)

#### Step 2.1: Replace GetLedgerCloseMeta
- [ ] Remove manual checkpoint calculation
- [ ] Use `archive.GetLedgers()` method
- [ ] Implement proper error handling
- [ ] Add debug logging for troubleshooting

#### Step 2.2: Replace StreamLedgerCloseMeta
- [ ] Implement checkpoint-aware batch processing
- [ ] Use `CheckpointManager` for efficient ranges
- [ ] Add progress tracking and metrics
- [ ] Implement proper cancellation handling

#### Step 2.3: Remove Manual File Access
- [ ] Remove direct `storage.GetFile()` calls
- [ ] Remove manual XDR unmarshaling
- [ ] Simplify error handling
- [ ] Clean up unused code

### Phase 3: Testing and Validation (Week 2)

#### Step 3.1: Unit Testing
- [ ] Test checkpoint boundary handling
- [ ] Test individual ledger retrieval
- [ ] Test batch processing efficiency
- [ ] Test error scenarios

#### Step 3.2: Integration Testing
- [ ] Test with known working ranges (409907-409948)
- [ ] Test with previously failing ranges (501100+)
- [ ] Test with various checkpoint boundaries
- [ ] Test with different archive configurations

#### Step 3.3: Performance Validation
- [ ] Measure processing throughput
- [ ] Compare with previous implementation
- [ ] Validate memory usage
- [ ] Test with large ledger ranges

### Phase 4: Production Deployment (Week 2)

#### Step 4.1: Staged Rollout
- [ ] Deploy to development environment
- [ ] Run comprehensive tests
- [ ] Deploy to staging with production data
- [ ] Monitor for issues

#### Step 4.2: Production Validation
- [ ] Process known working ranges
- [ ] Process previously failing ranges
- [ ] Monitor system metrics
- [ ] Validate data integrity

## Expected Outcomes

### Technical Benefits
- ✅ **Correct checkpoint handling** using stellar-go's CheckpointManager
- ✅ **Automatic XDR decompression** without manual processing
- ✅ **Efficient batch processing** with checkpoint-aware algorithms
- ✅ **Proper error handling** with stellar-go's built-in validation
- ✅ **Support for all ledger ranges** including 501100+

### Performance Benefits
- ✅ **Reduced memory usage** through streaming processing
- ✅ **Better throughput** with checkpoint-based batching
- ✅ **Lower network overhead** with efficient archive access
- ✅ **Improved reliability** with proper error recovery

### Maintenance Benefits
- ✅ **Simplified codebase** by removing manual implementations
- ✅ **Better maintainability** using standard stellar-go patterns
- ✅ **Future-proof** with official stellar-go updates
- ✅ **Enhanced debugging** with stellar-go's built-in logging

## Implementation Details

### Updated Server Architecture

```go
type StellarArchiveReader struct {
    archive historyarchive.ArchiveInterface
    logger  *zap.Logger
}

func NewStellarArchiveReader(archiveURL string, logger *zap.Logger) (*StellarArchiveReader, error) {
    archive, err := historyarchive.Connect(archiveURL, historyarchive.ArchiveOptions{
        ConnectOptions: storage.ConnectOptions{
            Context: context.Background(),
        },
    })
    if err != nil {
        return nil, fmt.Errorf("failed to connect to archive: %w", err)
    }

    return &StellarArchiveReader{
        archive: archive,
        logger:  logger,
    }, nil
}
```

### LedgerCloseMeta Conversion

```go
func convertToLedgerCloseMeta(ledger *historyarchive.Ledger) *xdr.LedgerCloseMeta {
    return &xdr.LedgerCloseMeta{
        V: 1,
        V1: &xdr.LedgerCloseMetaV1{
            LedgerHeader: ledger.Header,
            TxSet: xdr.GeneralizedTransactionSet{
                V: 1,
                V1TxSet: &xdr.TransactionSetV1{
                    PreviousLedgerHash: ledger.Header.Header.PreviousLedgerHash,
                    Txs: extractTransactions(ledger.Transaction),
                },
            },
            TxProcessing: extractTransactionResults(ledger.TransactionResult),
        },
    }
}
```

### Configuration Updates

```env
# stellar-live-source-datalake/.secrets
export ARCHIVE_URL="gcs://obsrvr-stellar-ledger-data-testnet-data/landing/ledgers/testnet"
export STORAGE_TYPE=GCS
export GOOGLE_APPLICATION_CREDENTIALS=$HOME/.config/gcloud/application_default_credentials.json
export ENABLE_FLOWCTL=true
export FLOWCTL_ENDPOINT=localhost:8080
export ENABLE_UNIFIED_EVENTS=true
```

## Risk Mitigation

### Technical Risks
| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Archive URL format issues | Medium | High | Comprehensive testing with various URLs |
| Data conversion errors | Low | High | Extensive unit tests for conversion functions |
| Performance regression | Low | Medium | Benchmark testing and monitoring |
| Backward compatibility | Low | Medium | Gradual rollout with rollback capability |

### Business Risks
| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Service downtime | Low | High | Staged deployment with health checks |
| Data integrity issues | Low | High | Validation testing with known datasets |
| Extended implementation time | Medium | Medium | Phased approach with clear milestones |

## Success Metrics

### Technical Success
- ✅ Process ledger ranges 501100+ without EOF errors
- ✅ Maintain processing throughput within 10% of current performance
- ✅ Reduce memory usage by leveraging streaming processing
- ✅ All existing APIs remain functional

### Business Success
- ✅ Zero data loss during migration
- ✅ Successful processing of all Protocol 23 ledger ranges
- ✅ Improved system reliability and stability
- ✅ Enhanced debugging and monitoring capabilities

## Timeline

### Week 1: Core Implementation
- **Day 1-2**: Fix archive connection and URL format
- **Day 3-4**: Implement LedgerCloseMeta conversion
- **Day 5**: Replace GetLedgerCloseMeta method

### Week 2: Completion and Testing
- **Day 1-2**: Replace StreamLedgerCloseMeta method
- **Day 3-4**: Comprehensive testing and validation
- **Day 5**: Production deployment and monitoring

## Conclusion

This fix plan addresses the root cause of the data retrieval issues by properly leveraging stellar-go's built-in archive functionality. The implementation will:

1. **Eliminate manual archive file handling** in favor of stellar-go's proven methods
2. **Provide correct checkpoint management** using CheckpointManager
3. **Enable efficient batch processing** with checkpoint-aware algorithms
4. **Support all ledger ranges** including those that previously failed

The result will be a robust, maintainable, and efficient stellar archive data retrieval system that properly handles Protocol 23 requirements and can scale to future protocol updates.