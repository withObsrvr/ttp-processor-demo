# Stellar Live Source RPC Improvements

## Executive Summary

After analyzing the `cdp-pipeline-workflow` source adapter (production-grade reference implementation), several key improvements can be made to `stellar-live-source` to raise it to the same level as `stellar-live-source-datalake`.

**Current Status:**
- ✅ stellar-live-source already has many enterprise features (circuit breaker, metrics, flowctl integration)
- ❌ Missing critical features for production reliability and flexibility
- 🎯 Gap analysis shows 4 key missing features

---

## Comparison Matrix

| Feature | stellar-live-source | cdp-pipeline-workflow | Priority |
|---------|---------------------|----------------------|----------|
| Circuit Breaker | ✅ Implemented | ✅ Implemented | ✅ Done |
| Enterprise Metrics | ✅ Implemented | ✅ Implemented | ✅ Done |
| Retry with Backoff | ✅ Implemented | ✅ Implemented | ✅ Done |
| Health Check Endpoint | ✅ Implemented | ✅ Implemented | ✅ Done |
| Flowctl/Console Heartbeat | ✅ Implemented | ✅ Implemented | ✅ Done |
| Historical Cache | ✅ Implemented | ❌ Not present | ✅ Better |
| Predictive Prefetch | ✅ Implemented | ❌ Not present | ✅ Better |
| **Checkpointing** | ❌ Missing | ✅ Implemented | 🔴 **CRITICAL** |
| **GetLedgerRange API** | ❌ Missing | ✅ Implemented | 🔴 **CRITICAL** |
| **Streaming vs Batch Mode** | ❌ Missing | ✅ Implemented | 🟡 Important |
| **Range Semantics** | ❌ Missing | ✅ Implemented | 🟡 Important |

---

## Critical Missing Features

### 1. Checkpointing for Crash Recovery 🔴

**Problem:** stellar-live-source has no mechanism to save processing state. If the service crashes or restarts, it must rely on the client to track where it left off.

**cdp-pipeline-workflow Implementation:**

```go
// source_adapter_rpc.go:619-675
type Checkpoint struct {
    LastProcessedLedger uint64    `json:"last_processed_ledger"`
    Timestamp           time.Time `json:"timestamp"`
    TotalProcessed      uint64    `json:"total_processed"`
    ErrorCount          uint64    `json:"error_count"`
    LastError           string    `json:"last_error,omitempty"`
}

func (s *SorobanSourceAdapter) SaveCheckpoint() error {
    checkpoint := Checkpoint{
        LastProcessedLedger: s.lastProcessedLedger,
        Timestamp:           time.Now(),
        TotalProcessed:      s.totalProcessed,
        ErrorCount:          s.errorCount,
    }

    data, err := json.Marshal(checkpoint)
    if err != nil {
        return err
    }

    return os.WriteFile(s.checkpointPath, data, 0644)
}

func (s *SorobanSourceAdapter) LoadCheckpoint() error {
    data, err := os.ReadFile(s.checkpointPath)
    if err != nil {
        if os.IsNotExist(err) {
            return nil // No checkpoint exists yet
        }
        return err
    }

    var checkpoint Checkpoint
    if err := json.Unmarshal(data, &checkpoint); err != nil {
        return err
    }

    s.lastProcessedLedger = checkpoint.LastProcessedLedger
    s.totalProcessed = checkpoint.TotalProcessed
    return nil
}
```

**Recommendation for stellar-live-source:**

1. Add checkpoint file path to `Config`:
```go
type Config struct {
    // ... existing fields ...
    CheckpointPath string // Path to checkpoint file (default: ./checkpoints/stellar-live-source.json)
    CheckpointInterval time.Duration // How often to save checkpoint (default: 30s)
}
```

2. Add checkpoint struct and methods to `RawLedgerServer`:
```go
type Checkpoint struct {
    LastProcessedLedger uint32    `json:"last_processed_ledger"`
    Timestamp           time.Time `json:"timestamp"`
    TotalProcessed      int64     `json:"total_processed"`
    TotalBytesProcessed int64     `json:"total_bytes_processed"`
    ErrorCount          int       `json:"error_count"`
    LastError           string    `json:"last_error,omitempty"`
}

func (s *RawLedgerServer) SaveCheckpoint(lastSeq uint32) error
func (s *RawLedgerServer) LoadCheckpoint() (*Checkpoint, error)
```

3. Integrate into streaming loop:
```go
// In StreamRawLedgers, after processing ledgers:
if time.Since(lastCheckpoint) >= s.config.CheckpointInterval {
    if err := s.SaveCheckpoint(lastProcessedSeq); err != nil {
        s.logger.Warn("Failed to save checkpoint", zap.Error(err))
    }
    lastCheckpoint = time.Now()
}
```

**Benefits:**
- ✅ Automatic crash recovery
- ✅ Resume from last processed ledger
- ✅ Reduced data loss on restart
- ✅ Better observability (can inspect checkpoint file)

---

### 2. GetLedgerRange Batch API 🔴

**Problem:** stellar-live-source only supports streaming mode (`StreamRawLedgers`). For batch processing or backfill scenarios, a range-based API is more efficient and easier to use.

**cdp-pipeline-workflow Implementation:**

```go
// source_adapter_rpc.go:398-462
func (s *SorobanSourceAdapter) GetLedgers(ctx context.Context, from uint64, to uint64) (*LedgersResponse, error) {
    // Range semantics: [from, to) - from inclusive, to exclusive
    limit := to - from

    req := methods.GetLedgersRequest{
        StartLedger: from,
        Pagination: &methods.LedgerPaginationOptions{
            Limit: uint(limit),
        },
    }

    resp, err := s.client.GetLedgers(ctx, req)
    if err != nil {
        return nil, fmt.Errorf("failed to get ledgers [%d, %d): %w", from, to, err)
    }

    ledgers := make([]*Ledger, 0, len(resp.Ledgers))
    for _, ledgerInfo := range resp.Ledgers {
        // Process each ledger...
        ledgers = append(ledgers, &Ledger{
            Sequence: uint64(ledgerInfo.Sequence),
            Data:     rawXdrBytes,
        })
    }

    return &LedgersResponse{
        Ledgers:      ledgers,
        LatestLedger: uint64(resp.LatestLedger),
    }, nil
}
```

**Recommendation for stellar-live-source:**

1. Add new gRPC service method to `raw_ledger_service.proto`:
```protobuf
service RawLedgerService {
    // Existing streaming method
    rpc StreamRawLedgers(StreamLedgersRequest) returns (stream RawLedger);

    // NEW: Batch range method
    rpc GetLedgerRange(GetLedgerRangeRequest) returns (GetLedgerRangeResponse);
}

message GetLedgerRangeRequest {
    uint32 start_ledger = 1;  // Inclusive
    uint32 end_ledger = 2;    // Exclusive (end - start = number of ledgers)
}

message GetLedgerRangeResponse {
    repeated RawLedger ledgers = 1;
    uint32 latest_ledger = 2;  // Current network latest
}
```

2. Implement server method:
```go
func (s *RawLedgerServer) GetLedgerRange(ctx context.Context, req *rawledger.GetLedgerRangeRequest) (*rawledger.GetLedgerRangeResponse, error) {
    // Range semantics: [start_ledger, end_ledger) - start inclusive, end exclusive
    if req.EndLedger <= req.StartLedger {
        return nil, status.Error(codes.InvalidArgument, "end_ledger must be greater than start_ledger")
    }

    limit := req.EndLedger - req.StartLedger
    if limit > s.config.MaxBatchSize {
        return nil, status.Errorf(codes.InvalidArgument, "range too large: %d (max: %d)", limit, s.config.MaxBatchSize)
    }

    // Build request
    getLedgersReq := protocol.GetLedgersRequest{
        StartLedger: req.StartLedger,
        Pagination: &protocol.LedgerPaginationOptions{
            Limit: uint(limit),
        },
    }

    // Fetch ledgers from RPC
    resp, err := s.rpcClient.GetLedgers(ctx, getLedgersReq)
    if err != nil {
        return nil, status.Errorf(codes.Internal, "failed to get ledgers: %v", err)
    }

    // Process and return
    ledgers := make([]*rawledger.RawLedger, 0, len(resp.Ledgers))
    for _, ledgerInfo := range resp.Ledgers {
        rawXdrBytes, err := base64.StdEncoding.DecodeString(ledgerInfo.LedgerMetadata)
        if err != nil {
            return nil, status.Errorf(codes.Internal, "failed to decode ledger %d: %v", ledgerInfo.Sequence, err)
        }

        ledgers = append(ledgers, &rawledger.RawLedger{
            Sequence:           ledgerInfo.Sequence,
            LedgerCloseMetaXdr: rawXdrBytes,
        })
    }

    return &rawledger.GetLedgerRangeResponse{
        Ledgers:      ledgers,
        LatestLedger: resp.LatestLedger,
    }, nil
}
```

3. Add config for max batch size:
```go
type Config struct {
    // ... existing fields ...
    MaxBatchSize int // Maximum ledgers per GetLedgerRange call (default: 1000)
}
```

**Benefits:**
- ✅ Efficient batch processing for backfill
- ✅ Simpler client code for known ranges
- ✅ Better control over resource usage
- ✅ Compatible with existing streaming API

**Use Cases:**
```go
// Backfill scenario: Process specific range
resp, err := client.GetLedgerRange(ctx, &rawledger.GetLedgerRangeRequest{
    StartLedger: 1000000,
    EndLedger:   1001000,  // Get exactly 1000 ledgers
})

// vs streaming scenario: Continuous processing
stream, err := client.StreamRawLedgers(ctx, &rawledger.StreamLedgersRequest{
    StartLedger: 1001000,  // Continue from where batch left off
})
```

---

### 3. Streaming vs Batch Mode Detection 🟡

**Problem:** stellar-live-source doesn't automatically detect whether it should be in streaming or batch mode based on the request parameters.

**cdp-pipeline-workflow Implementation:**

```go
// source_adapter_rpc.go:937-970
func (s *SorobanSourceAdapter) isStreamingMode() bool {
    // If no end_ledger is specified, we're in streaming mode
    _, hasEndLedger := s.config.Extra["end_ledger"]
    return !hasEndLedger
}

func (s *SorobanSourceAdapter) Start(ctx context.Context) error {
    if s.isStreamingMode() {
        s.logger.Info("Starting in STREAMING mode (no end_ledger specified)")
        return s.streamLedgers(ctx)
    } else {
        s.logger.Info("Starting in BATCH mode (end_ledger specified)",
            zap.Uint64("end_ledger", s.endLedger),
        )
        return s.batchProcess(ctx)
    }
}
```

**Recommendation for stellar-live-source:**

This is less critical for stellar-live-source since it's primarily a streaming service. However, with the addition of `GetLedgerRange`, clients can explicitly choose the mode they want.

**Alternative:** Document usage patterns:
```markdown
# Usage Patterns

## Streaming Mode (Real-time ingestion)
Use `StreamRawLedgers` when you need continuous, real-time ledger data:
- Latest network data
- Automatic polling for new ledgers
- Long-running connections

## Batch Mode (Backfill/Historical)
Use `GetLedgerRange` when you need specific ledger ranges:
- Historical data backfill
- Known ranges to process
- Finite processing jobs
```

---

### 4. Proper Range Semantics 🟡

**Problem:** stellar-live-source uses cursor-based pagination which is great for streaming but doesn't provide clear range semantics for batch processing.

**cdp-pipeline-workflow Range Semantics:**

```go
// [from, to) - from INCLUSIVE, to EXCLUSIVE
// This matches standard slice/range semantics in most languages

GetLedgers(1000, 1100)  // Returns ledgers 1000-1099 (exactly 100 ledgers)
GetLedgers(1100, 1200)  // Returns ledgers 1100-1199 (next 100 ledgers, no gap or overlap)
```

**Recommendation:**

Already addressed by the proposed `GetLedgerRange` API above, which uses `[start_ledger, end_ledger)` semantics.

---

## Implementation Priority

### Phase 1: Critical (Week 1) 🔴

1. **Checkpointing** - Server/checkpoint.go
   - Implement checkpoint save/load logic
   - Add config options
   - Integrate into streaming loop
   - Add checkpoint health endpoint

2. **GetLedgerRange API** - Update protos and server
   - Update `raw_ledger_service.proto`
   - Regenerate protobuf code
   - Implement `GetLedgerRange` method
   - Add max batch size config

### Phase 2: Important (Week 2) 🟡

3. **Documentation** - Usage guides
   - Document streaming vs batch patterns
   - Add example client code
   - Update README with new features

4. **Testing** - Integration tests
   - Test checkpoint recovery
   - Test GetLedgerRange edge cases
   - Test range semantics

---

## Code Structure Proposal

```
stellar-live-source/go/
├── server/
│   ├── server.go           # Main server (existing)
│   ├── config.go           # Config (existing)
│   ├── metrics.go          # Metrics (existing)
│   ├── flowctl.go          # Flowctl integration (existing)
│   ├── cache.go            # Historical cache (existing)
│   ├── checkpoint.go       # NEW: Checkpointing logic
│   └── range_handler.go    # NEW: GetLedgerRange implementation
├── protos/
│   └── raw_ledger_service.proto  # Updated with GetLedgerRange
└── gen/
    └── raw_ledger_service/       # Regenerated protobuf code
```

---

## Configuration Changes

### Updated Config Struct

```go
type Config struct {
    // ... existing fields ...

    // Checkpointing
    CheckpointPath     string        `env:"CHECKPOINT_PATH" default:"./checkpoints/stellar-live-source.json"`
    CheckpointInterval time.Duration `env:"CHECKPOINT_INTERVAL" default:"30s"`

    // Batch API limits
    MaxBatchSize int `env:"MAX_BATCH_SIZE" default:"1000"`
}
```

### Environment Variables

```bash
# Checkpointing
CHECKPOINT_PATH=./checkpoints/stellar-live-source.json
CHECKPOINT_INTERVAL=30s

# Batch API
MAX_BATCH_SIZE=1000
```

---

## Testing Strategy

### Checkpoint Testing

```go
func TestCheckpointSaveLoad(t *testing.T) {
    // Create temp checkpoint file
    tmpfile, _ := os.CreateTemp("", "checkpoint-*.json")
    defer os.Remove(tmpfile.Name())

    server := NewRawLedgerServer("http://localhost:8000")
    server.config.CheckpointPath = tmpfile.Name()

    // Save checkpoint
    err := server.SaveCheckpoint(12345)
    assert.NoError(t, err)

    // Load checkpoint
    checkpoint, err := server.LoadCheckpoint()
    assert.NoError(t, err)
    assert.Equal(t, uint32(12345), checkpoint.LastProcessedLedger)
}

func TestCheckpointRecovery(t *testing.T) {
    // Simulate crash scenario
    // 1. Start stream, process some ledgers
    // 2. Save checkpoint
    // 3. Restart server
    // 4. Verify it resumes from checkpoint
}
```

### Range API Testing

```go
func TestGetLedgerRange(t *testing.T) {
    server := setupTestServer(t)

    // Test normal range
    resp, err := server.GetLedgerRange(context.Background(), &rawledger.GetLedgerRangeRequest{
        StartLedger: 1000,
        EndLedger:   1100,
    })
    assert.NoError(t, err)
    assert.Len(t, resp.Ledgers, 100)
    assert.Equal(t, uint32(1000), resp.Ledgers[0].Sequence)
    assert.Equal(t, uint32(1099), resp.Ledgers[99].Sequence)

    // Test invalid range (end <= start)
    _, err = server.GetLedgerRange(context.Background(), &rawledger.GetLedgerRangeRequest{
        StartLedger: 1000,
        EndLedger:   1000,
    })
    assert.Error(t, err)

    // Test range too large
    _, err = server.GetLedgerRange(context.Background(), &rawledger.GetLedgerRangeRequest{
        StartLedger: 1000,
        EndLedger:   1000 + server.config.MaxBatchSize + 1,
    })
    assert.Error(t, err)
}
```

---

## Migration Path

### For Existing Deployments

1. **No breaking changes** - All existing streaming clients continue to work
2. **Optional checkpointing** - Enable with `CHECKPOINT_PATH` env var
3. **New API is additive** - `GetLedgerRange` is a new method, doesn't affect `StreamRawLedgers`

### Rollout Strategy

```
Week 1: Implement checkpointing
  - Add checkpoint.go
  - Update Config
  - Integrate into StreamRawLedgers
  - Test with existing deployments

Week 2: Implement GetLedgerRange
  - Update protos
  - Implement range handler
  - Add batch size limits
  - Integration tests

Week 3: Documentation & examples
  - Update README
  - Add client examples
  - Write migration guide
```

---

## Performance Considerations

### Checkpointing Overhead

- **Write frequency:** Default 30s interval
- **File size:** ~500 bytes JSON file
- **I/O impact:** Minimal (async write recommended)

**Optimization:**
```go
// Async checkpoint saving to avoid blocking stream
go func() {
    if err := s.SaveCheckpoint(lastSeq); err != nil {
        s.logger.Warn("Failed to save checkpoint", zap.Error(err))
    }
}()
```

### GetLedgerRange Limits

- **Default max:** 1000 ledgers per request
- **Rationale:** Balance between throughput and memory usage
- **Tuneable:** via `MAX_BATCH_SIZE` env var

**Memory estimate:**
```
1000 ledgers × ~100KB/ledger = ~100MB per request
```

---

## Comparison Summary

### What stellar-live-source Does Better

1. ✅ **Historical cache** - cdp-pipeline-workflow doesn't have caching
2. ✅ **Predictive prefetch** - Smart access pattern prediction
3. ✅ **Data source metrics** - Tracks local vs historical vs hybrid
4. ✅ **Latency tracking** - P99 latency histograms

### What cdp-pipeline-workflow Does Better

1. ✅ **Checkpointing** - Automatic crash recovery
2. ✅ **Range API** - Efficient batch processing
3. ✅ **Clear semantics** - [from, to) range convention

### After Implementation

stellar-live-source will have **all features from both implementations**, making it the most complete Stellar RPC adapter available.

---

## Success Metrics

### Before Implementation

- Streaming API only
- No crash recovery
- Manual client state tracking

### After Implementation

- ✅ Streaming AND batch APIs
- ✅ Automatic checkpoint recovery
- ✅ Server-side state persistence
- ✅ Faster backfill with GetLedgerRange
- ✅ Enterprise-grade reliability

---

## References

### Files Analyzed

1. **cdp-pipeline-workflow:**
   - `/home/tillman/Documents/cdp-pipeline-workflow/source_adapter_rpc.go` (978 lines)
   - Checkpointing: lines 619-675
   - GetLedgers: lines 398-462
   - Circuit breaker: lines 76-136
   - Console heartbeat: lines 256-274

2. **stellar-live-source:**
   - `/home/tillman/Documents/ttp-processor-demo/stellar-live-source/go/server/server.go` (696 lines)
   - `/home/tillman/Documents/ttp-processor-demo/stellar-live-source/go/server/config.go` (167 lines)
   - `/home/tillman/Documents/ttp-processor-demo/stellar-live-source/go/server/flowctl.go` (179 lines)
   - `/home/tillman/Documents/ttp-processor-demo/stellar-live-source/go/server/metrics.go` (152 lines)

---

## Conclusion

By implementing **checkpointing** and **GetLedgerRange**, stellar-live-source will reach feature parity with cdp-pipeline-workflow while maintaining its superior caching and prefetch capabilities.

**Recommendation:** Prioritize checkpointing (critical for production reliability) first, then add GetLedgerRange for batch processing support.

**Timeline:** 2-3 weeks to full implementation and testing.
