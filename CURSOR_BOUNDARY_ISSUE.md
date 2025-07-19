# Stellar RPC Cursor Boundary Issue Analysis & Solutions

## Problem Description

When polling the Stellar RPC endpoint at high frequency (1-3 seconds), the `stellar-live-source` service encounters a cursor validation error that triggers the circuit breaker:

```
[-32602] cursor ('522859') must be between the oldest ledger: 401900 and the latest ledger: 522859 for this rpc instance
```

### Root Cause Analysis

1. **Cursor Mechanics**: The service uses cursor-based pagination to track position in the ledger stream
2. **Boundary Condition**: When polling frequency is high, the cursor often points to the very latest available ledger
3. **RPC Validation**: Stellar RPC validates that cursors fall within the available ledger range
4. **Edge Case**: When cursor equals `latest_ledger`, the next request may be rejected if no newer ledgers exist yet

### Why This Happens More with Fast Polling

- **5-second polling**: Usually catches ledgers after they're well-established
- **2-second polling**: Often requests the "cutting edge" where new ledgers may not be immediately available
- **Timing issue**: Stellar ledgers close every ~5-6 seconds, so 2-second polling hits boundary conditions more frequently

## Current Circuit Breaker Behavior

```go
// Current configuration
circuitBreaker := NewCircuitBreaker(5, 30*time.Second)
```

- **5 failures** → Circuit opens for **30 seconds**
- With 2-second polling, cursor boundary errors accumulate quickly
- Results in service unavailability despite RPC server being healthy

## Proposed Solutions

### Solution 1: Intelligent Cursor Boundary Handling ⭐ **RECOMMENDED**

**Approach**: Detect cursor boundary errors and handle them gracefully without triggering circuit breaker.

**Implementation**:

```go
// Add to server.go after line 252
if err != nil {
    // Check if this is a cursor boundary error (not a real failure)
    if isCursorBoundaryError(err) {
        s.logger.Debug("Cursor at boundary, resetting to latest",
            zap.String("cursor", getLedgersReq.Pagination.Cursor),
            zap.Error(err),
        )
        
        // Reset cursor and continue without recording failure
        getLedgersReq.Pagination.Cursor = ""
        getLedgersReq.StartLedger = resp.LatestLedger
        time.Sleep(2 * time.Second) // Wait for new ledgers
        continue
    }
    
    // Only record actual failures
    s.metrics.ErrorCount++
    s.metrics.ErrorTypes[err.Error()]++
    s.metrics.LastError = err
    s.metrics.LastErrorTime = time.Now()
    s.circuitBreaker.RecordFailure()
}

// Helper function to identify cursor boundary errors
func isCursorBoundaryError(err error) bool {
    errStr := err.Error()
    return strings.Contains(errStr, "cursor") && 
           strings.Contains(errStr, "must be between") &&
           strings.Contains(errStr, "latest ledger")
}
```

**Benefits**:
- Maintains fast polling (2 seconds)
- Eliminates false circuit breaker triggers
- Gracefully handles "caught up" state
- No external dependencies

### Solution 2: Dynamic Polling Strategy

**Approach**: Adjust polling frequency based on how close we are to the latest ledger.

**Implementation**:

```go
// Dynamic sleep calculation
func (s *RawLedgerServer) calculateSleepDuration(lastProcessed, latestLedger uint32) time.Duration {
    gap := latestLedger - lastProcessed
    
    switch {
    case gap > 10:
        return 1 * time.Second  // Far behind, poll aggressively
    case gap > 5:
        return 2 * time.Second  // Catching up
    case gap > 1:
        return 3 * time.Second  // Close to head
    default:
        return 5 * time.Second  // At head, poll conservatively
    }
}

// Usage in main loop
sleepDuration := s.calculateSleepDuration(lastProcessedSeq, resp.LatestLedger)
time.Sleep(sleepDuration)
```

**Benefits**:
- Adaptive performance
- Reduces boundary conditions
- Maintains efficiency when behind

### Solution 3: Enhanced Circuit Breaker Configuration

**Approach**: Make circuit breaker more tolerant of boundary conditions.

**Implementation**:

```go
// More tolerant circuit breaker
circuitBreaker := NewCircuitBreaker(10, 15*time.Second)

// Or add error-type awareness
type SmartCircuitBreaker struct {
    *CircuitBreaker
    boundaryErrorCount int
}

func (scb *SmartCircuitBreaker) RecordFailure(err error) {
    if isCursorBoundaryError(err) {
        scb.boundaryErrorCount++
        // Only count boundary errors if they're excessive
        if scb.boundaryErrorCount > 20 {
            scb.CircuitBreaker.RecordFailure()
            scb.boundaryErrorCount = 0
        }
    } else {
        scb.CircuitBreaker.RecordFailure()
    }
}
```

**Benefits**:
- Quick implementation
- Maintains current architecture
- Allows tuning tolerance

### Solution 4: Cursor Reset Strategy

**Approach**: Periodically reset cursor to avoid boundary accumulation.

**Implementation**:

```go
// Add cursor reset logic
consecutiveBoundaryErrors := 0

if isCursorBoundaryError(err) {
    consecutiveBoundaryErrors++
    
    if consecutiveBoundaryErrors >= 3 {
        s.logger.Info("Resetting cursor due to boundary errors",
            zap.Int("consecutive_errors", consecutiveBoundaryErrors),
        )
        
        // Reset to latest available ledger
        getLedgersReq.Pagination.Cursor = ""
        getLedgersReq.StartLedger = resp.LatestLedger
        consecutiveBoundaryErrors = 0
    }
    
    time.Sleep(5 * time.Second) // Wait for new ledgers
    continue
}
```

## Recommended Implementation Plan

### Phase 1: Immediate Fix (Solution 1)
1. Implement `isCursorBoundaryError()` helper function
2. Add boundary error detection in main loop
3. Skip circuit breaker recording for boundary errors
4. Add cursor reset logic for boundary conditions

### Phase 2: Enhancement (Solution 2)
1. Implement dynamic polling strategy
2. Monitor performance and boundary error frequency
3. Fine-tune polling intervals based on gap analysis

### Phase 3: Long-term (Solution 3)
1. Enhance circuit breaker with error-type awareness
2. Add comprehensive metrics for different error types
3. Implement alerting for genuine RPC failures vs boundary conditions

## Testing Strategy

### Test Cases
1. **Boundary Condition**: Start polling when already at latest ledger
2. **High Frequency**: Poll every 1-2 seconds for extended period
3. **Network Issues**: Simulate genuine RPC failures
4. **Mixed Scenarios**: Combine boundary conditions with real failures

### Success Criteria
- No circuit breaker triggers for cursor boundary errors
- Maintain <2 second latency for new ledger detection
- Graceful handling of "caught up" state
- Clear distinction between real failures and boundary conditions

## Configuration Recommendations

```go
// Recommended settings for high-frequency polling
const (
    defaultPollingInterval    = 2 * time.Second
    boundaryWaitInterval     = 5 * time.Second  // Wait when at boundary
    maxBoundaryRetries       = 3                // Before cursor reset
    circuitBreakerThreshold  = 8                // More tolerant
    circuitBreakerTimeout    = 15 * time.Second // Faster recovery
)
```

## Monitoring & Observability

### Additional Metrics
```go
type BoundaryMetrics struct {
    BoundaryErrorCount     int64
    CursorResetCount      int64
    AvgLedgerGap          float64
    MaxConsecutiveBoundaryErrors int
}
```

### Health Endpoint Enhancement
```json
{
  "metrics": {
    "boundary_errors": 15,
    "cursor_resets": 3,
    "avg_ledger_gap": 1.2,
    "polling_strategy": "dynamic"
  }
}
```

## Future Considerations

### Stellar Network Evolution
- **Higher TPS**: Faster ledger close times will reduce boundary conditions
- **Improved RPC**: Better cursor handling in future Stellar RPC versions
- **Streaming APIs**: Potential move to WebSocket-based streaming

### Performance Optimization
- **Connection Pooling**: Reduce connection overhead
- **Batch Processing**: Handle multiple ledgers per request when available
- **Predictive Polling**: Use ledger timing to predict optimal polling intervals

## Conclusion

The cursor boundary issue is a natural consequence of high-frequency polling at the "head" of the ledger stream. Solution 1 (Intelligent Cursor Boundary Handling) provides the most robust fix with minimal complexity, allowing 2-second polling while gracefully handling boundary conditions.

This approach maintains the performance benefits of fast polling while eliminating false positive circuit breaker triggers, resulting in a more reliable and responsive ledger streaming service.