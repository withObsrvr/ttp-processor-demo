package server

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	rawledger "github.com/stellar/stellar-live-source/gen/raw_ledger_service"
	"github.com/stellar/stellar-rpc/protocol"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetLedgerRange implements the batch API for retrieving a specific range of ledgers
// Range semantics: [start_ledger, end_ledger) - start inclusive, end exclusive
func (s *RawLedgerServer) GetLedgerRange(ctx context.Context, req *rawledger.GetLedgerRangeRequest) (*rawledger.GetLedgerRangeResponse, error) {
	s.logger.Info("GetLedgerRange request",
		zap.Uint32("start_ledger", req.StartLedger),
		zap.Uint32("end_ledger", req.EndLedger),
	)

	// Validate range
	if req.EndLedger <= req.StartLedger {
		return nil, status.Error(codes.InvalidArgument, "end_ledger must be greater than start_ledger")
	}

	// Calculate range size
	rangeSize := req.EndLedger - req.StartLedger

	// Check against max batch size
	if rangeSize > uint32(s.config.MaxBatchSize) {
		return nil, status.Errorf(codes.InvalidArgument,
			"range too large: %d ledgers requested (max: %d)",
			rangeSize, s.config.MaxBatchSize)
	}

	// Check circuit breaker
	if !s.circuitBreaker.Allow() {
		s.logger.Warn("Circuit breaker open - service temporarily unavailable",
			zap.String("state", s.circuitBreaker.state),
		)
		return nil, status.Error(codes.Unavailable, "service temporarily unavailable")
	}

	// Build RPC request
	getLedgersReq := protocol.GetLedgersRequest{
		StartLedger: req.StartLedger,
		Pagination: &protocol.LedgerPaginationOptions{
			Limit: uint(rangeSize),
		},
	}

	// Fetch ledgers from RPC with timeout
	startTime := time.Now()
	resp, err := s.rpcClient.GetLedgers(ctx, getLedgersReq)
	latency := time.Since(startTime)

	// Update metrics
	s.metrics.mu.Lock()
	s.metrics.TotalLatency += latency
	s.metrics.LatencyHistogram = append(s.metrics.LatencyHistogram, latency)
	if err != nil {
		s.metrics.ErrorCount++
		s.metrics.ErrorTypes[err.Error()]++
		s.metrics.LastError = err
		s.metrics.LastErrorTime = time.Now()
		s.circuitBreaker.RecordFailure()
	} else {
		s.metrics.SuccessCount++
		s.circuitBreaker.RecordSuccess()
	}
	s.metrics.mu.Unlock()

	if err != nil {
		s.logger.Error("Failed to get ledgers from RPC",
			zap.Error(err),
			zap.Uint32("start_ledger", req.StartLedger),
			zap.Uint32("end_ledger", req.EndLedger),
		)
		return nil, status.Errorf(codes.Internal, "failed to get ledgers: %v", err)
	}

	// Process and collect ledgers
	ledgers := make([]*rawledger.RawLedger, 0, len(resp.Ledgers))

	for _, ledgerInfo := range resp.Ledgers {
		// Skip ledgers outside the requested range
		// This handles the case where RPC returns ledgers outside our range
		if ledgerInfo.Sequence < req.StartLedger || ledgerInfo.Sequence >= req.EndLedger {
			s.logger.Debug("Skipping ledger outside requested range",
				zap.Uint32("sequence", ledgerInfo.Sequence),
				zap.Uint32("start_ledger", req.StartLedger),
				zap.Uint32("end_ledger", req.EndLedger),
			)
			continue
		}

		// Check cache first if enabled
		var rawXdrBytes []byte
		var source string = "local"
		fromCache := false

		if s.cache != nil {
			if cachedData, cachedSource, exists := s.cache.Get(ledgerInfo.Sequence); exists {
				rawXdrBytes = cachedData
				source = cachedSource
				fromCache = true
				s.metrics.CacheHits++
				s.logger.Debug("Ledger found in cache",
					zap.Uint32("sequence", ledgerInfo.Sequence),
					zap.String("source", source),
				)
			}
		}

		// If not in cache, decode from response
		if !fromCache {
			var err error
			rawXdrBytes, err = base64.StdEncoding.DecodeString(ledgerInfo.LedgerMetadata)
			if err != nil {
				s.logger.Error("Failed to decode ledger metadata",
					zap.Uint32("sequence", ledgerInfo.Sequence),
					zap.Error(err),
				)
				return nil, status.Errorf(codes.Internal,
					"failed to decode ledger metadata for sequence %d: %v",
					ledgerInfo.Sequence, err)
			}

			// Determine if this is from external datastore
			if s.config.ServeFromDatastore {
				if latestResp, err := s.rpcClient.GetLatestLedger(ctx); err == nil {
					if ledgerInfo.Sequence < latestResp.Sequence-DefaultRetentionLedgers {
						source = "historical"
					}
				}
			}

			// Cache the ledger if caching is enabled
			if s.cache != nil {
				s.cache.Put(ledgerInfo.Sequence, rawXdrBytes, source)
			}
		}

		// Create ledger message
		rawLedgerMsg := &rawledger.RawLedger{
			Sequence:           ledgerInfo.Sequence,
			LedgerCloseMetaXdr: rawXdrBytes,
		}

		ledgers = append(ledgers, rawLedgerMsg)

		// Update metrics
		s.metrics.mu.Lock()
		s.metrics.TotalProcessed++
		s.metrics.TotalBytesProcessed += int64(len(rawXdrBytes))
		s.metrics.LastSuccessfulSeq = ledgerInfo.Sequence
		s.metrics.mu.Unlock()

		// Update data source specific metrics
		s.metrics.UpdateDataSourceMetrics(source, latency)
	}

	// Log warning if we got fewer ledgers than expected
	if len(ledgers) < int(rangeSize) {
		s.logger.Warn("Received fewer ledgers than requested",
			zap.Int("requested", int(rangeSize)),
			zap.Int("received", len(ledgers)),
			zap.Uint32("start_ledger", req.StartLedger),
			zap.Uint32("end_ledger", req.EndLedger),
		)
	}

	s.logger.Info("GetLedgerRange completed",
		zap.Int("ledgers_returned", len(ledgers)),
		zap.Duration("latency", latency),
	)

	return &rawledger.GetLedgerRangeResponse{
		Ledgers:      ledgers,
		LatestLedger: resp.LatestLedger,
	}, nil
}

// GetLedgerRangeWithRetry is a helper that implements retry logic for GetLedgerRange
// This can be used by clients that want automatic retry handling
func (s *RawLedgerServer) GetLedgerRangeWithRetry(ctx context.Context, req *rawledger.GetLedgerRangeRequest, maxRetries int) (*rawledger.GetLedgerRangeResponse, error) {
	var lastErr error
	backoff := s.config.InitialBackoff

	for attempt := 0; attempt <= maxRetries; attempt++ {
		resp, err := s.GetLedgerRange(ctx, req)
		if err == nil {
			return resp, nil
		}

		lastErr = err

		// Don't retry on invalid argument errors
		if status.Code(err) == codes.InvalidArgument {
			return nil, err
		}

		// Don't retry if context is cancelled
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// Calculate backoff with jitter if we're going to retry
		if attempt < maxRetries {
			s.logger.Warn("GetLedgerRange failed, retrying",
				zap.Error(err),
				zap.Int("attempt", attempt+1),
				zap.Int("max_retries", maxRetries),
				zap.Duration("backoff", backoff),
			)

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
				// Calculate next backoff
				backoff = s.calculateBackoff(backoff, attempt)
			}
		}
	}

	return nil, fmt.Errorf("failed after %d retries: %w", maxRetries, lastErr)
}
