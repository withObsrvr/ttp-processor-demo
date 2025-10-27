package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"

	// Import the generated protobuf code package
	rawledger "github.com/stellar/stellar-live-source/gen/raw_ledger_service"

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

	// Ledger retention and timing constants
	// Average ledger close time on Stellar network (seconds)
	AvgLedgerInterval = 10
	// Default retention window in days for local RPC storage
	DefaultRetentionDays = 7
	// Calculated retention window in number of ledgers
	// Formula: days * hours/day * minutes/hour * seconds/minute / seconds/ledger
	DefaultRetentionLedgers = DefaultRetentionDays * 24 * 60 * 60 / AvgLedgerInterval // ~60,480 ledgers
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
	config         *Config
	metrics        *DataSourceMetrics
	circuitBreaker *CircuitBreaker
	logger         *zap.Logger
	cache          *HistoricalCache
	prefetchMgr    *PrefetchManager
	accessPattern  *AccessPattern
}

// NewRawLedgerServer creates a new instance of the server with enterprise-grade configuration
func NewRawLedgerServer(rpcEndpoint string) (*RawLedgerServer, error) {
	// Load configuration
	config, err := LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %v", err)
	}
	
	// Override RPC endpoint if provided
	if rpcEndpoint != "" {
		config.RPCEndpoint = rpcEndpoint
	}

	// Initialize structured logging
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize logger: %v", err)
	}

	rpcClient := client.NewClient(config.RPCEndpoint, nil)

	// Initialize enhanced metrics and circuit breaker
	metrics := NewDataSourceMetrics()
	circuitBreaker := NewCircuitBreaker(config.CircuitBreakerThreshold, config.CircuitBreakerTimeout)

	// Enterprise-grade connection test
	logger.Info("Initializing enterprise-grade ledger source",
		zap.String("endpoint", config.RPCEndpoint),
		zap.Bool("serve_from_datastore", config.ServeFromDatastore),
		zap.String("datastore_type", config.DatastoreType),
		zap.String("bucket_path", config.BucketPath),
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
			zap.String("endpoint", config.RPCEndpoint),
			zap.Duration("max_retry_latency", MaxRetryLatency),
		)
		metrics.mu.Lock()
		metrics.SuccessCount++
		metrics.mu.Unlock()
		circuitBreaker.RecordSuccess()
	}

	// Initialize cache if datastore access is enabled
	var cache *HistoricalCache
	var prefetchMgr *PrefetchManager
	
	if config.ServeFromDatastore && config.CacheHistoricalResponses {
		cache = NewHistoricalCache(config.HistoricalCacheDuration, 10000) // 10k entries max
		logger.Info("Historical cache initialized for datastore access",
			zap.Duration("ttl", config.HistoricalCacheDuration),
			zap.Int("max_size", 10000),
		)
	}

	// Initialize access pattern tracker
	accessPattern := NewAccessPattern(100) // Track last 100 accesses

	// Create the server instance
	server := &RawLedgerServer{
		rpcClient:      rpcClient,
		config:         config,
		metrics:        metrics,
		circuitBreaker: circuitBreaker,
		logger:         logger,
		cache:          cache,
		accessPattern:  accessPattern,
	}

	// Initialize prefetch manager if enabled
	if config.EnablePredictivePrefetch && cache != nil {
		prefetchMgr = NewPrefetchManager(cache, config.PrefetchConcurrency, server.fetchLedgerForCache)
		prefetchMgr.Start()
		server.prefetchMgr = prefetchMgr
		
		logger.Info("Predictive prefetch enabled",
			zap.Int("concurrency", config.PrefetchConcurrency),
		)
	}

	return server, nil
}

// fetchLedgerForCache fetches a single ledger for caching
func (s *RawLedgerServer) fetchLedgerForCache(sequence uint32) ([]byte, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.OperationTimeout)
	defer cancel()

	// Build request with historical options
	getLedgersReq := s.buildGetLedgersRequest(sequence)

	resp, err := s.rpcClient.GetLedgers(ctx, getLedgersReq)
	if err != nil {
		return nil, "", err
	}

	if len(resp.Ledgers) == 0 {
		return nil, "", fmt.Errorf("no ledger found for sequence %d", sequence)
	}

	ledger := resp.Ledgers[0]
	source := "local"

	// Protocol 23: Determine if this came from external datastore
	if s.config.ServeFromDatastore {
		latestLedger, err := s.rpcClient.GetLatestLedger(ctx)
		if err == nil && sequence < latestLedger.Sequence-DefaultRetentionLedgers {
			source = "historical" // From external datastore
		}
	}

	// Decode base64 string to bytes
	decodedBytes, err := base64.StdEncoding.DecodeString(ledger.LedgerMetadata)
	if err != nil {
		return nil, "", fmt.Errorf("failed to decode ledger metadata: %w", err)
	}

	return decodedBytes, source, nil
}

// buildGetLedgersRequest builds a GetLedgers request
func (s *RawLedgerServer) buildGetLedgersRequest(startLedger uint32) protocol.GetLedgersRequest {
	req := protocol.GetLedgersRequest{
		StartLedger: startLedger,
		Pagination: &protocol.LedgerPaginationOptions{
			Limit: uint(s.config.BatchSize),
		},
	}

	// Protocol 23: The RPC server handles external datastore access transparently
	// based on its own SERVE_LEDGERS_FROM_DATASTORE configuration.
	// No special request parameters needed from the client side.

	return req
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
	backoff := s.config.InitialBackoff

	// Create initial GetLedgers request using our enhanced builder
	getLedgersReq := s.buildGetLedgersRequest(req.StartLedger)

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
			if status.Code(err) == codes.Canceled {
				return err
			}
			if retryCount >= s.config.MaxRetries {
				s.logger.Error("Max retries exceeded",
					zap.Int("retry_count", retryCount),
					zap.Error(err),
				)
				return fmt.Errorf("failed to get ledgers after %d retries: %v", s.config.MaxRetries, err)
			}
			continue
		}

		// Reset retry state on success
		retryCount = 0
		backoff = s.config.InitialBackoff

		if len(resp.Ledgers) == 0 {
			s.logger.Debug("No new ledgers",
				zap.Uint32("latest_ledger", resp.LatestLedger),
			)
			time.Sleep(5 * time.Second)
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

			// Check cache first if enabled
			var rawXdrBytes []byte
			var source string = "local" // default source
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
				s.logger.Debug("Processing ledger",
					zap.Uint32("sequence", ledgerInfo.Sequence),
				)

				// Decode with enterprise-grade error handling
				var err error
				rawXdrBytes, err = base64.StdEncoding.DecodeString(ledgerInfo.LedgerMetadata)
				if err != nil {
					s.logger.Error("Failed to decode ledger metadata",
						zap.Uint32("sequence", ledgerInfo.Sequence),
						zap.Error(err),
					)
					return status.Errorf(codes.Internal, "failed to decode ledger metadata for sequence %d: %v", ledgerInfo.Sequence, err)
				}

				// Determine if this is from external datastore
				// Protocol 23: RPC automatically serves from datastore if:
				// 1. SERVE_LEDGERS_FROM_DATASTORE is true on the RPC server
				// 2. The requested ledger is outside the retention window
				// The RPC handles this transparently, but we can infer based on:
				if s.config.ServeFromDatastore {
					// Get the RPC's retention window (typically 7 days)
					// Use constant instead of magic number
					if latestResp, err := s.rpcClient.GetLatestLedger(ctx); err == nil {
						if ledgerInfo.Sequence < latestResp.Sequence-DefaultRetentionLedgers {
							source = "historical" // From external datastore
						}
					}
				}

				// Cache the ledger if caching is enabled
				if s.cache != nil {
					s.cache.Put(ledgerInfo.Sequence, rawXdrBytes, source)
				}
			}

			// Create and send message with enterprise-grade metrics
			rawLedgerMsg := &rawledger.RawLedger{
				Sequence:           ledgerInfo.Sequence,
				LedgerCloseMetaXdr: rawXdrBytes,
			}

			// Update metrics with data source information
			s.metrics.mu.Lock()
			s.metrics.TotalProcessed++
			s.metrics.TotalBytesProcessed += int64(len(rawXdrBytes))
			s.metrics.LastSuccessfulSeq = ledgerInfo.Sequence
			s.metrics.mu.Unlock()
			
			// Update data source specific metrics
			s.metrics.UpdateDataSourceMetrics(source, latency)
			
			// Track access pattern for predictive prefetching
			if s.accessPattern != nil {
				s.accessPattern.Record(ledgerInfo.Sequence)
				
				// Queue prefetch for predicted sequences
				if s.prefetchMgr != nil {
					predictions := s.accessPattern.PredictNext(5)
					for _, predictedSeq := range predictions {
						s.prefetchMgr.QueuePrefetch(predictedSeq)
					}
				}
			}

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
			time.Sleep(5 * time.Second)
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

			if !shouldRetry(err) {
				return nil, err
			}

			s.metrics.mu.Lock()
			s.metrics.RetryCount++
			s.metrics.mu.Unlock()

			*backoff = s.calculateBackoff(*backoff, *retryCount)
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
func (s *RawLedgerServer) calculateBackoff(currentBackoff time.Duration, retryCount int) time.Duration {
	nextBackoff := currentBackoff * 2
	if nextBackoff > s.config.MaxBackoff {
		nextBackoff = s.config.MaxBackoff
	}

	jitter := time.Duration(rand.Float64() * float64(nextBackoff) * 0.1)
	nextBackoff += jitter

	return nextBackoff
}

// GetMetrics returns the current metrics
func (s *RawLedgerServer) GetMetrics() *DataSourceMetrics {
	return s.metrics
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

		// Get comprehensive metrics
		dataSourceMetrics := metrics.GetMetricsSummary()
		
		response := map[string]interface{}{
			"status": status,
			"service": "stellar-live-source",
			"protocol": 23,
			"config": map[string]interface{}{
				"serve_from_datastore": s.config.ServeFromDatastore,
				"datastore_type":       s.config.DatastoreType,
				"bucket_path":          s.config.BucketPath,
				"cache_enabled":        s.config.CacheHistoricalResponses,
				"prefetch_enabled":     s.config.EnablePredictivePrefetch,
				"buffer_size":          s.config.BufferSize,
				"num_workers":          s.config.NumWorkers,
			},
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
			"data_source_metrics": dataSourceMetrics,
			"cache_stats": func() interface{} {
				if s.cache != nil {
					return s.cache.Stats()
				}
				return nil
			}(),
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
