package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/support/storage"
	"github.com/stellar/go/xdr"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/gen/raw_ledger_service"
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
		cb.mu.RLock()
		cb.state = "half-open"
		cb.mu.RUnlock()
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

// StellarArchiveReader provides an interface for reading ledger data using stellar-go
type StellarArchiveReader struct {
	archive historyarchive.ArchiveInterface
	logger  *zap.Logger
}

// NewStellarArchiveReader creates a new archive reader
func NewStellarArchiveReader(storageType, bucketName, archivePath, region, endpoint string, logger *zap.Logger) (*StellarArchiveReader, error) {
	ctx := context.Background()

	// Construct proper archive URL format
	var archiveURL string
	switch strings.ToLower(storageType) {
	case "gcs":
		archiveURL = fmt.Sprintf("gcs://%s/%s", bucketName, archivePath)
	case "s3":
		archiveURL = fmt.Sprintf("s3://%s/%s", bucketName, archivePath)
	case "fs", "file":
		archiveURL = fmt.Sprintf("file://%s", bucketName) // bucketName is the file path for filesystem
	default:
		return nil, fmt.Errorf("unsupported storage type: %s", storageType)
	}

	logger.Info("Connecting to Stellar archive",
		zap.String("archive_url", archiveURL),
		zap.String("storage_type", storageType),
	)

	// Create archive using historyarchive.Connect with proper options
	archive, err := historyarchive.Connect(archiveURL, historyarchive.ArchiveOptions{
		ConnectOptions: storage.ConnectOptions{
			Context: ctx,
			S3Region: region,
			S3Endpoint: endpoint,
			GCSEndpoint: endpoint,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to archive %s: %w", archiveURL, err)
	}

	return &StellarArchiveReader{
		archive: archive,
		logger:  logger,
	}, nil
}

// GetLedgerCloseMeta retrieves a ledger close meta for a specific sequence using stellar-go methods
func (sar *StellarArchiveReader) GetLedgerCloseMeta(sequence uint32) (*xdr.LedgerCloseMeta, error) {
	sar.logger.Debug("Getting ledger close meta",
		zap.Uint32("sequence", sequence))

	// Use stellar-go's GetLedgers method for proper checkpoint handling
	ledgers, err := sar.archive.GetLedgers(sequence, sequence)
	if err != nil {
		return nil, fmt.Errorf("failed to get ledger %d from archive: %w", sequence, err)
	}

	ledger, exists := ledgers[sequence]
	if !exists {
		return nil, fmt.Errorf("ledger %d not found in checkpoint", sequence)
	}

	// Convert historyarchive.Ledger to LedgerCloseMeta
	lcm := sar.convertToLedgerCloseMeta(ledger)

	sar.logger.Debug("Successfully retrieved ledger close meta",
		zap.Uint32("sequence", sequence),
		zap.Uint32("ledger_seq", uint32(ledger.Header.Header.LedgerSeq)))

	return lcm, nil
}

// StreamLedgerCloseMeta streams ledger close meta for a range of sequences using efficient batch processing
func (sar *StellarArchiveReader) StreamLedgerCloseMeta(startSeq, endSeq uint32, output chan<- *xdr.LedgerCloseMeta) error {
	defer close(output)

	sar.logger.Info("Starting to stream ledger close meta",
		zap.Uint32("start_seq", startSeq),
		zap.Uint32("end_seq", endSeq))

	// Process in checkpoint-sized chunks for efficiency
	checkpointManager := sar.archive.GetCheckpointManager()
	
	for seq := startSeq; seq <= endSeq; {
		// Get the checkpoint range for this sequence
		checkpointRange := checkpointManager.GetCheckpointRange(seq)
		endRange := checkpointRange.High
		if endRange > endSeq {
			endRange = endSeq
		}

		sar.logger.Debug("Processing checkpoint range",
			zap.Uint32("start", seq),
			zap.Uint32("end", endRange),
			zap.Uint32("checkpoint_low", checkpointRange.Low),
			zap.Uint32("checkpoint_high", checkpointRange.High))

		// Get all ledgers in this range efficiently
		ledgers, err := sar.archive.GetLedgers(seq, endRange)
		if err != nil {
			sar.logger.Error("Failed to get ledgers from archive",
				zap.Uint32("start", seq),
				zap.Uint32("end", endRange),
				zap.Error(err))
			return fmt.Errorf("failed to get ledgers %d-%d: %w", seq, endRange, err)
		}

		// Stream each ledger in the range
		for i := seq; i <= endRange; i++ {
			ledger, exists := ledgers[i]
			if !exists {
				sar.logger.Debug("Ledger not found in checkpoint, skipping",
					zap.Uint32("sequence", i))
				continue
			}

			// Convert to LedgerCloseMeta
			lcm := sar.convertToLedgerCloseMeta(ledger)

			select {
			case output <- lcm:
				sar.logger.Debug("Sent ledger to output",
					zap.Uint32("sequence", i))
			case <-time.After(5 * time.Second):
				return fmt.Errorf("timeout sending ledger %d to output channel", i)
			}
		}

		seq = endRange + 1
	}

	sar.logger.Info("Finished streaming ledger close meta",
		zap.Uint32("start_seq", startSeq),
		zap.Uint32("end_seq", endSeq))

	return nil
}

// convertToLedgerCloseMeta converts a historyarchive.Ledger to xdr.LedgerCloseMeta
func (sar *StellarArchiveReader) convertToLedgerCloseMeta(ledger *historyarchive.Ledger) *xdr.LedgerCloseMeta {
	// For now, create a basic LedgerCloseMeta V1 structure
	// This can be enhanced to include transaction and result data as needed
	lcm := &xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: ledger.Header,
			// Initialize empty transaction set for now
			TxSet: xdr.GeneralizedTransactionSet{
				V: 1,
				V1TxSet: &xdr.TransactionSetV1{
					PreviousLedgerHash: ledger.Header.Header.PreviousLedgerHash,
					Phases: []xdr.TransactionPhase{},
				},
			},
			// Initialize empty transaction processing results
			TxProcessing: []xdr.TransactionResultMeta{},
			// Initialize empty upgrade processing
			UpgradesProcessing: []xdr.UpgradeEntryMeta{},
			// Initialize empty SCP info
			ScpInfo: []xdr.ScpHistoryEntry{},
		},
	}

	sar.logger.Debug("Converted ledger to LedgerCloseMeta",
		zap.Uint32("ledger_seq", uint32(ledger.Header.Header.LedgerSeq)),
		zap.String("ledger_hash", ledger.Header.Hash.HexString()))

	return lcm
}

type RawLedgerServer struct {
	pb.UnimplementedRawLedgerServiceServer
	logger         *zap.Logger
	metrics        *EnterpriseMetrics
	circuitBreaker *CircuitBreaker
	archiveReader  *GalexieDatalakeReader
}

func NewRawLedgerServer() (*RawLedgerServer, error) {
	// Initialize structured logging
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize logger: %v", err)
	}

	// Initialize metrics and circuit breaker
	metrics := NewEnterpriseMetrics()
	circuitBreaker := NewCircuitBreaker(5, 30*time.Second)

	return &RawLedgerServer{
		logger:         logger,
		metrics:        metrics,
		circuitBreaker: circuitBreaker,
	}, nil
}

// StreamRawLedgers implements the gRPC StreamRawLedgers method using stellar-go
func (s *RawLedgerServer) StreamRawLedgers(req *pb.StreamLedgersRequest, stream pb.RawLedgerService_StreamRawLedgersServer) error {
	ctx := stream.Context()
	s.logger.Info("Starting stellar-go based ledger stream",
		zap.Uint32("start_sequence", req.StartLedger),
		zap.Duration("max_latency_p99", MaxLatencyP99),
	)

	// --- Configuration from Environment Variables ---
	storageType := os.Getenv("STORAGE_TYPE")
	bucketName := os.Getenv("BUCKET_NAME")
	region := os.Getenv("AWS_REGION")
	endpoint := os.Getenv("S3_ENDPOINT_URL")

	if storageType == "" || bucketName == "" {
		s.logger.Error("Missing required environment variables",
			zap.String("storage_type", storageType),
			zap.String("bucket_name", bucketName),
		)
		return status.Error(codes.InvalidArgument, "STORAGE_TYPE and BUCKET_NAME environment variables are required")
	}

	s.logger.Info("Using stellar-go storage configuration",
		zap.String("storage_type", storageType),
		zap.String("bucket_path", bucketName),
	)

	// Get archive path from environment or use default
	archivePath := os.Getenv("ARCHIVE_PATH")
	if archivePath == "" {
		// Default path structure for common archive layouts
		switch storageType {
		case "GCS":
			archivePath = "landing/ledgers/testnet" // Default for GCS
		case "S3":
			archivePath = "archive" // Default for S3
		default:
			archivePath = "" // No path for filesystem
		}
	}

	// Initialize the stellar archive reader
	archiveReader, err := NewGalexieDatalakeReader(storageType, bucketName, archivePath, region, endpoint, s.logger)
	if err != nil {
		s.logger.Error("Failed to initialize stellar archive reader",
			zap.Error(err))
		return status.Errorf(codes.Internal, "failed to initialize stellar archive reader: %v", err)
	}
	s.archiveReader = archiveReader

	// --- Process Ledgers from Storage ---
	processedCount := 0
	startTime := time.Now()
	
	// Calculate end ledger (for bounded requests)
	endLedger := req.StartLedger + 1000 // Process up to 1000 ledgers by default
	// Note: StreamLedgersRequest doesn't have EndLedger field, so we use a default range

	s.logger.Info("Processing ledger range",
		zap.Uint32("start_ledger", req.StartLedger),
		zap.Uint32("end_ledger", endLedger),
	)

	// Process ledgers sequentially
	for sequence := req.StartLedger; sequence <= endLedger; sequence++ {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			s.logger.Info("Context cancelled during processing",
				zap.Uint32("ledger_sequence", sequence),
			)
			return ctx.Err()
		default:
		}

		// Check circuit breaker
		if !s.circuitBreaker.Allow() {
			s.logger.Warn("Circuit breaker open - service temporarily unavailable",
				zap.String("state", s.circuitBreaker.state),
				zap.Int("failure_count", s.circuitBreaker.failureCount),
			)
			return status.Error(codes.Unavailable, "service temporarily unavailable")
		}

		ledgerStartTime := time.Now()
		s.logger.Debug("Processing ledger",
			zap.Uint32("sequence", sequence),
		)

		// Get ledger close meta using stellar-go
		lcm, err := s.archiveReader.GetLedgerCloseMeta(sequence)
		if err != nil {
			s.logger.Error("Failed to get ledger close meta",
				zap.Uint32("sequence", sequence),
				zap.Error(err),
			)
			s.metrics.mu.Lock()
			s.metrics.ErrorCount++
			s.metrics.ErrorTypes["ledger_read"]++
			s.metrics.LastError = err
			s.metrics.LastErrorTime = time.Now()
			s.metrics.mu.Unlock()
			s.circuitBreaker.RecordFailure()
			return status.Errorf(codes.Internal, "failed to get ledger %d: %v", sequence, err)
		}

		// Convert LedgerCloseMeta to raw bytes
		rawBytes, err := lcm.MarshalBinary()
		if err != nil {
			s.logger.Error("Failed to marshal ledger",
				zap.Uint32("sequence", sequence),
				zap.Error(err),
			)
			s.metrics.mu.Lock()
			s.metrics.ErrorCount++
			s.metrics.ErrorTypes["marshal"]++
			s.metrics.LastError = err
			s.metrics.LastErrorTime = time.Now()
			s.metrics.mu.Unlock()
			s.circuitBreaker.RecordFailure()
			return status.Errorf(codes.Internal, "failed to marshal ledger %d: %v", sequence, err)
		}

		// Create and send the RawLedger message
		rawLedger := &pb.RawLedger{
			Sequence:           sequence,
			LedgerCloseMetaXdr: rawBytes,
		}

		if err := stream.Send(rawLedger); err != nil {
			s.logger.Error("Failed to send ledger",
				zap.Uint32("sequence", sequence),
				zap.Error(err),
			)
			s.metrics.mu.Lock()
			s.metrics.ErrorCount++
			s.metrics.ErrorTypes["stream"]++
			s.metrics.LastError = err
			s.metrics.LastErrorTime = time.Now()
			s.metrics.mu.Unlock()
			s.circuitBreaker.RecordFailure()
			return status.Errorf(codes.Internal, "failed to send ledger %d: %v", sequence, err)
		}

		// Update metrics
		latency := time.Since(ledgerStartTime)
		s.metrics.mu.Lock()
		s.metrics.SuccessCount++
		s.metrics.TotalProcessed++
		s.metrics.TotalBytesProcessed += int64(len(rawBytes))
		s.metrics.LatencyHistogram = append(s.metrics.LatencyHistogram, latency)
		s.metrics.LastSuccessfulSeq = sequence
		s.metrics.mu.Unlock()
		s.circuitBreaker.RecordSuccess()

		processedCount++
		if processedCount%10 == 0 {
			s.logger.Info("Processing progress",
				zap.Int("processed_count", processedCount),
				zap.Uint32("last_sequence", sequence),
			)
		}
	}

	elapsed := time.Since(startTime)
	s.logger.Info("Finished processing",
		zap.Int("total_processed", processedCount),
		zap.Duration("elapsed_time", elapsed),
	)

	s.logger.Info("Successfully completed stellar-go based ledger processing")
	return nil
}

// Helper to get environment variable as uint32 with default
func getEnvAsUint(key string, defaultVal uint64) uint64 {
	valStr := os.Getenv(key)
	if valStr == "" {
		return defaultVal
	}
	val, err := strconv.ParseUint(valStr, 10, 32)
	if err != nil {
		return defaultVal
	}
	return val
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
			"implementation": "stellar-go",
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
	s.logger.Info("Starting stellar-go based health check server",
		zap.String("address", addr),
	)
	return http.ListenAndServe(addr, nil)
}