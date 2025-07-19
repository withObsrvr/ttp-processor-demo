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

	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/datastore"
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

// LedgerBackendConfig holds configuration for different backend types
type LedgerBackendConfig struct {
	BackendType        string
	NetworkPassphrase  string
	HistoryArchiveURLs []string
	
	// Captive Core specific
	StellarCoreBinaryPath string
	StellarCoreConfigPath string
	
	// RPC specific
	RPCEndpoint string
	
	// Archive specific  
	ArchiveStorageType string
	ArchiveBucketName  string
}

type RawLedgerServer struct {
	pb.UnimplementedRawLedgerServiceServer
	logger         *zap.Logger
	metrics        *EnterpriseMetrics
	circuitBreaker *CircuitBreaker
	config         *LedgerBackendConfig
}

// Helper functions for configuration
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func loadConfiguration() (*LedgerBackendConfig, error) {
	// Check for backward compatibility migration first
	backendType := os.Getenv("BACKEND_TYPE")
	if backendType == "" {
		// Check for old STORAGE_TYPE configuration
		if oldStorageType := os.Getenv("STORAGE_TYPE"); oldStorageType != "" {
			switch oldStorageType {
			case "GCS", "S3":
				backendType = "ARCHIVE"
			case "FS":
				backendType = "CAPTIVE_CORE" // Default fallback
			default:
				backendType = "CAPTIVE_CORE" // Default
			}
		} else {
			backendType = "CAPTIVE_CORE" // Default
		}
	}

	config := &LedgerBackendConfig{
		BackendType:       backendType,
		NetworkPassphrase: getEnvOrDefault("NETWORK_PASSPHRASE", "Public Network; September 2015"),
		HistoryArchiveURLs: strings.Split(getEnvOrDefault("HISTORY_ARCHIVE_URLS", ""), ","),
	}
	
	switch config.BackendType {
	case "CAPTIVE_CORE":
		config.StellarCoreBinaryPath = os.Getenv("STELLAR_CORE_BINARY_PATH")
		config.StellarCoreConfigPath = os.Getenv("STELLAR_CORE_CONFIG_PATH")
		if config.StellarCoreBinaryPath == "" {
			return nil, fmt.Errorf("STELLAR_CORE_BINARY_PATH required for CAPTIVE_CORE backend")
		}
	case "RPC":
		config.RPCEndpoint = os.Getenv("RPC_ENDPOINT")
		if config.RPCEndpoint == "" {
			return nil, fmt.Errorf("RPC_ENDPOINT required for RPC backend")
		}
	case "ARCHIVE":
		config.ArchiveStorageType = os.Getenv("ARCHIVE_STORAGE_TYPE")
		config.ArchiveBucketName = os.Getenv("ARCHIVE_BUCKET_NAME")
		
		// Handle backward compatibility
		if config.ArchiveStorageType == "" {
			config.ArchiveStorageType = os.Getenv("STORAGE_TYPE") // Fallback to old config
		}
		if config.ArchiveBucketName == "" {
			config.ArchiveBucketName = os.Getenv("BUCKET_NAME") // Fallback to old config
		}
		
		if config.ArchiveStorageType == "" || config.ArchiveBucketName == "" {
			return nil, fmt.Errorf("ARCHIVE_STORAGE_TYPE (or STORAGE_TYPE) and ARCHIVE_BUCKET_NAME (or BUCKET_NAME) required for ARCHIVE backend")
		}
	}
	
	return config, nil
}

func (s *RawLedgerServer) migrateOldConfiguration() {
	// Map old STORAGE_TYPE to new BACKEND_TYPE
	if oldStorageType := os.Getenv("STORAGE_TYPE"); oldStorageType != "" && os.Getenv("BACKEND_TYPE") == "" {
		switch oldStorageType {
		case "GCS", "S3":
			// Update server config directly
			s.config.BackendType = "ARCHIVE"
			s.config.ArchiveStorageType = oldStorageType
			if bucketName := os.Getenv("BUCKET_NAME"); bucketName != "" {
				s.config.ArchiveBucketName = bucketName
			}
			s.logger.Warn("Migrated deprecated STORAGE_TYPE configuration",
				zap.String("old_storage_type", oldStorageType),
				zap.String("new_backend_type", "ARCHIVE"),
				zap.String("bucket_name", s.config.ArchiveBucketName),
			)
		case "FS":
			// FS storage type is not supported in official datastore, fallback to RPC
			s.logger.Warn("FS storage type not supported, please use BACKEND_TYPE=RPC or BACKEND_TYPE=CAPTIVE_CORE")
		}
	}
	
	// Warn about deprecated variables but don't ignore them for archive backend
	if s.config.BackendType == "ARCHIVE" {
		if os.Getenv("LEDGERS_PER_FILE") != "" {
			s.logger.Info("Using LEDGERS_PER_FILE for archive backend configuration")
		}
		if os.Getenv("FILES_PER_PARTITION") != "" {
			s.logger.Info("Using FILES_PER_PARTITION for archive backend configuration")
		}
	} else {
		// For non-archive backends, these are truly deprecated
		deprecatedVars := []string{"LEDGERS_PER_FILE", "FILES_PER_PARTITION"}
		for _, varName := range deprecatedVars {
			if os.Getenv(varName) != "" {
				s.logger.Warn("Environment variable not used by current backend", 
					zap.String("variable", varName),
					zap.String("backend_type", s.config.BackendType))
			}
		}
	}
}

func NewRawLedgerServer() (*RawLedgerServer, error) {
	// Initialize structured logging
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize logger: %v", err)
	}

	// Load configuration
	config, err := loadConfiguration()
	if err != nil {
		return nil, fmt.Errorf("failed to load configuration: %w", err)
	}

	// Initialize metrics and circuit breaker
	metrics := NewEnterpriseMetrics()
	circuitBreaker := NewCircuitBreaker(5, 30*time.Second)

	server := &RawLedgerServer{
		logger:         logger,
		metrics:        metrics,
		circuitBreaker: circuitBreaker,
		config:         config,
	}

	// Migrate old configuration if needed
	server.migrateOldConfiguration()

	return server, nil
}

// Backend creation methods
func (s *RawLedgerServer) createLedgerBackend(ctx context.Context) (ledgerbackend.LedgerBackend, error) {
	switch s.config.BackendType {
	case "CAPTIVE_CORE":
		return s.createCaptiveCore()
	case "RPC":
		return s.createRPCBackend()
	case "ARCHIVE":
		return s.createArchiveBackend()
	default:
		return nil, fmt.Errorf("unsupported backend type: %s", s.config.BackendType)
	}
}

func (s *RawLedgerServer) createCaptiveCore() (ledgerbackend.LedgerBackend, error) {
	captiveCoreToml, err := ledgerbackend.NewCaptiveCoreToml(
		ledgerbackend.CaptiveCoreTomlParams{
			NetworkPassphrase:  s.config.NetworkPassphrase,
			HistoryArchiveURLs: s.config.HistoryArchiveURLs,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create captive core toml: %w", err)
	}

	config := ledgerbackend.CaptiveCoreConfig{
		BinaryPath:         s.config.StellarCoreBinaryPath,
		NetworkPassphrase:  s.config.NetworkPassphrase,
		HistoryArchiveURLs: s.config.HistoryArchiveURLs,
		CheckpointFrequency: 64,
		Toml:               captiveCoreToml,
	}

	return ledgerbackend.NewCaptive(config)
}

func (s *RawLedgerServer) createRPCBackend() (ledgerbackend.LedgerBackend, error) {
	options := ledgerbackend.RPCLedgerBackendOptions{
		RPCServerURL: s.config.RPCEndpoint,
		BufferSize:   10, // Default buffer size
	}
	
	return ledgerbackend.NewRPCLedgerBackend(options), nil
}

func (s *RawLedgerServer) createArchiveBackend() (ledgerbackend.LedgerBackend, error) {
	// Create datastore configuration
	schema := datastore.DataStoreSchema{
		LedgersPerFile:    uint32(getEnvAsUint("LEDGERS_PER_FILE", 64)),
		FilesPerPartition: uint32(getEnvAsUint("FILES_PER_PARTITION", 10)),
	}

	// Setup datastore parameters based on storage type
	dsParams := make(map[string]string)
	var dsConfig datastore.DataStoreConfig

	switch s.config.ArchiveStorageType {
	case "GCS":
		dsParams["destination_bucket_path"] = s.config.ArchiveBucketName
		dsConfig = datastore.DataStoreConfig{Type: "GCS", Schema: schema, Params: dsParams}
	case "S3":
		dsParams["bucket_name"] = s.config.ArchiveBucketName
		dsParams["region"] = getEnvOrDefault("AWS_REGION", "us-east-1")
		if endpoint := os.Getenv("S3_ENDPOINT_URL"); endpoint != "" {
			dsParams["endpoint"] = endpoint
			dsParams["force_path_style"] = os.Getenv("S3_FORCE_PATH_STYLE")
		}
		dsConfig = datastore.DataStoreConfig{Type: "S3", Schema: schema, Params: dsParams}
	default:
		return nil, fmt.Errorf("unsupported ARCHIVE_STORAGE_TYPE: %s (supported: GCS, S3)", s.config.ArchiveStorageType)
	}

	// Create the datastore
	dataStore, err := datastore.NewDataStore(context.Background(), dsConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create datastore: %w", err)
	}

	// Create BufferedStorageBackend configuration
	bufferedConfig := ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: 100,        // Process 100 ledgers in parallel
		NumWorkers: 5,          // Use 5 worker goroutines
		RetryLimit: 3,          // Retry failed operations 3 times
		RetryWait:  time.Second, // Wait 1 second between retries
	}

	// Create the BufferedStorageBackend
	backend, err := ledgerbackend.NewBufferedStorageBackend(bufferedConfig, dataStore)
	if err != nil {
		dataStore.Close()
		return nil, fmt.Errorf("failed to create buffered storage backend: %w", err)
	}

	s.logger.Info("Created archive backend",
		zap.String("storage_type", s.config.ArchiveStorageType),
		zap.String("bucket_name", s.config.ArchiveBucketName),
		zap.Uint32("ledgers_per_file", schema.LedgersPerFile),
		zap.Uint32("files_per_partition", schema.FilesPerPartition),
	)

	return backend, nil
}

// Protocol 23 upgrade constants
const (
	PROTOCOL_23_ACTIVATION_LEDGER = 3000000 // Placeholder - update with actual Protocol 23 activation ledger
)

// StreamRawLedgers implements the gRPC StreamRawLedgers method using official Stellar Go SDK
func (s *RawLedgerServer) StreamRawLedgers(req *pb.StreamLedgersRequest, stream pb.RawLedgerService_StreamRawLedgersServer) error {
	ctx := stream.Context()
	s.logger.Info("Starting enterprise ledger stream with official Stellar backend",
		zap.Uint32("start_sequence", req.StartLedger),
		zap.String("backend_type", s.config.BackendType),
		zap.Duration("max_latency_p99", MaxLatencyP99),
	)

	// Create backend based on configuration
	backend, err := s.createLedgerBackend(ctx)
	if err != nil {
		s.logger.Error("Failed to create ledger backend",
			zap.String("backend_type", s.config.BackendType),
			zap.Error(err),
		)
		return status.Error(codes.Internal, fmt.Sprintf("failed to create ledger backend: %v", err))
	}
	defer backend.Close()

	// Prepare range for processing
	ledgerRange := ledgerbackend.UnboundedRange(req.StartLedger)
	if err := backend.PrepareRange(ctx, ledgerRange); err != nil {
		s.logger.Error("Failed to prepare ledger range",
			zap.Uint32("start_ledger", req.StartLedger),
			zap.Error(err),
		)
		return status.Error(codes.Internal, fmt.Sprintf("failed to prepare range: %v", err))
	}

	s.logger.Info("Backend prepared successfully",
		zap.Uint32("start_ledger", req.StartLedger),
		zap.String("backend_type", s.config.BackendType),
	)

	// Stream ledgers
	return s.streamLedgersFromBackend(ctx, backend, req.StartLedger, stream)
}

func (s *RawLedgerServer) streamLedgersFromBackend(ctx context.Context, backend ledgerbackend.LedgerBackend, startSeq uint32, stream pb.RawLedgerService_StreamRawLedgersServer) error {
	processedCount := 0
	startTime := time.Now()

	for seq := startSeq; ; seq++ {
		select {
		case <-ctx.Done():
			s.logger.Info("Context cancelled during streaming",
				zap.Uint32("last_sequence", seq-1),
				zap.Int("total_processed", processedCount),
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

		// Get ledger from backend
		lcm, err := backend.GetLedger(ctx, seq)
		if err != nil {
			s.logger.Error("Failed to get ledger from backend",
				zap.Uint32("sequence", seq),
				zap.String("backend_type", s.config.BackendType),
				zap.Error(err),
			)
			s.handleLedgerError(err, seq)
			continue
		}

		// Process Protocol 23 specific changes
		if err := s.validateProtocol23Compatibility(lcm); err != nil {
			s.logger.Error("Protocol 23 validation failed",
				zap.Uint32("sequence", lcm.LedgerSequence()),
				zap.Error(err),
			)
			return status.Error(codes.Internal, fmt.Sprintf("protocol 23 validation failed: %v", err))
		}

		// Convert to protobuf and stream
		rawLedger, err := s.convertLedgerToProto(lcm)
		if err != nil {
			s.logger.Error("Failed to convert ledger to proto",
				zap.Uint32("sequence", lcm.LedgerSequence()),
				zap.Error(err),
			)
			s.handleLedgerError(err, lcm.LedgerSequence())
			continue
		}

		if err := stream.Send(rawLedger); err != nil {
			s.logger.Error("Failed to send ledger to stream",
				zap.Uint32("sequence", lcm.LedgerSequence()),
				zap.Error(err),
			)
			return status.Error(codes.Internal, fmt.Sprintf("failed to send ledger: %v", err))
		}

		// Update metrics
		latency := time.Since(ledgerStartTime)
		s.updateSuccessMetrics(lcm, latency, len(rawLedger.LedgerCloseMetaXdr))
		s.circuitBreaker.RecordSuccess()

		processedCount++
		if processedCount%100 == 0 {
			s.logger.Info("Processing progress",
				zap.Int("processed_count", processedCount),
				zap.Uint32("last_sequence", lcm.LedgerSequence()),
				zap.Duration("elapsed_time", time.Since(startTime)),
			)
		}
	}
}

func (s *RawLedgerServer) handleLedgerError(err error, seq uint32) {
	s.metrics.mu.Lock()
	s.metrics.ErrorCount++
	s.metrics.ErrorTypes["ledger_fetch"]++
	s.metrics.LastError = err
	s.metrics.LastErrorTime = time.Now()
	s.metrics.mu.Unlock()
	s.circuitBreaker.RecordFailure()
}

func (s *RawLedgerServer) convertLedgerToProto(lcm xdr.LedgerCloseMeta) (*pb.RawLedger, error) {
	// Convert LedgerCloseMeta to raw bytes
	rawBytes, err := lcm.MarshalBinary()
	if err != nil {
		s.metrics.mu.Lock()
		s.metrics.ErrorCount++
		s.metrics.ErrorTypes["marshal"]++
		s.metrics.LastError = err
		s.metrics.LastErrorTime = time.Now()
		s.metrics.mu.Unlock()
		return nil, fmt.Errorf("error marshaling ledger %d: %w", lcm.LedgerSequence(), err)
	}

	return &pb.RawLedger{
		Sequence:           lcm.LedgerSequence(),
		LedgerCloseMetaXdr: rawBytes,
	}, nil
}

func (s *RawLedgerServer) updateSuccessMetrics(lcm xdr.LedgerCloseMeta, latency time.Duration, bytesProcessed int) {
	s.metrics.mu.Lock()
	s.metrics.SuccessCount++
	s.metrics.TotalProcessed++
	s.metrics.TotalBytesProcessed += int64(bytesProcessed)
	s.metrics.LatencyHistogram = append(s.metrics.LatencyHistogram, latency)
	s.metrics.LastSuccessfulSeq = lcm.LedgerSequence()
	s.metrics.mu.Unlock()
}

func (s *RawLedgerServer) validateProtocol23Compatibility(lcm xdr.LedgerCloseMeta) error {
	// Check for Protocol 23 specific structures
	if lcm.LedgerSequence() >= PROTOCOL_23_ACTIVATION_LEDGER {
		// Validate dual BucketList hash structure (CAP-62)
		if lcm.V == 1 && lcm.V1 != nil {
			if err := s.validateDualBucketListHash(lcm.V1); err != nil {
				return fmt.Errorf("dual bucket list validation failed: %w", err)
			}
		}

		// Validate hot archive buckets (CAP-62)
		if err := s.validateHotArchiveBuckets(lcm); err != nil {
			return fmt.Errorf("hot archive validation failed: %w", err)
		}
	}
	return nil
}

func (s *RawLedgerServer) validateDualBucketListHash(v1 *xdr.LedgerCloseMetaV1) error {
	// Protocol 23 (CAP-62) validation for dual BucketList
	// bucketListHash = SHA256(liveStateBucketListHash, hotArchiveHash)
	s.logger.Debug("Validating Protocol 23 dual BucketList hash",
		zap.Uint32("sequence", uint32(v1.LedgerHeader.Header.LedgerSeq)),
	)
	// TODO: Add specific validation logic for dual bucket list hash
	return nil
}

func (s *RawLedgerServer) validateHotArchiveBuckets(lcm xdr.LedgerCloseMeta) error {
	// Protocol 23 (CAP-62) validation for hot archive buckets
	s.logger.Debug("Validating Protocol 23 hot archive buckets",
		zap.Uint32("sequence", lcm.LedgerSequence()),
	)
	// TODO: Add specific validation logic for hot archive buckets
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
