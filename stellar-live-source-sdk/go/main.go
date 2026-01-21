package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/withObsrvr/flowctl-sdk/pkg/source"
	flowctlv1 "github.com/withObsrvr/flow-proto/go/gen/flowctl/v1"
	stellarv1 "github.com/withObsrvr/flow-proto/go/gen/stellar/v1"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stellar/go-stellar-sdk/xdr"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func main() {
	// Initialize logger
	logger, err := zap.NewProduction()
	if err != nil {
		panic("failed to initialize logger: " + err.Error())
	}
	defer logger.Sync()

	// Load configuration from environment
	config := source.DefaultConfig()
	// flowctl sets FLOWCTL_COMPONENT_ID, fall back to COMPONENT_ID for backwards compatibility
	config.ID = getEnv("FLOWCTL_COMPONENT_ID", getEnv("COMPONENT_ID", "stellar-live-source"))
	config.Name = "Stellar Live Source"
	config.Description = "Streams Stellar ledger data from various backends"
	config.Version = "2.0.0-sdk"
	config.Endpoint = getEnv("PORT", ":50052")
	config.HealthPort = parseInt(getEnv("HEALTH_PORT", "8081"))
	config.BufferSize = parseInt(getEnv("BUFFER_SIZE", "100"))
	config.OutputEventTypes = []string{"stellar.ledger.v1"}

	// Configure flowctl integration
	if strings.ToLower(getEnv("ENABLE_FLOWCTL", "false")) == "true" {
		config.FlowctlConfig.Enabled = true
		config.FlowctlConfig.Endpoint = getEnv("FLOWCTL_ENDPOINT", "localhost:8080")
		config.FlowctlConfig.HeartbeatInterval = parseDuration(getEnv("FLOWCTL_HEARTBEAT_INTERVAL", "10s"))
	}

	// Load backend configuration
	backendConfig, err := loadBackendConfig()
	if err != nil {
		log.Fatalf("Failed to load backend configuration: %v", err)
	}

	logger.Info("Stellar Live Source starting",
		zap.String("backend_type", backendConfig.BackendType),
		zap.String("network", backendConfig.NetworkPassphrase))

	// Create source
	src, err := source.New(config)
	if err != nil {
		log.Fatalf("Failed to create source: %v", err)
	}

	// Register producer
	err = src.OnProduce(func(ctx context.Context, req *flowctlv1.StreamRequest) (<-chan *flowctlv1.Event, error) {
		return produceLedgers(ctx, backendConfig, req, logger, config.ID)
	})
	if err != nil {
		log.Fatalf("Failed to register producer: %v", err)
	}

	// Start the source
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := src.Start(ctx); err != nil {
		log.Fatalf("Failed to start source: %v", err)
	}

	logger.Info("Stellar Live Source is running",
		zap.String("endpoint", config.Endpoint),
		zap.Int("health_port", config.HealthPort),
		zap.Int("buffer_size", config.BufferSize),
		zap.Bool("flowctl_enabled", config.FlowctlConfig.Enabled))

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// Stop the source gracefully
	logger.Info("Shutting down source...")
	if err := src.Stop(); err != nil {
		log.Fatalf("Failed to stop source: %v", err)
	}

	logger.Info("Source stopped successfully")
}

// BackendConfig holds configuration for different backend types
type BackendConfig struct {
	BackendType        string
	NetworkPassphrase  string
	HistoryArchiveURLs []string

	// Captive Core specific
	StellarCoreBinaryPath string
	StellarCoreConfigPath string

	// RPC specific
	RPCEndpoint       string
	RPCAuthHeader     string // Authorization header value (e.g., "Api-Key xxx" or "Bearer xxx")
	RPCCustomHeaders  map[string]string // Additional custom headers

	// Archive specific
	ArchiveStorageType string
	ArchiveBucketName  string
	ArchivePath        string
	LedgersPerFile     int
	FilesPerPartition  int
}

func loadBackendConfig() (*BackendConfig, error) {
	// Determine backend type (with backward compatibility)
	backendType := os.Getenv("BACKEND_TYPE")
	if backendType == "" {
		// Check for old STORAGE_TYPE configuration
		if oldStorageType := os.Getenv("STORAGE_TYPE"); oldStorageType != "" {
			switch oldStorageType {
			case "GCS", "S3":
				backendType = "ARCHIVE"
			default:
				backendType = "CAPTIVE_CORE"
			}
		} else {
			backendType = "CAPTIVE_CORE"
		}
	}

	config := &BackendConfig{
		BackendType:       backendType,
		NetworkPassphrase: getEnv("NETWORK_PASSPHRASE", "Public Network ; September 2015"),
	}

	// Parse history archive URLs
	if urls := getEnv("HISTORY_ARCHIVE_URLS", ""); urls != "" {
		config.HistoryArchiveURLs = strings.Split(urls, ",")
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

		// Load authorization header
		config.RPCAuthHeader = os.Getenv("RPC_AUTH_HEADER")

		// Load custom headers from comma-separated key:value pairs
		// Format: HEADER1:VALUE1,HEADER2:VALUE2
		if customHeaders := os.Getenv("RPC_CUSTOM_HEADERS"); customHeaders != "" {
			config.RPCCustomHeaders = make(map[string]string)
			for _, pair := range strings.Split(customHeaders, ",") {
				parts := strings.SplitN(strings.TrimSpace(pair), ":", 2)
				if len(parts) == 2 {
					config.RPCCustomHeaders[parts[0]] = parts[1]
				}
			}
		}

	case "ARCHIVE":
		config.ArchiveStorageType = getEnv("ARCHIVE_STORAGE_TYPE", os.Getenv("STORAGE_TYPE"))
		config.ArchiveBucketName = getEnv("ARCHIVE_BUCKET_NAME", os.Getenv("BUCKET_NAME"))
		config.ArchivePath = getEnv("ARCHIVE_PATH", getEnv("BUCKET_PATH", ""))
		config.LedgersPerFile = parseInt(getEnv("LEDGERS_PER_FILE", "1"))
		config.FilesPerPartition = parseInt(getEnv("FILES_PER_PARTITION", "64000"))

		if config.ArchiveStorageType == "" || config.ArchiveBucketName == "" {
			return nil, fmt.Errorf("STORAGE_TYPE and BUCKET_NAME required for ARCHIVE backend")
		}

	default:
		return nil, fmt.Errorf("unsupported backend type: %s", config.BackendType)
	}

	return config, nil
}

func createBackend(ctx context.Context, config *BackendConfig, logger *zap.Logger) (ledgerbackend.LedgerBackend, error) {
	switch config.BackendType {
	case "CAPTIVE_CORE":
		return createCaptiveCore(config, logger)
	case "RPC":
		return createRPCBackend(config, logger)
	case "ARCHIVE":
		return createArchiveBackend(config, logger)
	default:
		return nil, fmt.Errorf("unsupported backend type: %s", config.BackendType)
	}
}

func createCaptiveCore(config *BackendConfig, logger *zap.Logger) (ledgerbackend.LedgerBackend, error) {
	captiveCoreToml, err := ledgerbackend.NewCaptiveCoreToml(
		ledgerbackend.CaptiveCoreTomlParams{
			NetworkPassphrase:  config.NetworkPassphrase,
			HistoryArchiveURLs: config.HistoryArchiveURLs,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create captive core toml: %w", err)
	}

	cfg := ledgerbackend.CaptiveCoreConfig{
		BinaryPath:          config.StellarCoreBinaryPath,
		NetworkPassphrase:   config.NetworkPassphrase,
		HistoryArchiveURLs:  config.HistoryArchiveURLs,
		CheckpointFrequency: 64,
		Toml:                captiveCoreToml,
	}

	logger.Info("Creating Captive Core backend",
		zap.String("binary_path", cfg.BinaryPath))

	return ledgerbackend.NewCaptive(cfg)
}

func createRPCBackend(config *BackendConfig, logger *zap.Logger) (ledgerbackend.LedgerBackend, error) {
	options := ledgerbackend.RPCLedgerBackendOptions{
		RPCServerURL: config.RPCEndpoint,
		BufferSize:   10,
	}

	// Create custom HTTP client with authorization headers if configured
	if config.RPCAuthHeader != "" || len(config.RPCCustomHeaders) > 0 {
		transport := &authTransport{
			base:          http.DefaultTransport,
			authHeader:    config.RPCAuthHeader,
			customHeaders: config.RPCCustomHeaders,
		}
		options.HttpClient = &http.Client{
			Transport: transport,
		}

		logger.Info("Creating RPC backend with authentication",
			zap.String("rpc_endpoint", config.RPCEndpoint),
			zap.Bool("has_auth_header", config.RPCAuthHeader != ""),
			zap.Int("custom_headers_count", len(config.RPCCustomHeaders)))
	} else {
		logger.Info("Creating RPC backend",
			zap.String("rpc_endpoint", config.RPCEndpoint))
	}

	return ledgerbackend.NewRPCLedgerBackend(options), nil
}

// authTransport is an http.RoundTripper that adds authorization headers
type authTransport struct {
	base          http.RoundTripper
	authHeader    string
	customHeaders map[string]string
}

func (t *authTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Clone the request to avoid modifying the original
	reqClone := req.Clone(req.Context())

	// Add authorization header if configured
	if t.authHeader != "" {
		reqClone.Header.Set("Authorization", t.authHeader)
	}

	// Add custom headers
	for key, value := range t.customHeaders {
		reqClone.Header.Set(key, value)
	}

	return t.base.RoundTrip(reqClone)
}

func createArchiveBackend(config *BackendConfig, logger *zap.Logger) (ledgerbackend.LedgerBackend, error) {
	// Create datastore schema
	schema := datastore.DataStoreSchema{
		LedgersPerFile:    uint32(config.LedgersPerFile),
		FilesPerPartition: uint32(config.FilesPerPartition),
	}

	// Setup datastore parameters based on storage type
	dsParams := make(map[string]string)
	var dsConfig datastore.DataStoreConfig

	switch strings.ToUpper(config.ArchiveStorageType) {
	case "GCS":
		// Construct full path including archive path
		fullPath := config.ArchiveBucketName
		if config.ArchivePath != "" {
			fullPath = config.ArchiveBucketName + "/" + config.ArchivePath
		}
		dsParams["destination_bucket_path"] = fullPath
		dsConfig = datastore.DataStoreConfig{Type: "GCS", Schema: schema, Params: dsParams}

	case "S3":
		dsParams["bucket_name"] = config.ArchiveBucketName
		dsParams["region"] = getEnv("AWS_REGION", "us-east-1")
		if endpoint := os.Getenv("S3_ENDPOINT_URL"); endpoint != "" {
			dsParams["endpoint"] = endpoint
			dsParams["force_path_style"] = os.Getenv("S3_FORCE_PATH_STYLE")
		}
		dsConfig = datastore.DataStoreConfig{Type: "S3", Schema: schema, Params: dsParams}

	default:
		return nil, fmt.Errorf("unsupported archive storage type: %s", config.ArchiveStorageType)
	}

	logger.Info("Creating Archive backend",
		zap.String("storage_type", config.ArchiveStorageType),
		zap.String("bucket", config.ArchiveBucketName),
		zap.String("path", config.ArchivePath),
		zap.Int("ledgers_per_file", config.LedgersPerFile),
		zap.Int("files_per_partition", config.FilesPerPartition))

	// Create the datastore
	dataStore, err := datastore.NewDataStore(context.Background(), dsConfig)
	if err != nil {
		logger.Error("Failed to create datastore",
			zap.String("storage_type", config.ArchiveStorageType),
			zap.Error(err))
		return nil, fmt.Errorf("failed to create datastore: %w", err)
	}

	// Create BufferedStorageBackend configuration
	bufferedConfig := ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: 100,
		NumWorkers: 5,
		RetryLimit: 3,
		RetryWait:  time.Second,
	}

	// Create the BufferedStorageBackend
	backend, err := ledgerbackend.NewBufferedStorageBackend(bufferedConfig, dataStore, schema)
	if err != nil {
		dataStore.Close()
		return nil, fmt.Errorf("failed to create buffered storage backend: %w", err)
	}

	logger.Info("Successfully created Archive backend")
	return backend, nil
}

// produceLedgers implements the producer function for the Source SDK
func produceLedgers(
	ctx context.Context,
	config *BackendConfig,
	req *flowctlv1.StreamRequest,
	logger *zap.Logger,
	componentID string,
) (<-chan *flowctlv1.Event, error) {
	// Create event channel
	eventCh := make(chan *flowctlv1.Event, 100)

	// Determine start and end ledger from params
	startLedger := uint32(1)
	if startLedgerStr, ok := req.Params["start_ledger"]; ok {
		if parsed, err := strconv.ParseUint(startLedgerStr, 10, 32); err == nil {
			startLedger = uint32(parsed)
		}
	}

	// Optional end ledger for bounded ranges
	var endLedger *uint32
	if endLedgerStr, ok := req.Params["end_ledger"]; ok {
		if parsed, err := strconv.ParseUint(endLedgerStr, 10, 32); err == nil {
			end := uint32(parsed)
			endLedger = &end
		}
	}

	if endLedger != nil {
		logger.Info("Starting ledger production",
			zap.Uint32("start_ledger", startLedger),
			zap.Uint32("end_ledger", *endLedger),
			zap.String("backend_type", config.BackendType))
	} else {
		logger.Info("Starting ledger production (unbounded)",
			zap.Uint32("start_ledger", startLedger),
			zap.String("backend_type", config.BackendType))
	}

	// Start background goroutine to stream ledgers
	go func() {
		defer close(eventCh)

		// Create backend
		backend, err := createBackend(ctx, config, logger)
		if err != nil {
			logger.Error("Failed to create backend", zap.Error(err))
			return
		}
		defer backend.Close()

		// Prepare range
		ledgerRange := ledgerbackend.UnboundedRange(startLedger)
		if err := backend.PrepareRange(ctx, ledgerRange); err != nil {
			logger.Error("Failed to prepare range", zap.Error(err))
			return
		}

		logger.Info("Backend prepared, starting ledger stream")

		// Stream ledgers
		for seq := startLedger; ; seq++ {
			// Check if we've reached the end ledger
			if endLedger != nil && seq > *endLedger {
				logger.Info("Reached end ledger, stopping stream",
					zap.Uint32("end_ledger", *endLedger))
				return
			}

			select {
			case <-ctx.Done():
				logger.Info("Context cancelled, stopping ledger stream",
					zap.Uint32("last_sequence", seq-1))
				return
			default:
			}

			// Get ledger with timeout
			ledgerCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			lcm, err := backend.GetLedger(ledgerCtx, seq)
			cancel()

			if err != nil {
				if err == context.DeadlineExceeded {
					logger.Warn("Ledger read timeout",
						zap.Uint32("sequence", seq),
						zap.String("backend_type", config.BackendType))
				} else {
					logger.Error("Failed to get ledger",
						zap.Uint32("sequence", seq),
						zap.Error(err))
				}
				// On error, wait and retry
				time.Sleep(1 * time.Second)
				continue
			}

			// Convert to proto event
			event, err := convertLedgerToEvent(lcm, componentID)
			if err != nil {
				logger.Error("Failed to convert ledger to event",
					zap.Uint32("sequence", seq),
					zap.Error(err))
				continue
			}

			// Send event
			select {
			case <-ctx.Done():
				return
			case eventCh <- event:
				if seq%100 == 0 {
					logger.Info("Streaming progress",
						zap.Uint32("sequence", seq))
				}
			}
		}
	}()

	return eventCh, nil
}

func convertLedgerToEvent(lcm xdr.LedgerCloseMeta, componentID string) (*flowctlv1.Event, error) {
	sequence := lcm.LedgerSequence()

	// Marshal XDR
	xdrBytes, err := lcm.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal ledger XDR: %w", err)
	}

	// Create RawLedger proto
	rawLedger := &stellarv1.RawLedger{
		Sequence:           sequence,
		LedgerCloseMetaXdr: xdrBytes,
	}

	// Marshal to protobuf
	payload, err := proto.Marshal(rawLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal RawLedger: %w", err)
	}

	// Create flowctl Event
	event := &flowctlv1.Event{
		Id:                fmt.Sprintf("stellar-ledger-%d", sequence),
		Type:              "stellar.ledger.v1",
		Payload:           payload,
		Metadata:          make(map[string]string),
		SourceComponentId: componentID,
		ContentType:       "application/protobuf",
		StellarCursor: &flowctlv1.StellarCursor{
			LedgerSequence: uint64(sequence),
		},
	}

	event.Metadata["ledger_sequence"] = fmt.Sprintf("%d", sequence)
	event.Metadata["xdr_size"] = fmt.Sprintf("%d", len(xdrBytes))

	return event, nil
}

// Helper functions
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func parseInt(s string) int {
	v, _ := strconv.Atoi(s)
	return v
}

func parseDuration(s string) time.Duration {
	d, _ := time.ParseDuration(s)
	return d
}
