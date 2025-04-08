package server

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/stellar/go/xdr"
	cdp "github.com/withObsrvr/stellar-cdp"
	datastore "github.com/withObsrvr/stellar-datastore"
	ledgerbackend "github.com/withObsrvr/stellar-ledgerbackend"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/stellar/ttp-processor-demo/stellar-live-source-datalake/gen/raw_ledger_service"
)

type RawLedgerServer struct {
	pb.UnimplementedRawLedgerServiceServer
}

func NewRawLedgerServer() *RawLedgerServer {
	return &RawLedgerServer{}
}

// StreamRawLedgers implements the gRPC StreamRawLedgers method by reading from storage
func (s *RawLedgerServer) StreamRawLedgers(req *pb.StreamLedgersRequest, stream pb.RawLedgerService_StreamRawLedgersServer) error {
	ctx := stream.Context()
	log.Printf("Received StreamRawLedgers request: StartLedger=%d", req.StartLedger)

	// --- Configuration from Environment Variables ---
	storageType := os.Getenv("STORAGE_TYPE") // e.g., "GCS", "S3", "FS"
	bucketName := os.Getenv("BUCKET_NAME")   // e.g., "my-stellar-ledgers" or "/path/to/ledgers" for FS
	region := os.Getenv("AWS_REGION")
	endpoint := os.Getenv("S3_ENDPOINT_URL") // For S3 compatible storage

	// Schema config (defaults match BufferedStorageSourceAdapter examples)
	ledgersPerFile := uint32(getEnvAsUint("LEDGERS_PER_FILE", 64))
	filesPerPartition := uint32(getEnvAsUint("FILES_PER_PARTITION", 10))

	if storageType == "" || bucketName == "" {
		log.Printf("Error: STORAGE_TYPE and BUCKET_NAME environment variables are required.")
		return status.Error(codes.InvalidArgument, "STORAGE_TYPE and BUCKET_NAME environment variables are required")
	}
	log.Printf("Using Storage: Type=%s, Bucket/Path=%s, LedgersPerFile=%d, FilesPerPartition=%d",
		storageType, bucketName, ledgersPerFile, filesPerPartition)

	// --- Setup Storage Backend ---
	schema := datastore.DataStoreSchema{
		LedgersPerFile:    ledgersPerFile,
		FilesPerPartition: filesPerPartition,
	}

	dsParams := map[string]string{}
	var dsConfig datastore.DataStoreConfig

	switch storageType {
	case "GCS":
		dsParams["destination_bucket_path"] = bucketName
		dsConfig = datastore.DataStoreConfig{Type: "GCS", Schema: schema, Params: dsParams}
	case "S3":
		dsParams["bucket_name"] = bucketName
		dsParams["region"] = region
		if endpoint != "" {
			dsParams["endpoint"] = endpoint
			dsParams["force_path_style"] = os.Getenv("S3_FORCE_PATH_STYLE")
		}
		dsConfig = datastore.DataStoreConfig{Type: "S3", Schema: schema, Params: dsParams}
	case "FS":
		dsParams["base_path"] = bucketName
		dsConfig = datastore.DataStoreConfig{Type: "FS", Schema: schema, Params: dsParams}
	default:
		log.Printf("Error: Unsupported STORAGE_TYPE: %s", storageType)
		return status.Error(codes.InvalidArgument, fmt.Sprintf("unsupported STORAGE_TYPE: %s", storageType))
	}

	// --- Setup CDP Publisher Config ---
	bufferedConfig := cdp.DefaultBufferedStorageBackendConfig(schema.LedgersPerFile)
	publisherConfig := cdp.PublisherConfig{
		DataStoreConfig:       dsConfig,
		BufferedStorageConfig: bufferedConfig,
	}

	// --- Define Ledger Range ---
	ledgerRange := ledgerbackend.UnboundedRange(uint32(req.StartLedger))
	log.Printf("Processing unbounded ledger range (available data) starting from: %d", req.StartLedger)

	// --- Process Ledgers from Storage ---
	log.Printf("Starting processing using cdp.ApplyLedgerMetadata...")
	processedCount := 0
	startTime := time.Now()

	err := cdp.ApplyLedgerMetadata(
		ledgerRange,
		publisherConfig,
		ctx,
		func(lcm xdr.LedgerCloseMeta) error {
			// Check for context cancellation
			select {
			case <-ctx.Done():
				log.Printf("Context cancelled during ApplyLedgerMetadata callback for ledger %d", lcm.LedgerSequence())
				return ctx.Err()
			default:
			}

			log.Printf("Processing ledger %d from storage", lcm.LedgerSequence())

			// Convert LedgerCloseMeta to raw bytes
			rawBytes, err := lcm.MarshalBinary()
			if err != nil {
				log.Printf("Error marshaling ledger %d: %v", lcm.LedgerSequence(), err)
				return fmt.Errorf("error marshaling ledger %d: %w", lcm.LedgerSequence(), err)
			}

			// Create and send the RawLedger message
			rawLedger := &pb.RawLedger{
				Sequence:           uint32(lcm.LedgerSequence()),
				LedgerCloseMetaXdr: rawBytes,
			}

			if err := stream.Send(rawLedger); err != nil {
				log.Printf("Error sending ledger %d to stream: %v", lcm.LedgerSequence(), err)
				return fmt.Errorf("error sending ledger to stream: %w", err)
			}

			processedCount++
			if processedCount%100 == 0 {
				log.Printf("Processed %d ledgers...", processedCount)
			}
			return nil
		},
	)

	elapsed := time.Since(startTime)
	log.Printf("Finished processing %d ledgers from storage in %s", processedCount, elapsed)

	if err != nil {
		if err == context.Canceled || err == context.DeadlineExceeded {
			log.Printf("Ledger processing stopped due to context cancellation: %v", err)
			return nil
		}
		log.Printf("Error during cdp.ApplyLedgerMetadata: %v", err)
		return err
	}

	log.Println("Successfully completed ledger processing.")
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
		log.Printf("Warning: Invalid value for %s: %s. Using default: %d", key, valStr, defaultVal)
		return defaultVal
	}
	return val
}
