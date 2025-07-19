package main

import (
	"context"
	"os"
	"strings"

	"github.com/stellar/go/support/storage"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewDevelopment()
	
	storageType := os.Getenv("STORAGE_TYPE")
	bucketName := os.Getenv("BUCKET_NAME")
	archivePath := os.Getenv("ARCHIVE_PATH")
	endpoint := os.Getenv("GCS_ENDPOINT")
	
	logger.Info("Testing storage backend",
		zap.String("storage_type", storageType),
		zap.String("bucket", bucketName),
		zap.String("archive_path", archivePath),
		zap.String("endpoint", endpoint))
	
	ctx := context.Background()
	var storageBackend storage.Storage
	var err error

	switch strings.ToLower(storageType) {
	case "gcs":
		storageBackend, err = storage.NewGCSBackend(ctx, bucketName, archivePath, endpoint)
	default:
		logger.Fatal("Only GCS supported in this test")
	}

	if err != nil {
		logger.Fatal("Failed to create storage backend", zap.Error(err))
	}

	logger.Info("Storage backend created successfully")
	
	// Test listing files with empty path
	files, errors := storageBackend.ListFiles("")
	
	logger.Info("Starting file listing...")
	
	count := 0
	for file := range files {
		select {
		case err := <-errors:
			if err != nil {
				logger.Error("Error during listing", zap.Error(err))
			}
		default:
		}
		
		logger.Info("Found file/directory", zap.String("path", file))
		count++
		
		if count >= 10 {
			logger.Info("Stopping after 10 entries...")
			break
		}
	}
	
	logger.Info("Completed file listing", zap.Int("total_found", count))
}