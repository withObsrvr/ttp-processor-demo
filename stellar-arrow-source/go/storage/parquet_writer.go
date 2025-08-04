package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/logging"
)

// ParquetWriter handles writing Arrow data to Parquet files
type ParquetWriter struct {
	logger       *logging.ComponentLogger
	allocator    memory.Allocator
	basePath     string
	
	// Partitioning configuration
	partitionBy  []string
	partitionFormat string
	
	// File management
	maxFileSize  int64
	compression  string // Store as string, apply when creating writer
	
	// Writer state
	writers      map[string]*FileWriter
	mu           sync.RWMutex
	
	// Metrics
	filesWritten int64
	bytesWritten int64
	recordsWritten int64
}

// FileWriter manages a single Parquet file
type FileWriter struct {
	path         string
	file         *os.File
	writer       *pqarrow.FileWriter
	schema       *arrow.Schema
	rowsWritten  int64
	bytesWritten int64
	created      time.Time
	lastSizeCheck time.Time
	checkInterval int64 // Check size every N rows
}

// ParquetConfig configures the Parquet writer
type ParquetConfig struct {
	BasePath        string
	PartitionBy     []string
	PartitionFormat string // e.g., "2006/01/02" for daily partitions
	MaxFileSize     int64
	Compression     string // "snappy", "gzip", "lz4", "zstd", "none"
}

// NewParquetWriter creates a new Parquet writer
func NewParquetWriter(config *ParquetConfig, allocator memory.Allocator, logger *logging.ComponentLogger) (*ParquetWriter, error) {
	// Ensure base path exists
	if err := os.MkdirAll(config.BasePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base path: %w", err)
	}
	
	// Validate compression type
	compression := config.Compression
	switch compression {
	case "none", "snappy", "gzip", "lz4", "zstd":
		// Valid compression
	default:
		compression = "snappy"
	}
	
	pw := &ParquetWriter{
		logger:          logger,
		allocator:       allocator,
		basePath:        config.BasePath,
		partitionBy:     config.PartitionBy,
		partitionFormat: config.PartitionFormat,
		maxFileSize:     config.MaxFileSize,
		compression:     compression,
		writers:         make(map[string]*FileWriter),
	}
	
	if pw.maxFileSize <= 0 {
		pw.maxFileSize = 128 * 1024 * 1024 // 128MB default
	}
	
	logger.Info().
		Str("base_path", config.BasePath).
		Strs("partition_by", config.PartitionBy).
		Str("compression", config.Compression).
		Int64("max_file_size", pw.maxFileSize).
		Msg("Created Parquet writer")
	
	return pw, nil
}

// WriteRecord writes an Arrow record to a Parquet file
func (pw *ParquetWriter) WriteRecord(ctx context.Context, record arrow.Record, metadata map[string]string) error {
	// Determine partition path
	partitionPath := pw.getPartitionPath(record, metadata)
	
	pw.mu.Lock()
	defer pw.mu.Unlock()
	
	// Get or create writer for this partition
	writer, err := pw.getOrCreateWriter(partitionPath, record.Schema())
	if err != nil {
		return fmt.Errorf("failed to get writer: %w", err)
	}
	
	// Check file size BEFORE writing to see if we need to rotate
	// This prevents writing to a file that would exceed the limit
	if writer.bytesWritten > 0 && writer.bytesWritten >= pw.maxFileSize {
		pw.logger.Info().
			Str("path", writer.path).
			Int64("rows", writer.rowsWritten).
			Int64("bytes", writer.bytesWritten).
			Int64("max_size", pw.maxFileSize).
			Msg("Rotating Parquet file due to size limit (pre-write check)")
		
		// Close the current writer
		if err := pw.closeWriter(partitionPath); err != nil {
			// Log the error but continue - we'll create a new writer
			pw.logger.Warn().
				Err(err).
				Str("path", writer.path).
				Msg("Error closing writer during rotation, continuing")
		}
		
		// Get a new writer for the next file
		writer, err = pw.getOrCreateWriter(partitionPath, record.Schema())
		if err != nil {
			return fmt.Errorf("failed to create new writer after rotation: %w", err)
		}
	}
	
	// Write the record
	if err := writer.writer.Write(record); err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}
	
	writer.rowsWritten += record.NumRows()
	pw.recordsWritten += record.NumRows()
	
	// Check file size periodically to avoid expensive sync on every write
	shouldCheckSize := false
	if writer.checkInterval <= 0 {
		writer.checkInterval = 100 // Default: check every 100 rows
	}
	
	// Check size based on rows written or time elapsed
	if writer.rowsWritten%writer.checkInterval == 0 || 
	   time.Since(writer.lastSizeCheck) > 5*time.Second ||
	   writer.rowsWritten < 10 { // Always check for first few rows
		shouldCheckSize = true
	}
	
	if shouldCheckSize {
		// Sync to ensure data is written to disk
		if err := writer.file.Sync(); err != nil {
			// If sync fails, it might be because the file is closed
			// Just log and continue
			pw.logger.Debug().
				Err(err).
				Str("path", writer.path).
				Msg("Failed to sync file, may be closed")
		} else {
			// Update actual file size
			if info, err := writer.file.Stat(); err == nil {
				oldSize := writer.bytesWritten
				writer.bytesWritten = info.Size()
				writer.lastSizeCheck = time.Now()
				
				// Update global bytes written
				if oldSize > 0 {
					pw.bytesWritten += (writer.bytesWritten - oldSize)
				} else {
					// First time checking this file
					pw.bytesWritten += writer.bytesWritten
				}
				
				// Log progress periodically (every 10MB or when approaching limit)
				if writer.bytesWritten-oldSize > 10*1024*1024 || writer.bytesWritten > pw.maxFileSize*9/10 {
					pw.logger.Debug().
						Str("path", writer.path).
						Int64("bytes", writer.bytesWritten).
						Int64("max_size", pw.maxFileSize).
						Int64("rows", writer.rowsWritten).
						Float64("percent_full", float64(writer.bytesWritten)*100/float64(pw.maxFileSize)).
						Msg("Parquet file size update")
				}
			} else {
				pw.logger.Debug().
					Err(err).
					Str("path", writer.path).
					Msg("Failed to get file size, rotation may be delayed")
			}
		}
	}
	
	return nil
}

// getPartitionPath determines the partition path for a record
func (pw *ParquetWriter) getPartitionPath(record arrow.Record, metadata map[string]string) string {
	if len(pw.partitionBy) == 0 {
		return "default"
	}
	
	// Simple date-based partitioning
	if pw.partitionFormat != "" {
		// Look for timestamp column
		for i, field := range record.Schema().Fields() {
			if field.Name == "ledger_close_time" && field.Type.ID() == arrow.TIMESTAMP {
				col := record.Column(i)
				if col.Len() > 0 {
					timestampCol := col.(*array.Timestamp)
					if !timestampCol.IsNull(0) {
						ts := timestampCol.Value(0)
						t := time.Unix(0, int64(ts)*1000) // Convert microseconds to nanoseconds
						return t.Format(pw.partitionFormat)
					}
				}
			}
		}
	}
	
	// Fallback to metadata-based partitioning
	if ledgerSeq, ok := metadata["ledger_sequence"]; ok {
		// Partition by ledger range (e.g., every 10000 ledgers)
		return fmt.Sprintf("ledger_%s", ledgerSeq[:len(ledgerSeq)-4])
	}
	
	return "default"
}

// getOrCreateWriter gets an existing writer or creates a new one
func (pw *ParquetWriter) getOrCreateWriter(partitionPath string, schema *arrow.Schema) (*FileWriter, error) {
	if writer, exists := pw.writers[partitionPath]; exists {
		return writer, nil
	}
	
	// Create partition directory
	dirPath := filepath.Join(pw.basePath, partitionPath)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create partition directory: %w", err)
	}
	
	// Generate filename with timestamp
	filename := fmt.Sprintf("data_%s.parquet", time.Now().Format("20060102_150405"))
	filePath := filepath.Join(dirPath, filename)
	
	// Create file
	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}
	
	// Create Parquet writer properties
	// For now, use default compression until we figure out the exact API
	props := parquet.NewWriterProperties(
		parquet.WithDictionaryDefault(true),
		parquet.WithDataPageSize(1024*1024), // 1MB data pages
		parquet.WithCreatedBy("stellar-arrow-source"),
	)
	
	// Create Arrow writer properties
	arrowProps := pqarrow.NewArrowWriterProperties(
		pqarrow.WithStoreSchema(),
	)
	
	// Create Parquet writer
	writer, err := pqarrow.NewFileWriter(schema, file, props, arrowProps)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to create Parquet writer: %w", err)
	}
	
	fw := &FileWriter{
		path:          filePath,
		file:          file,
		writer:        writer,
		schema:        schema,
		created:       time.Now(),
		lastSizeCheck: time.Now(),
		checkInterval: 100, // Check every 100 rows
	}
	
	pw.writers[partitionPath] = fw
	pw.filesWritten++
	
	pw.logger.Info().
		Str("path", filePath).
		Str("partition", partitionPath).
		Int("schema_fields", len(schema.Fields())).
		Msg("Created new Parquet file")
	
	return fw, nil
}

// closeWriter closes a specific writer
func (pw *ParquetWriter) closeWriter(partitionPath string) error {
	writer, exists := pw.writers[partitionPath]
	if !exists {
		return nil
	}
	
	// Remove from map first to prevent any concurrent access
	delete(pw.writers, partitionPath)
	
	// Close Parquet writer (this also closes the underlying file)
	if err := writer.writer.Close(); err != nil {
		// Log the error but don't fail - the writer is already removed from the map
		pw.logger.Warn().
			Err(err).
			Str("path", writer.path).
			Msg("Error closing Parquet writer, but continuing")
	}
	
	pw.logger.Info().
		Str("path", writer.path).
		Int64("rows", writer.rowsWritten).
		Int64("bytes", writer.bytesWritten).
		Dur("duration", time.Since(writer.created)).
		Msg("Closed Parquet file")
	
	return nil
}

// Close closes all open writers
func (pw *ParquetWriter) Close() error {
	pw.mu.Lock()
	defer pw.mu.Unlock()
	
	var errs []error
	for partition := range pw.writers {
		if err := pw.closeWriter(partition); err != nil {
			errs = append(errs, err)
		}
	}
	
	pw.logger.Info().
		Int64("files_written", pw.filesWritten).
		Int64("bytes_written", pw.bytesWritten).
		Int64("records_written", pw.recordsWritten).
		Msg("Closed Parquet writer")
	
	if len(errs) > 0 {
		return fmt.Errorf("errors closing writers: %v", errs)
	}
	return nil
}

// GetStats returns writer statistics
func (pw *ParquetWriter) GetStats() ParquetStats {
	pw.mu.RLock()
	defer pw.mu.RUnlock()
	
	activeFiles := make([]string, 0, len(pw.writers))
	for _, writer := range pw.writers {
		activeFiles = append(activeFiles, writer.path)
	}
	
	return ParquetStats{
		FilesWritten:   pw.filesWritten,
		BytesWritten:   pw.bytesWritten,
		RecordsWritten: pw.recordsWritten,
		ActiveFiles:    activeFiles,
	}
}

// ParquetStats contains writer statistics
type ParquetStats struct {
	FilesWritten   int64
	BytesWritten   int64
	RecordsWritten int64
	ActiveFiles    []string
}

// ParquetArchiver provides scheduled archival of Arrow data to Parquet
type ParquetArchiver struct {
	writer   *ParquetWriter
	logger   *logging.ComponentLogger
	
	// Archival configuration
	schedule     time.Duration
	bufferSize   int
	
	// Buffer for records
	buffer       []arrow.Record
	bufferMu     sync.Mutex
	
	// Control
	stopCh       chan struct{}
	wg           sync.WaitGroup
}

// NewParquetArchiver creates a new archiver
func NewParquetArchiver(writer *ParquetWriter, schedule time.Duration, bufferSize int, logger *logging.ComponentLogger) *ParquetArchiver {
	return &ParquetArchiver{
		writer:     writer,
		logger:     logger,
		schedule:   schedule,
		bufferSize: bufferSize,
		buffer:     make([]arrow.Record, 0, bufferSize),
		stopCh:     make(chan struct{}),
	}
}

// Start starts the archiver
func (pa *ParquetArchiver) Start() {
	pa.wg.Add(1)
	go pa.archiveLoop()
	
	pa.logger.Info().
		Dur("schedule", pa.schedule).
		Int("buffer_size", pa.bufferSize).
		Msg("Started Parquet archiver")
}

// Stop stops the archiver
func (pa *ParquetArchiver) Stop() {
	close(pa.stopCh)
	pa.wg.Wait()
	
	// Flush remaining buffer
	pa.flush()
	
	pa.logger.Info().
		Msg("Stopped Parquet archiver")
}

// AddRecord adds a record to the archive buffer
func (pa *ParquetArchiver) AddRecord(record arrow.Record) {
	pa.bufferMu.Lock()
	defer pa.bufferMu.Unlock()
	
	record.Retain()
	pa.buffer = append(pa.buffer, record)
	
	// Flush if buffer is full
	if len(pa.buffer) >= pa.bufferSize {
		// Create a copy of the buffer to flush
		recordsToFlush := make([]arrow.Record, len(pa.buffer))
		copy(recordsToFlush, pa.buffer)
		pa.buffer = make([]arrow.Record, 0, pa.bufferSize)
		// Don't unlock here - defer will handle it
		
		// Flush synchronously (still holding the lock, but that's ok for small buffer)
		pa.flushRecords(recordsToFlush)
		return
	}
}

// archiveLoop runs the archival loop
func (pa *ParquetArchiver) archiveLoop() {
	defer pa.wg.Done()
	
	ticker := time.NewTicker(pa.schedule)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			pa.flush()
		case <-pa.stopCh:
			return
		}
	}
}

// flush writes buffered records to Parquet
func (pa *ParquetArchiver) flush() {
	pa.bufferMu.Lock()
	if len(pa.buffer) == 0 {
		pa.bufferMu.Unlock()
		return
	}
	
	records := pa.buffer
	pa.buffer = make([]arrow.Record, 0, pa.bufferSize)
	pa.bufferMu.Unlock()
	
	pa.flushRecords(records)
}

// flushRecords writes a batch of records to Parquet
func (pa *ParquetArchiver) flushRecords(records []arrow.Record) {
	ctx := context.Background()
	for _, record := range records {
		if err := pa.writer.WriteRecord(ctx, record, nil); err != nil {
			pa.logger.Error().
				Err(err).
				Msg("Failed to write record to Parquet")
			// Continue with other records even if one fails
		}
		record.Release()
	}
	
	pa.logger.Debug().
		Int("records", len(records)).
		Msg("Flushed records to Parquet")
}