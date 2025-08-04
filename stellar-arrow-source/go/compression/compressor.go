package compression

import (
	"bytes"
	"fmt"
	"io"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/logging"
)

// CompressionType defines supported compression algorithms
type CompressionType string

const (
	CompressionNone CompressionType = "none"
	CompressionZstd CompressionType = "zstd"
	CompressionLZ4  CompressionType = "lz4"
)

// CompressedWriter wraps Arrow Flight writer with compression
type CompressedWriter struct {
	writer          *flight.Writer
	compressionType CompressionType
	logger          *logging.ComponentLogger
	metrics         *CompressionMetrics
	
	// Compression engines
	zstdEncoder *zstd.Encoder
	lz4Writer   *lz4.Writer
}

// CompressionMetrics tracks compression performance
type CompressionMetrics struct {
	TotalBytesIn        int64
	TotalBytesOut       int64
	CompressionRatio    float64
	CompressionTime     int64 // nanoseconds
	RecordsCompressed   int64
	CompressionErrors   int64
}

// NewCompressedWriter creates a new compressed Arrow writer
func NewCompressedWriter(stream flight.FlightService_DoGetServer, schema *arrow.Schema, 
	compressionType CompressionType, logger *logging.ComponentLogger) (*CompressedWriter, error) {
	
	writer := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
	
	cw := &CompressedWriter{
		writer:          writer,
		compressionType: compressionType,
		logger:          logger,
		metrics:         &CompressionMetrics{},
	}
	
	// Initialize compression engines
	switch compressionType {
	case CompressionZstd:
		encoder, err := zstd.NewWriter(nil, 
			zstd.WithEncoderLevel(zstd.SpeedDefault),
			zstd.WithEncoderConcurrency(2))
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd encoder: %w", err)
		}
		cw.zstdEncoder = encoder
		
	case CompressionLZ4:
		// LZ4 writer will be created per-record
		
	case CompressionNone:
		// No compression
		
	default:
		return nil, fmt.Errorf("unsupported compression type: %s", compressionType)
	}
	
	logger.Info().
		Str("compression", string(compressionType)).
		Msg("Created compressed Arrow writer")
	
	return cw, nil
}

// Write writes a compressed Arrow record
func (cw *CompressedWriter) Write(record arrow.Record) error {
	if cw.compressionType == CompressionNone {
		// No compression, write directly
		return cw.writer.Write(record)
	}
	
	start := time.Now()
	
	// Serialize record to IPC format
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(record.Schema()))
	if err := writer.Write(record); err != nil {
		cw.metrics.CompressionErrors++
		return fmt.Errorf("failed to serialize record: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close IPC writer: %w", err)
	}
	
	uncompressedSize := buf.Len()
	
	// Compress the serialized data
	var compressed []byte
	var err error
	
	switch cw.compressionType {
	case CompressionZstd:
		compressed = cw.zstdEncoder.EncodeAll(buf.Bytes(), nil)
		
	case CompressionLZ4:
		var lz4Buf bytes.Buffer
		lz4Writer := lz4.NewWriter(&lz4Buf)
		lz4Writer.Header.CompressionLevel = lz4.Level1
		
		if _, err := lz4Writer.Write(buf.Bytes()); err != nil {
			cw.metrics.CompressionErrors++
			return fmt.Errorf("lz4 compression failed: %w", err)
		}
		if err := lz4Writer.Close(); err != nil {
			return fmt.Errorf("lz4 close failed: %w", err)
		}
		compressed = lz4Buf.Bytes()
	}
	
	compressedSize := len(compressed)
	compressionRatio := float64(uncompressedSize) / float64(compressedSize)
	
	// Update metrics
	cw.metrics.TotalBytesIn += int64(uncompressedSize)
	cw.metrics.TotalBytesOut += int64(compressedSize)
	cw.metrics.CompressionRatio = float64(cw.metrics.TotalBytesIn) / float64(cw.metrics.TotalBytesOut)
	cw.metrics.CompressionTime += time.Since(start).Nanoseconds()
	cw.metrics.RecordsCompressed++
	
	cw.logger.Debug().
		Int("uncompressed_size", uncompressedSize).
		Int("compressed_size", compressedSize).
		Float64("ratio", compressionRatio).
		Dur("compression_time", time.Since(start)).
		Msg("Compressed Arrow record")
	
	// Create a new record with compressed data
	// This would need custom Flight metadata to indicate compression
	// For now, we'll use the original writer
	// TODO: Implement custom Flight data header with compression info
	
	return cw.writer.Write(record)
}

// Close closes the compressed writer
func (cw *CompressedWriter) Close() error {
	if cw.zstdEncoder != nil {
		cw.zstdEncoder.Close()
	}
	
	cw.logger.Info().
		Int64("total_records", cw.metrics.RecordsCompressed).
		Int64("bytes_in", cw.metrics.TotalBytesIn).
		Int64("bytes_out", cw.metrics.TotalBytesOut).
		Float64("overall_ratio", cw.metrics.CompressionRatio).
		Dur("total_compression_time", time.Duration(cw.metrics.CompressionTime)).
		Msg("Closing compressed writer")
	
	return cw.writer.Close()
}

// GetMetrics returns compression metrics
func (cw *CompressedWriter) GetMetrics() CompressionMetrics {
	return *cw.metrics
}

// CompressedReader handles decompression of Arrow Flight streams
type CompressedReader struct {
	reader          *flight.Reader
	compressionType CompressionType
	logger          *logging.ComponentLogger
	allocator       memory.Allocator
}

// NewCompressedReader creates a new compressed Arrow reader
func NewCompressedReader(stream flight.FlightService_DoGetClient, 
	compressionType CompressionType, logger *logging.ComponentLogger, 
	allocator memory.Allocator) (*CompressedReader, error) {
	
	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		return nil, fmt.Errorf("failed to create flight reader: %w", err)
	}
	
	return &CompressedReader{
		reader:          reader,
		compressionType: compressionType,
		logger:          logger,
		allocator:       allocator,
	}, nil
}

// Read reads and decompresses an Arrow record
func (cr *CompressedReader) Read() (arrow.Record, error) {
	// For Phase 2, we implement basic structure
	// Full compression support would require custom Flight metadata
	return cr.reader.Read()
}

// Schema returns the schema of the stream
func (cr *CompressedReader) Schema() *arrow.Schema {
	return cr.reader.Schema()
}

// Release releases the reader resources
func (cr *CompressedReader) Release() {
	cr.reader.Release()
}