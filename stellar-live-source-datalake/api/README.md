# Stellar Live Source Datalake API Documentation

## Overview

The Stellar Live Source Datalake service provides enterprise-grade access to historical Stellar ledger data stored in cloud storage. It implements a robust streaming service with comprehensive monitoring, fault tolerance, and enterprise features.

## Features

### Enterprise Reliability
- Circuit breaker pattern for fault tolerance
- Exponential backoff with jitter for retries
- Comprehensive error handling and classification
- Context-aware operation cancellation

### Monitoring & Observability
- Structured logging with zap
- Comprehensive metrics collection
- Health check endpoint with detailed status
- Performance guarantees and SLAs

### Security
- API key authentication
- JWT bearer token support
- Secure gRPC communication
- Environment-based configuration

## API Specification

The API is documented using OpenAPI 3.0. See [openapi.yaml](./openapi.yaml) for the complete specification.

### gRPC Service

The primary interface for this service is gRPC, defined in the protocol buffers file. The service provides a streaming interface for historical ledger data access.

#### Service Definition
```protobuf
service RawLedgerService {
  // Streams raw ledger data from the specified sequence number
  rpc StreamRawLedgers(StreamLedgersRequest) returns (stream RawLedger) {}
}
```

#### Message Types
```protobuf
message StreamLedgersRequest {
  uint32 start_ledger = 1;
  optional uint32 end_ledger = 2;
}

message RawLedger {
  uint32 sequence = 1;
  bytes ledger_close_meta_xdr = 2;
}
```

#### Error Handling
The service uses standard gRPC error codes for error reporting. See the OpenAPI specification for a complete list of error codes and their meanings.

#### Client Implementation
Example client implementation in Go:
```go
conn, err := grpc.Dial("localhost:50052", grpc.WithInsecure())
if err != nil {
    log.Fatalf("did not connect: %v", err)
}
defer conn.Close()

client := pb.NewRawLedgerServiceClient(conn)
stream, err := client.StreamRawLedgers(context.Background(), &pb.StreamLedgersRequest{
    StartLedger: 1000000,
    EndLedger: 1000100, // Optional: specify end ledger for bounded range
})
if err != nil {
    log.Fatalf("could not stream ledgers: %v", err)
}

for {
    ledger, err := stream.Recv()
    if err == io.EOF {
        break
    }
    if err != nil {
        log.Fatalf("error receiving ledger: %v", err)
    }
    // Process ledger...
}
```

### Key Endpoints

#### Health Check
```
GET /health
```
Returns the current health status and comprehensive metrics of the service.

#### Raw Ledger Streaming
The service implements a gRPC streaming interface for efficient ledger data delivery.

## Configuration

### Environment Variables
| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| STORAGE_TYPE | Storage backend type (GCS, S3, FS) | Yes | - |
| BUCKET_NAME | Storage bucket/path name | Yes | - |
| LEDGERS_PER_FILE | Number of ledgers per file | No | 64 |
| FILES_PER_PARTITION | Number of files per partition | No | 10 |
| HEALTH_PORT | Health check server port | No | 8080 |
| AWS_REGION | AWS region for S3 | No* | - |
| S3_ENDPOINT_URL | Custom S3 endpoint URL | No | - |
| S3_FORCE_PATH_STYLE | Force path style for S3 | No | false |

*Required for S3 storage type

## Performance Guarantees

The service provides the following guarantees:
- 99.99% uptime
- P99 latency < 100ms
- Maximum retry latency < 1s
- Circuit breaker activation after 5 consecutive failures
- 30-second circuit breaker reset timeout

## Error Handling

The service implements comprehensive error handling:
- Error classification and tracking
- Retry strategies with exponential backoff
- Circuit breaker pattern for fault tolerance
- Detailed error reporting in health checks

## Monitoring

The service provides monitoring through:
- Health check endpoint with detailed metrics
- Structured logging with contextual information
- Performance metrics collection
- Error tracking and classification

## Security

The service implements:
- API key authentication
- JWT bearer token support
- Secure gRPC communication
- Environment-based configuration

## Getting Started

1. Set up the required environment variables
2. Start the service
3. Monitor the health check endpoint
4. Connect to the gRPC streaming interface

## Example Usage

```bash
# Start the service
STORAGE_TYPE=GCS \
BUCKET_NAME=my-stellar-ledgers \
LEDGERS_PER_FILE=64 \
FILES_PER_PARTITION=10 \
./stellar_live_source_datalake

# Check health status
curl http://localhost:8080/health
```

## Support

For support, contact:
- Email: support@obsrvr.com
- Documentation: https://docs.obsrvr.com
- GitHub Issues: https://github.com/withObsrvr/ttp-processor-demo/issues 