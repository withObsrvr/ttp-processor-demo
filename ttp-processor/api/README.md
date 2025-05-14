# TTP Processor API Documentation

## Overview

The TTP Processor service provides enterprise-grade processing and transformation of Stellar ledger data. It implements a robust streaming service with comprehensive monitoring, fault tolerance, and enterprise features, enabling efficient data transformation and analysis.

## Features

### Enterprise Reliability
- Circuit breaker pattern for fault tolerance
- Exponential backoff with jitter for retries
- Comprehensive error handling and classification
- Context-aware operation cancellation
- Transformation pipeline management

### Monitoring & Observability
- Structured logging with zap
- Comprehensive metrics collection
- Health check endpoint with detailed status
- Performance guarantees and SLAs
- Transformation statistics tracking

### Security
- API key authentication
- JWT bearer token support
- Secure gRPC communication
- Environment-based configuration

## API Specification

The API is documented using OpenAPI 3.0. See [openapi.yaml](./openapi.yaml) for the complete specification.

### gRPC Service

The primary interface for this service is gRPC, defined in the protocol buffers file. The service provides a streaming interface for processing ledger data.

#### Service Definition
```protobuf
service TTPProcessor {
  // Processes raw ledger data and returns transformed data
  rpc ProcessLedgers(ProcessLedgersRequest) returns (stream ProcessedLedger) {}
}
```

#### Message Types
```protobuf
message ProcessLedgersRequest {
  uint32 start_ledger = 1;
  optional uint32 end_ledger = 2;
  repeated string transformations = 3; // List of transformations to apply
}

message ProcessedLedger {
  uint32 sequence = 1;
  bytes transformed_data = 2; // Transformed ledger data in specified format
  map<string, string> metadata = 3; // Additional metadata about the transformation
}
```

#### Error Handling
The service uses standard gRPC error codes for error reporting. See the OpenAPI specification for a complete list of error codes and their meanings.

#### Client Implementation
Example client implementation in Go:
```go
conn, err := grpc.Dial("localhost:50053", grpc.WithInsecure())
if err != nil {
    log.Fatalf("did not connect: %v", err)
}
defer conn.Close()

client := pb.NewTTPProcessorClient(conn)
stream, err := client.ProcessLedgers(context.Background(), &pb.ProcessLedgersRequest{
    StartLedger: 1000000,
    EndLedger: 1000100, // Optional: specify end ledger for bounded range
    Transformations: []string{"normalize", "enrich", "aggregate"}, // List of transformations to apply
})
if err != nil {
    log.Fatalf("could not process ledgers: %v", err)
}

for {
    processed, err := stream.Recv()
    if err == io.EOF {
        break
    }
    if err != nil {
        log.Fatalf("error receiving processed ledger: %v", err)
    }
    // Process transformed data...
}
```

### Key Endpoints

#### Health Check
```
GET /health
```
Returns the current health status and comprehensive metrics of the service, including transformation statistics.

## Configuration

### Environment Variables
| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| SOURCE_URL | URL of the source service (stellar-live-source or stellar-live-source-datalake) | Yes | - |
| SOURCE_TYPE | Type of source service (live or datalake) | Yes | - |
| HEALTH_PORT | Health check server port | No | 8080 |
| MAX_RETRIES | Maximum number of retries for processing | No | 5 |
| RETRY_DELAY | Initial retry delay in milliseconds | No | 1000 |
| CIRCUIT_BREAKER_THRESHOLD | Number of failures before circuit breaker opens | No | 5 |
| CIRCUIT_BREAKER_TIMEOUT | Circuit breaker reset timeout in seconds | No | 30 |
| TRANSFORMATIONS | Comma-separated list of default transformations | No | - |

## Performance Guarantees

The service provides the following guarantees:
- 99.99% uptime
- P99 latency < 100ms
- Maximum retry latency < 1s
- Circuit breaker activation after 5 consecutive failures
- 30-second circuit breaker reset timeout
- Transformation completion within 5 seconds

## Error Handling

The service implements comprehensive error handling:
- Error classification and tracking
- Retry strategies with exponential backoff
- Circuit breaker pattern for fault tolerance
- Detailed error reporting in health checks
- Transformation error recovery

## Monitoring

The service provides monitoring through:
- Health check endpoint with detailed metrics
- Structured logging with contextual information
- Performance metrics collection
- Error tracking and classification
- Transformation statistics tracking

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
SOURCE_URL=http://localhost:50051 \
SOURCE_TYPE=live \
TRANSFORMATIONS=normalize,enrich,aggregate \
./ttp-processor

# Check health status
curl http://localhost:8080/health
```

## Support

For support, contact:
- Email: support@obsrvr.com
- Documentation: https://docs.obsrvr.com
- GitHub Issues: https://github.com/withObsrvr/ttp-processor-demo/issues 