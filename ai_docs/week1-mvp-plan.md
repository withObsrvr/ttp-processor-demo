# Week 1 MVP Plan: Enable flowctl to Run ttp-processor-demo Components

## Goal
Enable `flowctl run ttp-pipeline.yaml` to automatically start and orchestrate the three core components from the ttp-processor-demo repository:
- `stellar-live-source` (source)
- `ttp-processor` (processor)
- `consumer_app` (sink)

## Current State Analysis

### ttp-processor-demo Components
Based on detailed repository analysis:

1. **stellar-live-source**: Go service that streams Stellar blockchain ledger data via gRPC
   - **gRPC Service**: `RawLedgerService` with `StreamRawLedgers` method
   - **Proto**: `raw_ledger_service.proto` defines `RawLedger` message with sequence and XDR data
   - **Flowctl Integration**: Built-in support with `ENABLE_FLOWCTL=true`
   - **Health Check**: `/health` endpoint on port 8088 (configurable)
   - **Environment Variables**: `RPC_ENDPOINT`, `NETWORK_PASSPHRASE`, `FLOWCTL_ENDPOINT`

2. **ttp-processor**: Go service that processes raw ledger data and extracts TTP events
   - **gRPC Service**: `EventService` with `GetTTPEvents` method
   - **Proto**: `event_service.proto` and `token_transfer_event.proto` for structured events
   - **Event Types**: Transfer, Mint, Burn, Clawback, Fee events
   - **Flowctl Integration**: Built-in support with registration and heartbeat
   - **Input**: Consumes `RawLedger` from stellar-live-source
   - **Output**: Streams `TokenTransferEvent` messages

3. **consumer_app**: Node.js/TypeScript application that consumes processed events
   - **gRPC Client**: Uses generated TypeScript client for `EventService`
   - **Connection**: Connects to ttp-processor via `TTP_SERVICE_ADDRESS`
   - **Processing**: Streams TTP events and logs/processes them
   - **Flowctl Integration**: Optional integration with metrics reporting

### Current Challenges
- No Docker containers exist for these components
- No orchestration - all started manually
- Health check endpoints exist but need proper configuration
- gRPC ports need to be standardized (currently using defaults)
- flowctl factory functions are hardcoded to return mocks
- **Real gRPC proto implementation needed for production**

## Week 1 MVP Implementation Plan

### Phase 1: Containerize ttp-processor-demo Components (Day 1-2)

#### 1.1 Create Dockerfiles for Each Component
```dockerfile
# stellar-live-source/Dockerfile
FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o stellar-live-source cmd/stellar-live-source/main.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/stellar-live-source .
EXPOSE 50051
CMD ["./stellar-live-source"]
```

```dockerfile
# ttp-processor/Dockerfile
FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o ttp-processor cmd/ttp-processor/main.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/ttp-processor .
EXPOSE 50052
CMD ["./ttp-processor"]
```

```dockerfile
# consumer_app/Dockerfile
FROM node:22-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production
COPY . .
EXPOSE 3000
CMD ["node", "index.js"]
```

#### 1.2 Build and Push Docker Images
```bash
# Build images locally for testing
docker build -t withobsrvr/stellar-live-source:latest -f stellar-live-source/Dockerfile .
docker build -t withobsrvr/ttp-processor:latest -f ttp-processor/Dockerfile .
docker build -t withobsrvr/consumer-app:latest -f consumer_app/Dockerfile .

# Push to registry (for later)
docker push withobsrvr/stellar-live-source:latest
docker push withobsrvr/ttp-processor:latest
docker push withobsrvr/consumer-app:latest
```

### Phase 2: Create Production Docker Executor with gRPC Support (Day 2-3)

#### 2.1 Implement Production-Ready Docker Executor
```go
// internal/executor/production_docker.go
package executor

import (
    "context"
    "fmt"
    "os/exec"
    "time"
    "net/http"
    "strings"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    "google.golang.org/grpc/health/grpc_health_v1"
    
    "github.com/withobsrvr/flowctl/internal/model"
)

type ProductionDockerExecutor struct {
    network          string
    controlPlaneAddr string
}

func NewProductionDockerExecutor(controlPlaneAddr string) *ProductionDockerExecutor {
    return &ProductionDockerExecutor{
        network:          "flowctl-bridge",
        controlPlaneAddr: controlPlaneAddr,
    }
}

func (e *ProductionDockerExecutor) StartComponent(ctx context.Context, component model.Component) error {
    // Ensure bridge network exists
    if err := e.ensureNetwork(); err != nil {
        return fmt.Errorf("failed to ensure network: %w", err)
    }
    
    // Build docker run command with flowctl integration
    args := []string{
        "run", "-d",
        "--name", component.ID,
        "--network", e.network,
        "--rm", // Auto-cleanup on stop
    }
    
    // Add flowctl integration environment variables
    flowctlEnv := map[string]string{
        "ENABLE_FLOWCTL":         "true",
        "FLOWCTL_ENDPOINT":       e.controlPlaneAddr,
        "FLOWCTL_HEARTBEAT_INTERVAL": "10s",
        "COMPONENT_ID":           component.ID,
    }
    
    // Merge component env with flowctl env
    for k, v := range component.Env {
        args = append(args, "-e", fmt.Sprintf("%s=%s", k, v))
    }
    for k, v := range flowctlEnv {
        args = append(args, "-e", fmt.Sprintf("%s=%s", k, v))
    }
    
    // Add port bindings
    for _, port := range component.Ports {
        args = append(args, "-p", fmt.Sprintf("%d:%d", port.HostPort, port.ContainerPort))
    }
    
    // Add health check port if specified
    if component.HealthPort > 0 {
        args = append(args, "-p", fmt.Sprintf("%d:%d", component.HealthPort, component.HealthPort))
    }
    
    // Add image and command
    args = append(args, component.Image)
    if len(component.Command) > 0 {
        args = append(args, component.Command...)
    }
    
    // Execute docker run
    cmd := exec.CommandContext(ctx, "docker", args...)
    output, err := cmd.CombinedOutput()
    if err != nil {
        return fmt.Errorf("failed to start container %s: %w, output: %s", component.ID, err, output)
    }
    
    // Wait for component to be ready (both HTTP health and gRPC)
    if err := e.waitForReady(ctx, component); err != nil {
        // Cleanup on failure
        exec.Command("docker", "rm", "-f", component.ID).Run()
        return fmt.Errorf("component %s failed to become ready: %w", component.ID, err)
    }
    
    return nil
}

func (e *ProductionDockerExecutor) waitForReady(ctx context.Context, component model.Component) error {
    // Wait for HTTP health check
    if component.HealthPort > 0 {
        if err := e.waitForHTTPHealth(ctx, component); err != nil {
            return fmt.Errorf("HTTP health check failed: %w", err)
        }
    }
    
    // Wait for gRPC service to be ready
    if len(component.Ports) > 0 {
        if err := e.waitForGRPCReady(ctx, component); err != nil {
            return fmt.Errorf("gRPC readiness check failed: %w", err)
        }
    }
    
    return nil
}

func (e *ProductionDockerExecutor) waitForHTTPHealth(ctx context.Context, component model.Component) error {
    client := &http.Client{Timeout: 2 * time.Second}
    url := fmt.Sprintf("http://localhost:%d/health", component.HealthPort)
    
    for i := 0; i < 30; i++ { // 30 seconds timeout
        resp, err := client.Get(url)
        if err == nil && resp.StatusCode == 200 {
            resp.Body.Close()
            return nil
        }
        if resp != nil {
            resp.Body.Close()
        }
        
        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-time.After(1 * time.Second):
            continue
        }
    }
    
    return fmt.Errorf("HTTP health check timeout for component %s", component.ID)
}

func (e *ProductionDockerExecutor) waitForGRPCReady(ctx context.Context, component model.Component) error {
    if len(component.Ports) == 0 {
        return nil // No gRPC ports to check
    }
    
    // Try to connect to the gRPC service
    grpcAddr := fmt.Sprintf("localhost:%d", component.Ports[0].HostPort)
    
    for i := 0; i < 30; i++ { // 30 seconds timeout
        conn, err := grpc.DialContext(ctx, grpcAddr, 
            grpc.WithTransportCredentials(insecure.NewCredentials()),
            grpc.WithBlock(),
            grpc.WithTimeout(2*time.Second))
        
        if err == nil {
            // Try gRPC health check if available
            healthClient := grpc_health_v1.NewHealthClient(conn)
            _, healthErr := healthClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
            conn.Close()
            
            if healthErr == nil {
                return nil // gRPC service is ready
            }
            // If health check fails, service might still be starting
        }
        
        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-time.After(1 * time.Second):
            continue
        }
    }
    
    return fmt.Errorf("gRPC readiness check timeout for component %s", component.ID)
}

func (e *ProductionDockerExecutor) ensureNetwork() error {
    // Check if network exists
    cmd := exec.Command("docker", "network", "inspect", e.network)
    if err := cmd.Run(); err == nil {
        return nil // Network exists
    }
    
    // Create bridge network with DNS resolution
    cmd = exec.Command("docker", "network", "create", 
        "--driver", "bridge",
        "--attachable",
        e.network)
    return cmd.Run()
}

func (e *ProductionDockerExecutor) StopComponent(componentID string) error {
    cmd := exec.Command("docker", "rm", "-f", componentID)
    return cmd.Run()
}
```

#### 2.2 Update Factory Functions with Real Components
```go
// internal/core/pipeline_dag.go
// Replace the existing mock factory functions

var globalExecutor *executor.ProductionDockerExecutor

func init() {
    // Initialize global executor with control plane endpoint
    globalExecutor = executor.NewProductionDockerExecutor("http://localhost:8080")
}

func createSource(component model.Component) (source.Source, error) {
    // Start the component container
    if err := globalExecutor.StartComponent(context.Background(), component); err != nil {
        return nil, fmt.Errorf("failed to start source container: %w", err)
    }
    
    // Create component-specific source based on component ID
    switch component.ID {
    case "stellar-live-source":
        endpoint := fmt.Sprintf("localhost:%d", component.Ports[0].HostPort)
        return NewStellarLiveSource(component.ID, endpoint, globalExecutor, 1), nil
    default:
        return nil, fmt.Errorf("unknown source component: %s", component.ID)
    }
}

func createProcessor(component model.Component) (processor.Processor, error) {
    // Start the component container
    if err := globalExecutor.StartComponent(context.Background(), component); err != nil {
        return nil, fmt.Errorf("failed to start processor container: %w", err)
    }
    
    // Create component-specific processor based on component ID
    switch component.ID {
    case "ttp-processor":
        endpoint := fmt.Sprintf("localhost:%d", component.Ports[0].HostPort)
        return NewTTPProcessor(component.ID, endpoint, globalExecutor, 1, 0), nil
    default:
        return nil, fmt.Errorf("unknown processor component: %s", component.ID)
    }
}

func createSink(component model.Component) (sink.Sink, error) {
    // Start the component container
    if err := globalExecutor.StartComponent(context.Background(), component); err != nil {
        return nil, fmt.Errorf("failed to start sink container: %w", err)
    }
    
    // Create component-specific sink based on component ID
    switch component.ID {
    case "consumer-app":
        endpoint := fmt.Sprintf("localhost:%d", component.Ports[0].HostPort)
        return NewConsumerAppSink(component.ID, endpoint, globalExecutor), nil
    default:
        return nil, fmt.Errorf("unknown sink component: %s", component.ID)
    }
}
```

### Phase 3: Create Real gRPC Component Proxies with Proto Implementation (Day 3-4)

#### 3.1 Generate gRPC Client Code
```bash
# Add to Makefile
generate-grpc-clients:
	# Generate Go clients for flowctl
	protoc --go_out=./internal/grpc/gen --go_opt=paths=source_relative \
	       --go-grpc_out=./internal/grpc/gen --go-grpc_opt=paths=source_relative \
	       --proto_path=./proto \
	       raw_ledger_service/raw_ledger_service.proto \
	       event_service/event_service.proto \
	       ingest/processors/token_transfer/token_transfer_event.proto
```

#### 3.2 Implement Real gRPC Source for stellar-live-source
```go
// internal/core/stellar_live_source.go
package core

import (
    "context"
    "fmt"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    
    "github.com/withobsrvr/flowctl/internal/source"
    "github.com/withobsrvr/flowctl/internal/executor"
    pb "github.com/withobsrvr/flowctl/internal/grpc/gen/raw_ledger_service"
)

type StellarLiveSource struct {
    componentID string
    endpoint    string
    executor    *executor.ProductionDockerExecutor
    conn        *grpc.ClientConn
    client      pb.RawLedgerServiceClient
    startLedger uint32
}

func NewStellarLiveSource(componentID, endpoint string, executor *executor.ProductionDockerExecutor, startLedger uint32) *StellarLiveSource {
    return &StellarLiveSource{
        componentID: componentID,
        endpoint:    endpoint,
        executor:    executor,
        startLedger: startLedger,
    }
}

func (s *StellarLiveSource) Open(ctx context.Context) error {
    // Connect to the stellar-live-source gRPC service
    conn, err := grpc.DialContext(ctx, s.endpoint, 
        grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        return fmt.Errorf("failed to connect to stellar-live-source at %s: %w", s.endpoint, err)
    }
    
    s.conn = conn
    s.client = pb.NewRawLedgerServiceClient(conn)
    return nil
}

func (s *StellarLiveSource) Events(ctx context.Context, out chan<- source.EventEnvelope) error {
    // Create streaming request
    req := &pb.StreamLedgersRequest{
        StartLedger: s.startLedger,
    }
    
    // Start streaming raw ledgers
    stream, err := s.client.StreamRawLedgers(ctx, req)
    if err != nil {
        return fmt.Errorf("failed to start streaming: %w", err)
    }
    
    // Process incoming ledgers
    for {
        ledger, err := stream.Recv()
        if err != nil {
            return fmt.Errorf("stream error: %w", err)
        }
        
        // Convert RawLedger to EventEnvelope
        event := source.EventEnvelope{
            LedgerSeq: ledger.Sequence,
            Payload:   ledger.LedgerCloseMetaXdr, // Raw XDR bytes
            Cursor:    fmt.Sprintf("ledger-%d", ledger.Sequence),
        }
        
        select {
        case out <- event:
            // Event sent successfully
        case <-ctx.Done():
            return ctx.Err()
        }
    }
}

func (s *StellarLiveSource) Close() error {
    if s.conn != nil {
        s.conn.Close()
    }
    return nil // Container cleanup handled by executor
}

func (s *StellarLiveSource) Healthy() error {
    if s.conn == nil {
        return fmt.Errorf("no connection to %s", s.componentID)
    }
    return nil
}
```

#### 3.3 Implement Real gRPC Processor for ttp-processor
```go
// internal/core/ttp_processor.go
package core

import (
    "context"
    "fmt"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    
    "github.com/withobsrvr/flowctl/internal/processor"
    "github.com/withobsrvr/flowctl/internal/source"
    "github.com/withobsrvr/flowctl/internal/executor"
    pb "github.com/withobsrvr/flowctl/internal/grpc/gen/event_service"
)

type TTPProcessor struct {
    componentID string
    endpoint    string
    executor    *executor.ProductionDockerExecutor
    conn        *grpc.ClientConn
    client      pb.EventServiceClient
    startLedger uint32
    endLedger   uint32
}

func NewTTPProcessor(componentID, endpoint string, executor *executor.ProductionDockerExecutor, startLedger, endLedger uint32) *TTPProcessor {
    return &TTPProcessor{
        componentID: componentID,
        endpoint:    endpoint,
        executor:    executor,
        startLedger: startLedger,
        endLedger:   endLedger,
    }
}

func (t *TTPProcessor) Init(cfg map[string]any) error {
    // Connect to the ttp-processor gRPC service
    conn, err := grpc.Dial(t.endpoint, 
        grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        return fmt.Errorf("failed to connect to ttp-processor at %s: %w", t.endpoint, err)
    }
    
    t.conn = conn
    t.client = pb.NewEventServiceClient(conn)
    return nil
}

func (t *TTPProcessor) Process(ctx context.Context, e *source.EventEnvelope) ([]*processor.Message, error) {
    // For streaming processor, we'll start a stream and forward events
    // This is a simplified implementation - in production, you might want to 
    // correlate input events with output events more precisely
    
    req := &pb.GetEventsRequest{
        StartLedger: t.startLedger,
        EndLedger:   t.endLedger, // 0 for live stream
    }
    
    stream, err := t.client.GetTTPEvents(ctx, req)
    if err != nil {
        return nil, fmt.Errorf("failed to get TTP events: %w", err)
    }
    
    var messages []*processor.Message
    
    // Read events from the stream (with timeout to avoid blocking)
    eventCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
    defer cancel()
    
    for {
        select {
        case <-eventCtx.Done():
            return messages, nil // Return what we have
        default:
            event, err := stream.Recv()
            if err != nil {
                return messages, nil // Return what we have
            }
            
            // Convert TokenTransferEvent to processor.Message
            msg := &processor.Message{
                Type:    "token.transfer",
                Payload: t.serializeTokenTransferEvent(event),
            }
            
            messages = append(messages, msg)
        }
    }
}

func (t *TTPProcessor) serializeTokenTransferEvent(event *pb.TokenTransferEvent) []byte {
    // Convert the protobuf event to JSON or keep as binary
    // For now, convert to JSON for compatibility
    eventJSON := fmt.Sprintf(`{
        "sequence": %d,
        "timestamp": %d,
        "tx_hash": "%s",
        "component_id": "%s"
    }`, 
        event.Meta.LedgerSequence,
        event.Meta.Timestamp,
        event.Meta.TxHash,
        t.componentID)
    
    return []byte(eventJSON)
}

func (t *TTPProcessor) Flush(ctx context.Context) error {
    return nil
}

func (t *TTPProcessor) Name() string {
    return t.componentID
}

func (t *TTPProcessor) Close() error {
    if t.conn != nil {
        t.conn.Close()
    }
    return nil
}
```

#### 3.4 Implement Real gRPC Sink for consumer-app
```go
// internal/core/consumer_app_sink.go
package core

import (
    "context"
    "fmt"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    
    "github.com/withobsrvr/flowctl/internal/sink"
    "github.com/withobsrvr/flowctl/internal/processor"
    "github.com/withobsrvr/flowctl/internal/executor"
    pb "github.com/withobsrvr/flowctl/internal/grpc/gen/event_service"
)

type ConsumerAppSink struct {
    componentID string
    endpoint    string
    executor    *executor.ProductionDockerExecutor
    conn        *grpc.ClientConn
    client      pb.EventServiceClient
}

func NewConsumerAppSink(componentID, endpoint string, executor *executor.ProductionDockerExecutor) *ConsumerAppSink {
    return &ConsumerAppSink{
        componentID: componentID,
        endpoint:    endpoint,
        executor:    executor,
    }
}

func (c *ConsumerAppSink) Connect(cfg map[string]any) error {
    // Connect to the consumer-app gRPC service
    conn, err := grpc.Dial(c.endpoint, 
        grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        return fmt.Errorf("failed to connect to consumer-app at %s: %w", c.endpoint, err)
    }
    
    c.conn = conn
    c.client = pb.NewEventServiceClient(conn)
    return nil
}

func (c *ConsumerAppSink) Write(ctx context.Context, messages []*processor.Message) error {
    // Forward messages to the consumer app
    // The consumer app will handle the actual processing/storage
    
    for _, msg := range messages {
        // Log the message being sent to consumer app
        fmt.Printf("Forwarding message to consumer-app: %s\n", string(msg.Payload))
        
        // In a real implementation, you might want to send this via gRPC
        // For now, we'll just log it since the consumer app will stream directly
        // from the ttp-processor
    }
    
    return nil
}

func (c *ConsumerAppSink) Close() error {
    if c.conn != nil {
        c.conn.Close()
    }
    return nil
}
```

### Phase 4: Create TTP Pipeline Configuration (Day 4-5)

#### 4.1 Create Production ttp-pipeline.yaml
```yaml
# examples/ttp-pipeline.yaml
apiVersion: flowctl/v1
kind: Pipeline
metadata:
  name: ttp-processing-pipeline
  namespace: default
spec:
  description: "Production Token Transfer Protocol processing pipeline using ttp-processor-demo components"
  driver: dag
  
  sources:
    - id: stellar-live-source
      type: source
      image: withobsrvr/stellar-live-source:latest
      command: ["./stellar-live-source"]
      output_event_types: ["stellar.ledger", "stellar.transaction"]
      health_port: 8088  # As per FLOWCTL_INTEGRATION.md
      ports:
        - containerPort: 50051
          hostPort: 50051
      env:
        # Stellar configuration
        RPC_ENDPOINT: "https://horizon-testnet.stellar.org"
        NETWORK_PASSPHRASE: "Test SDF Network ; September 2015"
        STELLAR_NETWORK: "testnet"
        
        # gRPC configuration
        GRPC_PORT: "50051"
        
        # Health check configuration
        HEALTH_PORT: "8088"
        
        # Logging
        LOG_LEVEL: "info"
        
        # Flowctl integration (automatically added by executor)
        # ENABLE_FLOWCTL: "true"
        # FLOWCTL_ENDPOINT: "localhost:8080"
        # FLOWCTL_HEARTBEAT_INTERVAL: "10s"
        
      resources:
        requests:
          cpu: "0.1"
          memory: "128Mi"
        limits:
          cpu: "0.5"
          memory: "256Mi"
  
  processors:
    - id: ttp-processor
      type: processor
      image: withobsrvr/ttp-processor:latest
      command: ["./ttp-processor"]
      inputs: ["stellar-live-source"]
      input_event_types: ["stellar.ledger", "stellar.transaction"]
      output_event_types: ["token.transfer"]
      health_port: 8089
      ports:
        - containerPort: 50052
          hostPort: 50052
      env:
        # Input configuration
        LIVE_SOURCE_ENDPOINT: "stellar-live-source:50051"
        
        # gRPC configuration
        GRPC_PORT: "50052"
        
        # Health check configuration
        HEALTH_PORT: "8089"
        
        # Processing configuration
        LOG_LEVEL: "info"
        START_LEDGER: "1"
        END_LEDGER: "0"  # 0 for live stream
        
        # Flowctl integration (automatically added by executor)
        # ENABLE_FLOWCTL: "true"
        # FLOWCTL_ENDPOINT: "localhost:8080"
        
      resources:
        requests:
          cpu: "0.2"
          memory: "256Mi"
        limits:
          cpu: "1.0"
          memory: "512Mi"
        
  sinks:
    - id: consumer-app
      type: sink
      image: withobsrvr/consumer-app:latest
      command: ["npm", "start"]
      inputs: ["ttp-processor"]
      health_port: 3000
      ports:
        - containerPort: 3000
          hostPort: 3000
      env:
        # TTP processor connection
        TTP_SERVICE_ADDRESS: "ttp-processor:50052"
        
        # Server configuration
        PORT: "3000"
        
        # Processing configuration
        START_LEDGER: "1"
        END_LEDGER: "0"  # 0 for live stream
        
        # Logging
        LOG_LEVEL: "info"
        
        # Flowctl integration (automatically added by executor)
        # ENABLE_FLOWCTL: "true"
        # FLOWCTL_ENDPOINT: "localhost:8080"
        
      resources:
        requests:
          cpu: "0.1"
          memory: "128Mi"
        limits:
          cpu: "0.5"
          memory: "256Mi"
```

#### 4.2 Production Component Configuration

**Note**: The ttp-processor-demo components already have built-in flowctl integration and health check endpoints as documented in their `FLOWCTL_INTEGRATION.md` files. The key configurations are:

**stellar-live-source-datalake**:
- Health endpoint: `/health` on configurable port
- Flowctl integration: Enable with `ENABLE_FLOWCTL=true`
- Automatic registration and heartbeat to control plane
- Metrics reporting: success_count, error_count, total_processed
- Data lake storage: Supports GCS, S3, or local filesystem
- Configurable data partitioning: ledgers per file, files per partition

**ttp-processor**:
- Health endpoint: `/health` on configurable port
- Flowctl integration: Enable with `ENABLE_FLOWCTL=true`
- Connects to stellar-live-source via `LIVE_SOURCE_ENDPOINT`
- Streams processed events via gRPC EventService

**consumer-app**:
- Health endpoint: Available on main port
- Flowctl integration: Optional
- Connects to ttp-processor via `TTP_SERVICE_ADDRESS`
- Streams and processes TTP events

#### 4.3 Copy Proto Files to flowctl
```bash
# Create proto directory structure in flowctl
mkdir -p proto/raw_ledger_service
mkdir -p proto/event_service
mkdir -p proto/ingest/processors/token_transfer

# Copy proto files from ttp-processor-demo
cp ../ttp-processor-demo/stellar-live-source/protos/raw_ledger_service/raw_ledger_service.proto \
   proto/raw_ledger_service/

cp ../ttp-processor-demo/ttp-processor/protos/event_service/event_service.proto \
   proto/event_service/

cp ../ttp-processor-demo/ttp-processor/protos/ingest/processors/token_transfer/token_transfer_event.proto \
   proto/ingest/processors/token_transfer/
```

### Phase 5: Integration and Testing (Day 5)

#### 5.1 Test Pipeline Execution
```bash
# Build flowctl with new executor
make build

# Test the pipeline
./bin/flowctl run examples/ttp-pipeline.yaml

# Check component status
docker ps

# View logs
docker logs stellar-live-source-datalake
docker logs ttp-processor
docker logs consumer-app

# Test data flow
curl http://localhost:3000/health  # Consumer app health
```

#### 5.2 Add Cleanup Command
```bash
# Stop and cleanup all components
./bin/flowctl stop examples/ttp-pipeline.yaml
```

## Success Criteria

1. **Container Startup**: All three ttp-processor-demo components start in Docker containers
2. **Network Communication**: Components can communicate with each other via gRPC over bridge network
3. **Data Flow**: Data flows from stellar-live-source-datalake → ttp-processor → consumer-app
4. **Health Checks**: All components respond to health check endpoints
5. **Data Lake Integration**: stellar-live-source-datalake successfully reads from configured data lake (S3/GCS/FS)
6. **Cleanup**: Components stop and cleanup properly when pipeline is stopped
7. **Configuration**: Pipeline configuration is loaded and applied correctly
8. **Storage Backend**: Data lake configuration (bucket, region, partition settings) works correctly

## Minimal Test Plan

### Day 1-2: Container and Proto Setup
- Build Docker images for all three components
- Copy and generate gRPC proto files
- Test each component individually in containers
- Verify gRPC communication between containers

### Day 3-4: flowctl Integration with Real gRPC
- Implement and test ProductionDockerExecutor
- Implement real gRPC component proxies
- Test component factory functions with real proto clients
- Verify flowctl integration (registration, heartbeat, health checks)

### Day 5: End-to-End Production Testing
- Test complete pipeline execution with real data
- Verify data flow: stellar-live-source-datalake → ttp-processor → consumer-app
- Test pipeline cleanup and component lifecycle
- Verify flowctl control plane integration
- Performance and stability testing

## Known Limitations for Week 1

1. **Simplified Component Management**: Basic container lifecycle, not full ComponentHandle interface
2. **Limited Error Recovery**: Basic error handling, no sophisticated retry logic
3. **No Restart Policies**: Components don't automatically restart on failure
4. **Basic Resource Management**: CPU/memory limits specified but not enforced
5. **Hardcoded Configuration**: Some configuration values are hardcoded
6. **Single Network**: All components on same bridge network (good for MVP)
7. **No Load Balancing**: Single instance of each component

## Next Steps After Week 1

1. **Enhanced Component Management**: Implement full ComponentHandle interface
2. **Advanced Health Monitoring**: Add comprehensive health and performance monitoring
3. **Restart Policies**: Add automatic restart on failure with exponential backoff
4. **Resource Management**: Implement CPU/memory limits enforcement
5. **Configuration Management**: Make all configuration dynamic and externalized
6. **Monitoring**: Add comprehensive logging, metrics, and tracing
7. **Security**: Add component authentication and network security
8. **Multi-Instance Support**: Add support for scaling components

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Docker images don't build | Test Docker builds early, create simple Dockerfiles |
| gRPC communication fails | Start with simple HTTP health checks, add gRPC incrementally |
| Port conflicts | Use dynamic port allocation or well-known ports |
| Network isolation issues | Use bridge networks with proper DNS resolution |
| Component startup order | Implement basic dependency checking |

This plan provides a clear path to get the ttp-processor-demo components running with flowctl in one week, with room for enhancement in subsequent iterations.