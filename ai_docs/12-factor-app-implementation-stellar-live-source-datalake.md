# 12-Factor App Implementation Guide for stellar-live-source-datalake

## Executive Summary

This document outlines the implementation of an expanded 12-factor app methodology for the `stellar-live-source-datalake` service. The service currently demonstrates several 12-factor principles but requires enhancements to meet modern cloud-native standards for production deployment.

### Current State
- ✅ Environment-based configuration
- ✅ Basic health checks and metrics
- ✅ Structured logging with zap
- ✅ Process isolation and stateless design
- ❌ Limited observability and telemetry
- ❌ Security hardening needed
- ❌ Configuration management complexity
- ❌ Deployment automation gaps

### Benefits of Full Implementation
- Enhanced reliability and observability
- Simplified deployment and scaling
- Improved security posture
- Better developer experience
- Production-ready operations

## 12-Factor Analysis & Implementation Plan

### 1. Codebase ✅
**Current State**: Single codebase in Git with multiple deployment environments.
**Status**: Compliant
**Enhancements**: None needed

### 2. Dependencies ✅
**Current State**: Go modules with Nix for reproducible builds.
**Status**: Compliant
**Enhancements**: 
- Add dependency vulnerability scanning
- Implement SBOM generation

### 3. Config ⚠️
**Current State**: Environment variables with some hardcoded values.
**Gap**: No configuration validation, no structured config management.

**Implementation Plan**:
```go
// config/config.go
type Config struct {
    Server ServerConfig `yaml:"server"`
    Storage StorageConfig `yaml:"storage"`
    Flowctl FlowctlConfig `yaml:"flowctl"`
    Monitoring MonitoringConfig `yaml:"monitoring"`
    Security SecurityConfig `yaml:"security"`
}

type ServerConfig struct {
    Port int `yaml:"port" env:"SERVER_PORT" env-default:"50052"`
    HealthPort int `yaml:"health_port" env:"HEALTH_PORT" env-default:"8088"`
    GracefulShutdownTimeout time.Duration `yaml:"graceful_shutdown_timeout" env:"GRACEFUL_SHUTDOWN_TIMEOUT" env-default:"30s"`
}

type StorageConfig struct {
    Type string `yaml:"type" env:"STORAGE_TYPE" env-required:"true"`
    BucketName string `yaml:"bucket_name" env:"BUCKET_NAME" env-required:"true"`
    Region string `yaml:"region" env:"AWS_REGION"`
    Endpoint string `yaml:"endpoint" env:"S3_ENDPOINT_URL"`
    LedgersPerFile uint32 `yaml:"ledgers_per_file" env:"LEDGERS_PER_FILE" env-default:"64"`
    FilesPerPartition uint32 `yaml:"files_per_partition" env:"FILES_PER_PARTITION" env-default:"10"`
}
```

### 4. Backing Services ✅
**Current State**: External storage (S3/GCS/FS) treated as attached resources.
**Status**: Compliant
**Enhancements**: Add connection pooling and retry logic

### 5. Build, Release, Run ⚠️
**Current State**: Nix builds with Docker containers.
**Gap**: No formal release process or artifact management.

**Implementation Plan**:
- Add semantic versioning
- Implement GitOps deployment pipeline
- Add blue-green deployment strategy
- Create release automation

### 6. Processes ✅
**Current State**: Stateless service with shared-nothing architecture.
**Status**: Compliant

### 7. Port Binding ✅
**Current State**: gRPC on 50052, health checks on 8088.
**Status**: Compliant

### 8. Concurrency ✅
**Current State**: Horizontal scaling via container orchestration.
**Status**: Compliant
**Enhancements**: Add pod autoscaling based on metrics

### 9. Disposability ⚠️
**Current State**: Basic graceful shutdown.
**Gap**: Enhanced shutdown procedures and startup optimization.

**Implementation Plan**:
```go
// Enhanced graceful shutdown
func (s *Server) GracefulShutdown(ctx context.Context) error {
    log.Info("Starting graceful shutdown")
    
    // 1. Stop accepting new connections
    s.grpcServer.GracefulStop()
    
    // 2. Complete in-flight requests (with timeout)
    done := make(chan struct{})
    go func() {
        s.waitForInflightRequests()
        close(done)
    }()
    
    select {
    case <-done:
        log.Info("All in-flight requests completed")
    case <-ctx.Done():
        log.Warn("Graceful shutdown timeout, forcing stop")
        s.grpcServer.Stop()
    }
    
    // 3. Cleanup resources
    return s.cleanup()
}
```

### 10. Dev/Prod Parity ⚠️
**Current State**: Different environments use different configurations.
**Gap**: Need standardized development environment.

**Implementation Plan**:
- Create docker-compose.dev.yml for local development
- Add Tilt configuration for Kubernetes development
- Standardize environment configurations

### 11. Logs ✅
**Current State**: Structured logging with zap to stdout.
**Status**: Compliant
**Enhancements**: Add correlation IDs and distributed tracing

### 12. Admin Processes ✅
**Current State**: Separate admin tasks handled via CLI tools.
**Status**: Compliant

## Expanded Modern Factors

### 13. API Design
**Implementation**:
- OpenAPI/gRPC documentation
- Versioning strategy
- Rate limiting and throttling
- Circuit breaker patterns

### 14. Telemetry & Observability
**Implementation Plan**:
```go
// metrics/metrics.go
type Metrics struct {
    // Business metrics
    LedgersProcessed prometheus.Counter
    ProcessingLatency prometheus.Histogram
    ErrorRate prometheus.Counter
    
    // Infrastructure metrics
    GRPCRequestDuration prometheus.Histogram
    ActiveConnections prometheus.Gauge
    MemoryUsage prometheus.Gauge
    
    // SLI metrics
    Availability prometheus.Gauge
    ErrorBudget prometheus.Gauge
}

// Add distributed tracing
func (s *Server) StreamRawLedgers(req *pb.StreamLedgersRequest, stream pb.RawLedgerService_StreamRawLedgersServer) error {
    ctx := stream.Context()
    span, ctx := opentracing.StartSpanFromContext(ctx, "StreamRawLedgers")
    defer span.Finish()
    
    span.SetTag("start_ledger", req.StartLedger)
    span.SetTag("storage_type", os.Getenv("STORAGE_TYPE"))
    
    // ... processing logic with tracing
}
```

### 15. Security
**Implementation Plan**:
```go
// security/auth.go
type SecurityConfig struct {
    TLSEnabled bool `yaml:"tls_enabled" env:"TLS_ENABLED" env-default:"true"`
    TLSCertPath string `yaml:"tls_cert_path" env:"TLS_CERT_PATH"`
    TLSKeyPath string `yaml:"tls_key_path" env:"TLS_KEY_PATH"`
    APIKeyRequired bool `yaml:"api_key_required" env:"API_KEY_REQUIRED" env-default:"false"`
    RateLimitRPS int `yaml:"rate_limit_rps" env:"RATE_LIMIT_RPS" env-default:"100"`
}

// Add TLS support
func (s *Server) createGRPCServer() (*grpc.Server, error) {
    var opts []grpc.ServerOption
    
    if s.config.Security.TLSEnabled {
        creds, err := credentials.NewServerTLSFromFile(
            s.config.Security.TLSCertPath,
            s.config.Security.TLSKeyPath,
        )
        if err != nil {
            return nil, err
        }
        opts = append(opts, grpc.Creds(creds))
    }
    
    // Add rate limiting
    opts = append(opts, grpc.UnaryInterceptor(s.rateLimitInterceptor))
    
    return grpc.NewServer(opts...), nil
}
```

## Implementation Roadmap

### Phase 1: Core Improvements (Week 1-2)
1. **Enhanced Configuration Management**
   - Implement structured config with validation
   - Add configuration hot-reloading
   - Create environment-specific config files

2. **Improved Observability**
   - Add Prometheus metrics
   - Implement correlation IDs
   - Enhanced health checks with detailed status

3. **Security Hardening**
   - Add TLS support
   - Implement basic authentication
   - Add input validation

### Phase 2: Advanced Features (Week 3-4)
1. **Distributed Tracing**
   - OpenTelemetry integration
   - Jaeger/Zipkin compatibility
   - Cross-service trace correlation

2. **Enhanced Resilience**
   - Advanced circuit breaker patterns
   - Retry policies with exponential backoff
   - Graceful degradation modes

3. **Deployment Automation**
   - Kubernetes manifests
   - Helm chart creation
   - GitOps pipeline setup

### Phase 3: Optimization (Week 5-6)
1. **Performance Monitoring**
   - SLI/SLO implementation
   - Alerting rules
   - Performance profiling

2. **Developer Experience**
   - Local development environment
   - API documentation
   - Testing frameworks

## Technical Specifications

### Configuration Structure
```yaml
# config/production.yaml
server:
  port: 50052
  health_port: 8088
  graceful_shutdown_timeout: 30s
  max_concurrent_streams: 100

storage:
  type: "GCS"
  bucket_name: "stellar-ledgers-prod"
  ledgers_per_file: 64
  files_per_partition: 10
  retry_attempts: 3
  retry_backoff: "1s"

flowctl:
  enabled: true
  endpoint: "flowctl.internal:8080"
  heartbeat_interval: "10s"
  registration_timeout: "30s"

monitoring:
  metrics_enabled: true
  metrics_port: 9090
  tracing_enabled: true
  trace_endpoint: "jaeger.internal:14268"
  
security:
  tls_enabled: true
  tls_cert_path: "/etc/ssl/certs/server.crt"
  tls_key_path: "/etc/ssl/private/server.key"
  api_key_required: true
  rate_limit_rps: 1000
```

### Kubernetes Deployment
```yaml
# k8s/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: stellar-live-source-datalake
  labels:
    app: stellar-live-source-datalake
    version: v1.0.0
spec:
  replicas: 3
  selector:
    matchLabels:
      app: stellar-live-source-datalake
  template:
    metadata:
      labels:
        app: stellar-live-source-datalake
        version: v1.0.0
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
        prometheus.io/path: "/metrics"
    spec:
      containers:
      - name: stellar-live-source-datalake
        image: stellar-live-source-datalake:v1.0.0
        ports:
        - containerPort: 50052
          name: grpc
        - containerPort: 8088
          name: health
        - containerPort: 9090
          name: metrics
        env:
        - name: CONFIG_FILE
          value: "/etc/config/production.yaml"
        resources:
          requests:
            memory: "256Mi"
            cpu: "100m"
          limits:
            memory: "512Mi"
            cpu: "500m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8088
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8088
          initialDelaySeconds: 5
          periodSeconds: 5
        volumeMounts:
        - name: config
          mountPath: /etc/config
        - name: tls-certs
          mountPath: /etc/ssl/certs
          readOnly: true
      volumes:
      - name: config
        configMap:
          name: stellar-live-source-config
      - name: tls-certs
        secret:
          secretName: stellar-live-source-tls
```

### Monitoring & Alerting
```yaml
# monitoring/alerts.yaml
groups:
- name: stellar-live-source-datalake
  rules:
  - alert: HighErrorRate
    expr: rate(stellar_errors_total[5m]) > 0.1
    for: 2m
    labels:
      severity: warning
    annotations:
      summary: "High error rate in stellar-live-source-datalake"
      
  - alert: HighLatency
    expr: histogram_quantile(0.99, rate(stellar_processing_duration_seconds_bucket[5m])) > 0.5
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "High processing latency"
      
  - alert: ServiceDown
    expr: up{job="stellar-live-source-datalake"} == 0
    for: 1m
    labels:
      severity: critical
    annotations:
      summary: "stellar-live-source-datalake service is down"
```

## Development Workflow

### Local Development Setup
```yaml
# docker-compose.dev.yml
version: '3.8'
services:
  stellar-live-source-datalake:
    build:
      context: .
      dockerfile: Dockerfile.dev
    ports:
      - "50052:50052"
      - "8088:8088"
      - "9090:9090"
    environment:
      - CONFIG_FILE=/app/config/development.yaml
      - LOG_LEVEL=debug
    volumes:
      - ./config:/app/config
      - ./data:/app/data
    depends_on:
      - jaeger
      - prometheus

  jaeger:
    image: jaegertracing/all-in-one:latest
    ports:
      - "16686:16686"
      - "14268:14268"

  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9091:9090"
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml
```

### Testing Strategy
```go
// tests/integration_test.go
func TestStellarLiveSourceIntegration(t *testing.T) {
    // Setup test environment
    ctx := context.Background()
    config := loadTestConfig()
    server := setupTestServer(config)
    
    // Test scenarios
    t.Run("basic_streaming", func(t *testing.T) {
        client := setupGRPCClient(t)
        stream, err := client.StreamRawLedgers(ctx, &pb.StreamLedgersRequest{
            StartLedger: 1000,
        })
        require.NoError(t, err)
        
        // Verify streaming behavior
        ledger, err := stream.Recv()
        require.NoError(t, err)
        assert.Equal(t, uint32(1000), ledger.Sequence)
    })
    
    t.Run("error_handling", func(t *testing.T) {
        // Test circuit breaker behavior
        // Test retry logic
        // Test graceful degradation
    })
}
```

## Implementation Checklist

### Phase 1 (Core)
- [ ] Structured configuration management
- [ ] Configuration validation and hot-reloading
- [ ] Enhanced metrics with Prometheus
- [ ] Correlation IDs and structured logging
- [ ] Basic TLS support
- [ ] Enhanced health checks
- [ ] Graceful shutdown improvements

### Phase 2 (Advanced)
- [ ] Distributed tracing with OpenTelemetry
- [ ] Advanced circuit breaker patterns
- [ ] Retry policies with exponential backoff
- [ ] Kubernetes deployment manifests
- [ ] Helm chart creation
- [ ] Rate limiting and throttling

### Phase 3 (Optimization)
- [ ] SLI/SLO implementation
- [ ] Alerting rules and runbooks
- [ ] Performance profiling and optimization
- [ ] Comprehensive API documentation
- [ ] End-to-end testing suite
- [ ] Load testing and capacity planning

## Conclusion

This implementation plan transforms the stellar-live-source-datalake service into a production-ready, cloud-native application following modern 12-factor principles. The phased approach ensures minimal disruption while systematically improving reliability, observability, security, and developer experience.

The estimated timeline is 6 weeks for full implementation, with immediate benefits starting from Phase 1 completion. Each phase builds upon the previous, allowing for incremental deployment and validation.