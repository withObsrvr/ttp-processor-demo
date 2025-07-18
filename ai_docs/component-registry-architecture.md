# Flowctl Component Registry Architecture

## Problem Statement

Currently, flowctl requires developers to build Docker images for every custom component (source, processor, sink), creating massive friction:

- **High barrier to entry**: Must create Dockerfiles and build images
- **Slow iteration**: Each change requires rebuilding containers
- **Language lock-in**: Assumes single language per component
- **No discovery**: No way to find and reuse components

## Solution: GitHub-Based Component Registry

Inspired by Terraform's provider registry, we propose a component registry that:

1. **Fetches from GitHub**: Components are Git repositories
2. **Builds on-demand**: Registry builds containers from source OR native binaries
3. **Supports multiple languages**: Go, TypeScript, Python, Rust, etc.
4. **Multi-component bundles**: Related components in single repositories
5. **Dual execution modes**: Container-based OR native in-process
6. **Caches artifacts**: Immutable, signed binary cache
7. **Semantic versioning**: Proper dependency resolution

## Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   flowctl CLI   │    │  Registry API   │    │  Build Service  │
│                 │    │                 │    │                 │
│ - search        │◄──►│ - metadata      │◄──►│ - multi-lang    │
│ - install       │    │ - versions      │    │ - containerize  │
│ - dev (native)  │    │ - bundles       │    │ - native build  │
│ - publish       │    │ - download      │    │ - sign & cache  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Local Component │    │ Component Cache │    │ GitHub Repos    │
│     Cache       │    │  (Immutable)    │    │                 │
│                 │    │                 │    │ - source code   │
│ ~/.flowctl/     │    │ - containers    │    │ - component.yaml│
│ components/     │    │ - native bins   │    │ - bundle.yaml   │
│ bundles/        │    │ - signatures    │    │ - versions      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Component Structure

### Component Repository Layout

```
# Single Component Repository
stellar-source/
├── component.yaml          # Component specification
├── src/                   # Source code
│   ├── main.go           # Go implementation
│   └── types.go
├── typescript/            # Optional TypeScript version
│   ├── index.ts
│   └── package.json
├── python/                # Optional Python version
│   ├── __init__.py
│   └── requirements.txt
├── Dockerfile             # Optional custom Dockerfile
├── examples/              # Usage examples
│   └── stellar-pipeline.yaml
└── README.md

# Multi-Component Bundle Repository (Based on ttp-processor-demo pattern)
stellar-processing-suite/
├── bundle.yaml            # Bundle specification
├── stellar-live-source/   # Go source component
│   ├── main.go
│   ├── component.yaml
│   └── Dockerfile
├── ttp-processor/         # TypeScript processor component
│   ├── index.ts
│   ├── package.json
│   ├── component.yaml
│   └── Dockerfile
├── consumer-app/          # JavaScript sink component
│   ├── index.js
│   ├── package.json
│   ├── component.yaml
│   └── Dockerfile
├── examples/              # Bundle usage examples
│   ├── complete-pipeline.yaml
│   └── dev-pipeline.yaml
└── README.md
```

### Component Specification (`component.yaml`)

```yaml
apiVersion: component.flowctl.io/v1
kind: ComponentSpec
metadata:
  name: stellar-source
  version: v1.2.3
  namespace: stellar
  description: "Stellar blockchain data source"
  author: "stellar-team"
  homepage: "https://github.com/stellar/flowctl-stellar-source"
  repository: "https://github.com/stellar/flowctl-stellar-source"
  license: "Apache-2.0"
  tags: [blockchain, stellar, source]

spec:
  type: source
  
  # Execution modes
  execution:
    modes: [container, native]  # Supports both modes
    default: native             # Prefer native for development
  
  # Multi-language support
  languages:
    - name: go
      version: "1.21"
      main: src/main.go
      build:
        # Native build (for in-process execution)
        native:
          command: ["go", "build", "-o", "stellar-source", "./src"]
          binary: "stellar-source"
        # Container build  
        container:
          dockerfile: |
            FROM alpine:latest
            RUN apk --no-cache add ca-certificates
            COPY stellar-source /usr/local/bin/
            ENTRYPOINT ["stellar-source"]
    
    - name: typescript
      version: "20"
      main: typescript/index.ts
      build:
        native:
          command: ["npm", "run", "build"]
          binary: "dist/index.js"
          runtime: "node"
        container:
          dockerfile: |
            FROM node:20-alpine
            WORKDIR /app
            COPY package*.json ./
            RUN npm install --production
            COPY dist ./dist
            ENTRYPOINT ["node", "dist/index.js"]

  # Component interface
  interface:
    outputEventTypes: 
      - stellar.ledger
      - stellar.transaction
    
    config:
      network:
        type: string
        required: true
        description: "Stellar network (mainnet/testnet)"
        enum: [mainnet, testnet]
      
      horizonUrl:
        type: string
        required: false
        description: "Horizon API URL"
        default: "https://horizon.stellar.org"
      
      startLedger:
        type: integer
        required: false
        description: "Starting ledger sequence"
        default: 0

  # Resource requirements (for container mode)
  resources:
    requests:
      cpu: "100m"
      memory: "128Mi"
    limits:
      cpu: "500m"
      memory: "256Mi"

  # Platform support
  platforms:
    - linux/amd64
    - linux/arm64
    - darwin/amd64
```

### Bundle Specification (`bundle.yaml`)

Based on the ttp-processor-demo pattern:

```yaml
apiVersion: component.flowctl.io/v1
kind: ComponentBundle
metadata:
  name: stellar-processing-suite
  version: v1.0.0
  namespace: stellar
  description: "Complete Stellar TTP processing pipeline"
  author: "stellar-team"
  repository: "https://github.com/withObsrvr/ttp-processor-demo"
  license: "Apache-2.0"
  tags: [blockchain, stellar, ttp, bundle]

spec:
  # Bundle execution modes
  execution:
    modes: [container, native, hybrid]  # Hybrid: containers for infra, native for pipeline
    default: native
  
  # Communication protocol configuration
  communication:
    protocol: grpc
    preserveInNative: true  # Keep gRPC even in native mode
    ports:
      stellar-live-source: 50051
      ttp-processor: 50052
      consumer-app: 50053
    discovery: localhost  # Use localhost for native mode
  
  # Components in this bundle
  components:
    - name: stellar-live-source
      path: stellar-live-source/
      type: source
      language: go
      outputEventTypes: ["stellar.ledger", "stellar.transaction"]
      communication:
        protocol: grpc
        port: 50051
        streams: ["RawLedger"]
      
    - name: stellar-datalake-source  
      path: stellar-live-source-datalake/
      type: source
      language: go
      outputEventTypes: ["stellar.ledger", "stellar.transaction"]
      communication:
        protocol: grpc
        port: 50051
        streams: ["RawLedger"]
      
    - name: ttp-processor
      path: ttp-processor/
      type: processor
      language: typescript
      inputEventTypes: ["stellar.transaction"]
      outputEventTypes: ["ttp.event"]
      communication:
        protocol: grpc
        port: 50052
        streams: ["TokenTransferEvent"]
        consumes: ["RawLedger"]
      
    - name: consumer-app
      path: consumer_app/
      type: sink
      language: javascript
      inputEventTypes: ["ttp.event"]
      communication:
        protocol: grpc
        port: 50053
        consumes: ["TokenTransferEvent"]

  # Shared configuration for all components
  shared:
    environment:
      LOG_LEVEL: info
      METRICS_PORT: 9090
      # gRPC-specific configuration
      LIVE_SOURCE_ENDPOINT: localhost:50051
      TTP_PROCESSOR_ENDPOINT: localhost:50052
      
  # Pre-configured pipeline templates
  templates:
    - name: rpc-pipeline
      description: "Process TTP events from Stellar RPC"
      components:
        - stellar-live-source:
            config:
              dataSource: rpc
              rpcEndpoint: ${STELLAR_RPC_URL}
        - ttp-processor:
            config:
              sourceEndpoint: localhost:50051
        - consumer-app:
            config:
              processorEndpoint: localhost:50052
    
    - name: datalake-pipeline
      description: "Process TTP events from data lake"
      components:
        - stellar-datalake-source:
            config:
              dataSource: datalake
              storageBackend: ${STORAGE_BACKEND}
        - ttp-processor:
            config:
              sourceEndpoint: localhost:50051
        - consumer-app:
            config:
              processorEndpoint: localhost:50052

  # Bundle-level configuration
  config:
    stellar:
      network:
        type: string
        default: mainnet
        description: "Stellar network"
        env: NETWORK_PASSPHRASE
      
    output:
      format:
        type: string
        default: json
        enum: [json, csv, parquet]
        description: "Output format"
```

## Registry Service

### API Endpoints

```
# Component discovery
GET /v1/components                              # List all components
GET /v1/components/search?q=stellar&type=source  # Search components
GET /v1/components/:namespace/:name             # Get component details
GET /v1/components/:namespace/:name/versions    # List versions
GET /v1/components/:namespace/:name/:version    # Get specific version

# Artifact management
GET /v1/components/:namespace/:name/:version/download?lang=go&platform=linux/amd64
POST /v1/components/:namespace/:name/publish    # Publish component
POST /v1/components/:namespace/:name/webhook    # GitHub webhook

# Registry management
GET /v1/health                                  # Health check
GET /v1/stats                                   # Registry statistics
```

### Component Resolution

When a pipeline references a component or bundle:

```yaml
# Single component usage
sources:
  - name: stellar-data
    component: stellar/stellar-source@v1.2.3
    config:
      network: mainnet

# Bundle usage
sources:
  - name: stellar-data
    bundle: stellar/stellar-processing-suite@v1.0.0
    component: stellar-live-source
    template: rpc-pipeline
    config:
      network: mainnet
```

The resolution process:

1. **Parse reference**: Extract namespace, name, version constraint, execution mode
2. **Local cache check**: Look in `~/.flowctl/components/` or `~/.flowctl/bundles/`
3. **Registry lookup**: Query registry for component/bundle metadata
4. **Version resolution**: Apply semantic versioning (^1.2.3)
5. **Execution mode selection**: Choose native (default) or container mode
6. **Platform selection**: Choose appropriate platform (linux/amd64)
7. **Language preference**: Default based on component spec
8. **Artifact download**: Download signed binary or container image
9. **Signature verification**: Verify cryptographic signature
10. **Local installation**: Cache in local component directory

## Native Execution Engine

### Dual Execution Modes

The registry supports two execution modes that can be mixed within a single pipeline:

**1. Container Mode (Production)**
- Components run in isolated containers
- Full Docker/Kubernetes compatibility
- Better security and resource isolation
- Network-based communication via gRPC

**2. Native Mode (Development)**  
- Components run as native processes
- In-memory communication via channels
- Faster iteration and debugging
- No container overhead

**3. Hybrid Mode**
- Infrastructure services in containers (Redis, Kafka, etc.)
- Pipeline components run natively
- Best of both worlds for development

### Native Component Interface

Components built for native execution implement Go interfaces:

```go
// Native source interface
type NativeSource interface {
    Start(ctx context.Context) error
    Events() <-chan Event
    Stop() error
}

// Native processor interface  
type NativeProcessor interface {
    Process(ctx context.Context, input <-chan Event, output chan<- Event) error
    Configure(config map[string]interface{}) error
}

// Native sink interface
type NativeSink interface {
    Write(ctx context.Context, input <-chan Event) error
    Configure(config map[string]interface{}) error
}
```

### Multi-Language Native Support

**Go Components**: Direct interface implementation
```go
// Compiled as Go plugin or linked binary
func NewStellarSource(config Config) NativeSource {
    return &StellarSource{config: config}
}
```

**TypeScript/JavaScript Components**: Node.js runtime bridge
```typescript
// Runs via Node.js subprocess with IPC
export class TTPProcessor implements ProcessorInterface {
    async process(event: Event): Promise<Event> {
        // TypeScript processing logic
        return transformedEvent;
    }
}
```

**Python Components**: Python runtime bridge
```python
# Runs via Python subprocess with IPC
class DataEnricher:
    def process(self, event: dict) -> dict:
        # Python processing logic
        return enriched_event
```

### Runtime Bridge Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                Native Execution Engine                      │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌────────┐ │
│  │Go Source │    │TS Proc   │    │Py Proc   │    │Go Sink │ │
│  │:50051    │    │:50052    │    │:50053    │    │:50054  │ │
│  └────┬─────┘    └─────┬────┘    └─────┬────┘    └───┬────┘ │
│       │                │                │            │      │
├───────┼────────────────┼────────────────┼────────────┼──────┤
│       │          Protocol Abstraction Layer          │      │
│  ┌────▼──────────────────▼──────────────▼────────────▼────┐ │
│  │ • gRPC local routing (preserves existing protocols)    │ │
│  │ • Kafka embedded broker (in-memory for development)    │ │
│  │ • HTTP local proxy (maintain REST communication)       │ │
│  │ • Direct channels (optional optimization)              │ │
│  │ • Protocol bridging (gRPC ↔ Kafka ↔ HTTP ↔ channels)   │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

**Key Insight**: Native execution preserves existing communication protocols (gRPC, Kafka, HTTP) while running components as native processes. This ensures zero component rewrites while enabling rapid development iteration.

### Native Development Workflow

```bash
# Install bundle for native development
flowctl bundle install stellar/ttp-processing-suite

# Run entire pipeline natively (no Docker, preserves gRPC)
flowctl dev --bundle stellar/ttp-processing-suite --template rpc-pipeline --mode native

# Pipeline starts all components as native processes:
# ✓ stellar-live-source (Go binary on localhost:50051, gRPC server)
# ✓ ttp-processor (Node.js subprocess on localhost:50052, gRPC server)  
# ✓ consumer-app (Node.js subprocess on localhost:50053, gRPC server)
# ✓ Components communicate via gRPC (same as container mode)
# ✓ Zero component code changes required

# Make changes to any component
# Hot reload automatically restarts affected components
# gRPC connections automatically reconnect
```

## Build Service

### Multi-Language Build Pipeline

```go
type BuildService struct {
    builders map[string]LanguageBuilder
}

type LanguageBuilder interface {
    Detect(repo *git.Repository) bool
    Build(ctx context.Context, spec ComponentSpec, lang LanguageConfig) (*BuildResult, error)
}

// Go builder
type GoBuilder struct{}

func (g *GoBuilder) Build(ctx context.Context, spec ComponentSpec, lang LanguageConfig) (*BuildResult, error) {
    // 1. Set up Go environment
    cmd := exec.CommandContext(ctx, "go", "mod", "download")
    
    // 2. Build binary
    buildCmd := exec.CommandContext(ctx, lang.Build.Command...)
    
    // 3. Create Dockerfile
    dockerfile := lang.Build.Dockerfile
    if dockerfile == "" {
        dockerfile = g.DefaultDockerfile()
    }
    
    // 4. Build container
    containerTag := fmt.Sprintf("registry.flowctl.io/%s/%s:%s-go", spec.Namespace, spec.Name, spec.Version)
    
    return &BuildResult{
        Language: "go",
        Platform: "linux/amd64",
        Image: containerTag,
        Size: imageSize,
        BuildTime: time.Since(start),
    }, nil
}
```

### Build Caching

```
Registry Build Cache:
├── stellar/
│   └── stellar-source/
│       ├── v1.2.3/
│       │   ├── go-linux-amd64.tar.gz
│       │   ├── typescript-linux-amd64.tar.gz
│       │   └── python-linux-amd64.tar.gz
│       └── v1.2.2/
│           └── go-linux-amd64.tar.gz
└── auth/
    └── jwt-processor/
        └── v2.1.0/
            └── go-linux-amd64.tar.gz
```

## CLI Integration

### Component Commands

```bash
# Search and discovery
flowctl component search stellar                    # Search components
flowctl component info stellar/stellar-source      # Component details
flowctl component versions stellar/stellar-source  # List versions

# Installation and management
flowctl component install stellar/stellar-source@v1.2.3
flowctl component install stellar/stellar-source@^1.2.0  # Semantic versioning
flowctl component list                              # List installed
flowctl component update                            # Update all
flowctl component clean                             # Clean cache

# Development
flowctl component init my-processor --type processor --lang go
flowctl component build                             # Build locally
flowctl component test                              # Run tests
flowctl component publish --namespace myorg         # Publish to registry
```

### Bundle Commands

```bash
# Bundle discovery and management
flowctl bundle search stellar                       # Search bundles
flowctl bundle info stellar/ttp-processing-suite   # Bundle details
flowctl bundle install stellar/ttp-processing-suite@v1.0.0
flowctl bundle list                                 # List installed bundles

# Bundle development and publishing
flowctl bundle init stellar-suite --template multi-component
flowctl bundle validate                            # Validate bundle.yaml
flowctl bundle build                               # Build all components
flowctl bundle test                                # Test entire bundle
flowctl bundle publish --namespace stellar         # Publish bundle

# Pipeline generation from bundles
flowctl pipeline generate stellar/ttp-processing-suite:rpc-pipeline
flowctl pipeline generate stellar/ttp-processing-suite:datalake-pipeline
```

### Development Commands

```bash
# Native development mode (no Docker required)
flowctl dev pipeline.yaml --mode native           # Run natively
flowctl dev pipeline.yaml --mode hybrid           # Hybrid mode
flowctl dev pipeline.yaml --mode container        # Container mode

# Bundle-based development
flowctl dev --bundle stellar/ttp-processing-suite --template rpc-pipeline

# Hot reload and debugging
flowctl dev --watch                               # Auto-restart on changes
flowctl dev --debug                               # Enable debug logging
flowctl dev --profile                             # Enable profiling

# Multi-language component creation
flowctl component create my-source --lang go --template http-source
flowctl component create my-processor --lang typescript --template transform
flowctl component create my-sink --lang python --template database-writer
```

### Pipeline Configuration

```yaml
# Bundle-based pipeline (recommended)
apiVersion: flowctl/v1
kind: Pipeline
metadata:
  name: stellar-ttp-processing
spec:
  # Use bundle with template
  bundle: stellar/ttp-processing-suite@v1.0.0
  template: rpc-pipeline
  
  # Execution mode
  execution:
    mode: native  # native, container, or hybrid
    
  # Override bundle configuration
  config:
    stellar:
      network: mainnet
    output:
      format: json

---
# Component-based pipeline (traditional)
apiVersion: flowctl/v1
kind: Pipeline
metadata:
  name: stellar-analytics
spec:
  # Execution mode
  execution:
    mode: hybrid  # containers for infra, native for pipeline
    
  # Component dependencies
  components:
    - stellar/stellar-source@v1.2.3
    - stellar/transaction-processor@^1.0.0
    - analytics/clickhouse-sink@v2.1.0
  
  # Pipeline definition
  sources:
    - name: stellar-data
      component: stellar/stellar-source@v1.2.3
      config:
        network: mainnet
        horizonUrl: https://horizon.stellar.org
  
  processors:
    - name: process-transactions
      component: stellar/transaction-processor@^1.0.0
      inputs: [stellar-data]
      config:
        filterType: payment
        minAmount: 1000
  
  sinks:
    - name: analytics-db
      component: analytics/clickhouse-sink@v2.1.0
      inputs: [process-transactions]
      config:
        host: localhost:9000
        database: stellar_analytics
        table: transactions

---
# Mixed bundle and individual components
apiVersion: flowctl/v1
kind: Pipeline
metadata:
  name: hybrid-pipeline
spec:
  execution:
    mode: native
    
  # Use source from bundle
  sources:
    - name: stellar-data
      bundle: stellar/ttp-processing-suite@v1.0.0
      component: stellar-live-source
      config:
        network: mainnet
        
  # Use individual processor
  processors:
    - name: custom-processor
      component: myorg/custom-processor@v2.0.0
      inputs: [stellar-data]
      
  # Use sink from different bundle
  sinks:
    - name: analytics
      bundle: analytics/data-suite@v1.5.0
      component: clickhouse-sink
      inputs: [custom-processor]
```

## Security Model

### Component Signing

```go
type ComponentSigner struct {
    privateKey *rsa.PrivateKey
    publicKey  *rsa.PublicKey
}

func (s *ComponentSigner) SignComponent(artifact []byte) (*Signature, error) {
    hash := sha256.Sum256(artifact)
    signature, err := rsa.SignPKCS1v15(rand.Reader, s.privateKey, crypto.SHA256, hash[:])
    
    return &Signature{
        Algorithm: "RS256",
        Hash:      hex.EncodeToString(hash[:]),
        Value:     base64.StdEncoding.EncodeToString(signature),
        PublicKey: s.publicKey,
    }, err
}
```

### Trust Model

1. **Registry-signed components**: All components signed by registry
2. **Publisher verification**: GitHub organization verification
3. **Vulnerability scanning**: Automated security scanning
4. **Community ratings**: User reviews and ratings
5. **Audit trails**: All operations logged

## Usage Examples

### Publishing a Component

```bash
# Create new component
mkdir stellar-source && cd stellar-source
flowctl component init --type source --lang go

# Implement component
cat > component.yaml << EOF
apiVersion: component.flowctl.io/v1
kind: ComponentSpec
metadata:
  name: stellar-source
  version: v1.0.0
  namespace: stellar
spec:
  type: source
  languages:
    - name: go
      build:
        command: ["go", "build", "-o", "stellar-source", "./cmd/source"]
EOF

# Build and test locally
flowctl component build
flowctl component test --input examples/test-config.yaml

# Publish to registry
flowctl component publish --namespace stellar
```

### Using Components in Pipeline

```bash
# Search for components
flowctl component search --type source blockchain

# Install specific component
flowctl component install stellar/stellar-source@v1.2.3

# Generate pipeline template
flowctl pipeline generate \
  --source stellar/stellar-source \
  --processor data/json-transform \
  --sink analytics/clickhouse-sink

# Validate pipeline
flowctl pipeline validate stellar-pipeline.yaml

# Deploy pipeline
flowctl apply stellar-pipeline.yaml
```

## Benefits

### For Component Authors
- **Multi-language support**: Write in preferred language
- **Automatic building**: No Docker expertise required
- **Version management**: Semantic versioning built-in
- **Distribution**: Automatic packaging and distribution
- **Discoverability**: Components findable in registry

### For Pipeline Developers
- **No container building**: Use components directly
- **Rich ecosystem**: Reuse community components
- **Type safety**: Component interfaces validated
- **Dependency management**: Automatic resolution
- **Rapid development**: Mix and match components

### For Organizations
- **Private registries**: Host internal components
- **Security scanning**: Automatic vulnerability detection
- **Audit trails**: Complete usage tracking
- **Compliance**: Signed and verified components
- **Governance**: Component approval workflows

## Implementation Roadmap

### Phase 1: Core Registry & Native Execution (Months 1-2)
- [ ] Registry API service with bundle support
- [ ] Component and bundle metadata management
- [ ] GitHub integration for multi-component repositories
- [ ] Native execution engine for Go components
- [ ] Basic CLI commands (component, bundle, dev)
- [ ] Multi-language build pipeline

### Phase 2: Bundle Ecosystem & Developer Experience (Months 2-3)
- [ ] Bundle templates and generation
- [ ] Multi-language native execution (TypeScript, Python)
- [ ] Component/bundle search and discovery
- [ ] Hot reload and development mode
- [ ] Pipeline generation from bundle templates
- [ ] Local development without containers

### Phase 3: Production Features & Advanced Building (Months 3-4)
- [ ] Container and native artifact dual builds
- [ ] Hybrid execution mode (containers + native)
- [ ] Component resolution in production pipelines
- [ ] Artifact signing and verification
- [ ] Advanced caching strategies
- [ ] Performance optimization

### Phase 4: Enterprise & Ecosystem (Months 4-6)
- [ ] Private registries for organizations
- [ ] Component certification and security scanning
- [ ] Community marketplace and ratings
- [ ] Advanced pipeline templates
- [ ] Monitoring and analytics
- [ ] Enterprise security and compliance

### Phase 5: Advanced Runtime & Optimization (Months 6+)
- [ ] WASM plugin support (inspired by Flow repository)
- [ ] Advanced native runtime optimizations
- [ ] Distributed pipeline execution
- [ ] Auto-scaling and resource management
- [ ] Integration with cloud platforms
- [ ] Performance benchmarking and optimization

## Conclusion

The enhanced component registry eliminates the biggest friction points in flowctl while enabling powerful new development patterns:

### Key Improvements

**1. Eliminates Docker Build Friction**
- Multi-component bundles reduce build overhead
- Native execution mode removes container requirements for development
- Registry builds all artifacts automatically from source

**2. Embraces Real-World Patterns**
- Multi-component repositories match how developers actually organize code
- Language diversity within single pipelines (Go + TypeScript + Python)
- Bundle templates based on proven patterns (ttp-processor-demo, Flow)

**3. Progressive Developer Experience**
- Start with bundle templates, customize as needed
- Native development mode for rapid iteration
- Container mode for production deployment
- Hybrid mode for best of both worlds

**4. Production-Ready Features**
- Dual artifact builds (native + container)
- Cryptographic signing and verification
- Private registries for organizations
- Enterprise security and compliance

### Impact

This approach transforms flowctl from a container-orchestrator into a developer-friendly platform where:

- **Developers** can write multi-language components without Docker expertise
- **Users** get instant access to battle-tested component bundles
- **Organizations** can standardize on proven pipeline patterns
- **The community** can build and share complete processing suites

By learning from the repository evolution (ttp-processor-demo → cdp-pipeline-workflow → Flow) and successful registries (Terraform, npm, Go modules), we create an ecosystem that makes stream processing as accessible as web development.

The registry doesn't just store components—it enables patterns that reduce complexity while maintaining the power needed for production use cases.