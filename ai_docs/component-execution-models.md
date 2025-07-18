# Component Execution Models: Containers vs In-Process

## The Problem: Container-First Creates Friction

Currently, flowctl requires every component (source, processor, sink) to be packaged as a Docker image. This means to create a simple pipeline with custom logic, a developer must:

1. Write source code
2. Create Dockerfile
3. Build Docker image
4. Push to registry (or use local)
5. Reference in pipeline YAML
6. Repeat for EACH component

This is excessive friction for experimentation and development.

## Current Architecture Analysis

### Why Containers Are Required

Looking at the codebase, containers are deeply embedded in the architecture:

```go
// From internal/core/component.go
type Component struct {
    ID          string
    Type        ComponentType
    Image       string   // Always expects a container image
    Command     []string // Container command
    Environment map[string]string
    Ports       []Port   // Container ports
    // ... more container-specific fields
}
```

The DAG executor starts components as containers:

```go
// From internal/translator/docker.go
func generateService(comp Component) {
    service := Service{
        Image:       comp.Image,  // Must have an image
        Command:     comp.Command,
        Environment: comp.Environment,
        // ... container configuration
    }
}
```

### Component Communication

Components communicate via gRPC through the control plane:

```go
// Components must:
// 1. Register with control plane
// 2. Implement health checks
// 3. Send heartbeats
// 4. Handle gRPC streaming

// This assumes network isolation between components
```

### No In-Process Support

The simple pipeline executor has hardcoded mocks:

```go
// From internal/core/pipeline.go
func (p *SimplePipeline) Run(ctx context.Context) error {
    // Hardcoded test fixtures only
    src := testfixtures.CreateMockSource()
    proc := testfixtures.CreateMockProcessor(p.Processors[0].Name)
    snk := sink.NewStdoutSink()
    
    // No way to provide custom implementations
}
```

## The Developer Experience Impact

### Current Workflow (High Friction)

```bash
# 1. Create source
mkdir my-source && cd my-source
cat > main.go << EOF
// Custom source implementation
EOF
cat > Dockerfile << EOF
FROM golang:1.21
COPY . .
RUN go build -o source
CMD ["./source"]
EOF
docker build -t my-source .

# 2. Repeat for processor
# ... same process ...

# 3. Repeat for sink  
# ... same process ...

# 4. Finally run pipeline
cat > pipeline.yaml << EOF
sources:
  - image: my-source:latest
processors:
  - image: my-processor:latest
sinks:
  - image: my-sink:latest
EOF

flowctl run pipeline.yaml  # Still needs Docker daemon
```

### Desired Workflow (Low Friction)

```bash
# Write pipeline with inline code
cat > pipeline.yaml << EOF
source:
  type: custom
  code: |
    func (s *Source) Next() (Event, error) {
        return Event{Data: "hello"}, nil
    }

process:
  - type: transform
    code: |
      func (p *Processor) Process(e Event) (Event, error) {
          e.Data = strings.ToUpper(e.Data)
          return e, nil
      }

sink:
  type: stdout
EOF

flowctl dev pipeline.yaml  # Runs immediately, no Docker needed
```

## Proposed Solution: Hybrid Execution Model

### 1. Component Registry

Create a registry for built-in and custom components:

```go
// internal/core/registry/registry.go
type ComponentRegistry struct {
    sources    map[string]SourceFactory
    processors map[string]ProcessorFactory
    sinks      map[string]SinkFactory
}

type SourceFactory func(config map[string]interface{}) (Source, error)

// Register built-in components
func init() {
    DefaultRegistry.RegisterSource("file", NewFileSource)
    DefaultRegistry.RegisterSource("http", NewHTTPSource)
    DefaultRegistry.RegisterSource("kafka", NewKafkaSource)
    
    DefaultRegistry.RegisterProcessor("filter", NewFilterProcessor)
    DefaultRegistry.RegisterProcessor("transform", NewTransformProcessor)
    
    DefaultRegistry.RegisterSink("stdout", NewStdoutSink)
    DefaultRegistry.RegisterSink("file", NewFileSink)
}
```

### 2. Extended Component Definition

Support both execution modes:

```go
type Component struct {
    ID       string
    Type     ComponentType
    Mode     ExecutionMode  // "container" or "in-process"
    
    // For container mode
    Image    string
    Command  []string
    
    // For in-process mode
    Plugin   string         // Component type from registry
    Config   map[string]interface{}
    Code     string         // Optional inline code
}

type ExecutionMode string
const (
    ContainerMode ExecutionMode = "container"
    InProcessMode ExecutionMode = "in-process"
)
```

### 3. Updated Configuration Schema

```yaml
# Container mode (backward compatible)
source:
  mode: container
  image: my-source:latest
  
# In-process mode with built-in component
source:
  mode: in-process  # Optional, can be inferred
  type: kafka
  config:
    brokers: localhost:9092
    topic: events
    
# In-process mode with inline code
processor:
  type: custom
  code: |
    func Process(event Event) (Event, error) {
        // Transform logic here
        return event, nil
    }
```

### 4. Component Factory

Update the component creation logic:

```go
func createComponent(def ComponentDef) (Component, error) {
    switch def.Mode {
    case InProcessMode:
        return createInProcessComponent(def)
    case ContainerMode:
        return createContainerComponent(def)
    default:
        // Auto-detect: if image is specified, use container mode
        if def.Image != "" {
            return createContainerComponent(def)
        }
        return createInProcessComponent(def)
    }
}

func createInProcessComponent(def ComponentDef) (Component, error) {
    // Look up in registry
    factory, ok := registry.GetFactory(def.Type)
    if !ok {
        // Try to compile inline code
        if def.Code != "" {
            return compileInlineComponent(def.Code)
        }
        return nil, fmt.Errorf("unknown component type: %s", def.Type)
    }
    
    return factory(def.Config)
}
```

### 5. Built-in Components

Provide a rich library of components:

```go
// sources/file.go
type FileSource struct {
    path     string
    watcher  *fsnotify.Watcher
}

func (f *FileSource) Start(ctx context.Context, out chan<- Event) error {
    // Watch file and emit events
}

// processors/filter.go  
type FilterProcessor struct {
    expression string
    compiled   *govaluate.EvaluableExpression
}

func (f *FilterProcessor) Process(event Event) (Event, bool, error) {
    // Evaluate filter expression
    result, err := f.compiled.Evaluate(event.Data)
    return event, result.(bool), err
}

// sinks/http.go
type HTTPSink struct {
    url     string
    client  *http.Client
}

func (h *HTTPSink) Write(event Event) error {
    // POST event to HTTP endpoint
}
```

### 6. Development Mode Pipeline

For development, run everything in a single process:

```go
func (p *Pipeline) RunDevelopment(ctx context.Context) error {
    // Create all components in-process
    components := make([]Component, 0)
    
    for _, def := range p.Components {
        comp, err := createComponent(def)
        if err != nil {
            return err
        }
        components = append(components, comp)
    }
    
    // Wire them together with channels
    channels := createChannels(len(components))
    
    // Start each component with local channels
    for i, comp := range components {
        go comp.Run(ctx, channels[i], channels[i+1])
    }
    
    // No gRPC, no containers, just channels
    <-ctx.Done()
    return nil
}
```

## Implementation Phases

### Phase 1: Core Registry (Week 1-2)
- [ ] Create component registry system
- [ ] Define standard interfaces
- [ ] Implement 3-5 basic components

### Phase 2: Configuration Support (Week 2-3)
- [ ] Extend YAML schema for mode selection
- [ ] Update configuration parser
- [ ] Add validation for in-process components

### Phase 3: Execution Engine (Week 3-4)
- [ ] Modify pipeline executor for dual-mode
- [ ] Implement in-process wiring
- [ ] Add development mode runner

### Phase 4: Component Library (Week 4-8)
- [ ] Build comprehensive component library
- [ ] Add inline code compilation
- [ ] Create component documentation

## Benefits

### For Developers

1. **Instant Gratification**: Run pipelines immediately without building images
2. **Rapid Iteration**: Change code and re-run in seconds
3. **Easy Debugging**: Set breakpoints in component code
4. **Lower Resource Usage**: No container overhead for simple pipelines

### For Production

1. **Flexibility**: Mix in-process and containerized components
2. **Performance**: In-process components have lower latency
3. **Compatibility**: Existing container-based pipelines still work
4. **Progressive Enhancement**: Start simple, containerize when needed

## Example: Before and After

### Before (Current)
```bash
# 30+ minutes to get custom pipeline running
1. Write source code (5 min)
2. Create Dockerfiles (5 min)
3. Build images (10 min)
4. Debug image issues (10 min)
5. Finally run pipeline

# Each iteration requires rebuilding images
```

### After (Proposed)
```bash
# 2 minutes to get custom pipeline running
1. Write pipeline YAML with inline code
2. Run `flowctl dev`

# Iterations take seconds, not minutes
```

## Migration Path

1. **Phase 1**: Add in-process support alongside containers
2. **Phase 2**: Provide built-in component library
3. **Phase 3**: Deprecate container requirement for development
4. **Phase 4**: Containers become optional optimization

## Conclusion

The container requirement is the single biggest friction point for flowctl adoption. By supporting in-process components, we can:

- Reduce time-to-first-pipeline from 30+ minutes to 2 minutes
- Enable rapid experimentation and prototyping
- Maintain production-grade container support
- Create a progressive path from development to production

This hybrid approach gives developers the best of both worlds: quick iteration for development and container isolation for production.