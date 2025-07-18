# Plan: Enable flowctl to Execute Real Components

## Current State Analysis

### What Works
- `flowctl server` runs and components can register with it
- Manual component execution works (components connect successfully)
- Pipeline configuration supports full component definitions (image, command, env, etc.)
- DAG-based pipeline architecture exists for complex topologies

### What's Missing
- Component factory functions (`createSource`, `createProcessor`, `createSink`) are hardcoded to return mock components
- No component lifecycle management (starting/stopping processes)
- No container runtime integration for running component images
- Control plane is passive (only receives registrations, doesn't start components)

## Goal

Enable `flowctl run pipeline.yaml` to automatically start and orchestrate real components from the ttp-processor-demo repository without requiring manual component startup.

## Implementation Plan

### Phase 1: Component Execution Layer (Priority 1)

#### 1.1 Create Component Executor Interface
```go
// internal/executor/executor.go
type ComponentExecutor interface {
    Start(ctx context.Context, component model.Component) (ComponentHandle, error)
    Stop(componentID string) error
    Status(componentID string) ComponentStatus
    Logs(componentID string, follow bool) (io.ReadCloser, error)
}

// ComponentHandle provides lifecycle management for a running component
type ComponentHandle interface {
    ID() string
    WaitReady(ctx context.Context) error
    WaitExit() <-chan error
    Restart(ctx context.Context) error
    Logs(follow bool) (io.ReadCloser, error)
    Status() ComponentStatus
}

type ComponentStatus struct {
    State        ComponentState
    Health       HealthStatus
    LastStateChange time.Time
    ExitCode     *int
    Error        string
}

type ComponentState string
const (
    StateStarting   ComponentState = "starting"
    StateRunning    ComponentState = "running"
    StateUnhealthy  ComponentState = "unhealthy"
    StateStopping   ComponentState = "stopping"
    StateStopped    ComponentState = "stopped"
    StateCrashLoop  ComponentState = "crashloop"
)

type ExecutorType string
const (
    ExecutorTypeDocker ExecutorType = "docker"
    ExecutorTypeProcess ExecutorType = "process"
    ExecutorTypeNomad ExecutorType = "nomad"
    ExecutorTypeKubernetes ExecutorType = "kubernetes"
)
```

#### 1.2 Implement Docker Executor
```go
// internal/executor/docker_executor.go
type DockerExecutor struct {
    client    *docker.Client
    network   string
    handles   sync.Map // componentID -> *DockerHandle
}

type DockerHandle struct {
    id          string
    containerID string
    client      *docker.Client
    readyChan   chan error
    exitChan    chan error
    healthProbe HealthProbe
}

func (e *DockerExecutor) Start(ctx context.Context, component model.Component) (ComponentHandle, error) {
    // 1. Pull image if needed (with digest verification for security)
    if err := e.pullImageWithVerification(ctx, component.Image); err != nil {
        return nil, err
    }
    
    // 2. Create bridge network if needed (avoid host networking)
    networkID, err := e.ensureNetwork(ctx)
    if err != nil {
        return nil, err
    }
    
    // 3. Generate component token for control plane auth
    token, err := generateComponentToken(component.ID)
    if err != nil {
        return nil, err
    }
    
    // 4. Create container with proper port allocation
    containerConfig := &container.Config{
        Image: component.Image,
        Env: append(component.Env, 
            fmt.Sprintf("CONTROL_PLANE_ENDPOINT=%s", e.controlPlaneEndpoint),
            fmt.Sprintf("COMPONENT_TOKEN=%s", token),
        ),
        Cmd: component.Command,
        ExposedPorts: e.createPortSet(component.Ports),
    }
    
    hostConfig := &container.HostConfig{
        NetworkMode:  container.NetworkMode(networkID),
        PortBindings: e.createPortBindings(component.Ports),
        RestartPolicy: container.RestartPolicy{Name: "unless-stopped"},
    }
    
    // 5. Start container and create handle
    resp, err := e.client.ContainerCreate(ctx, containerConfig, hostConfig, nil, nil, component.ID)
    if err != nil {
        return nil, err
    }
    
    if err := e.client.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{}); err != nil {
        return nil, err
    }
    
    handle := &DockerHandle{
        id:          component.ID,
        containerID: resp.ID,
        client:      e.client,
        readyChan:   make(chan error, 1),
        exitChan:    make(chan error, 1),
        healthProbe: createHealthProbe(component.HealthCheck, component.HealthPort),
    }
    
    // Start health monitoring
    go handle.monitorHealth(ctx)
    go handle.monitorExit()
    
    e.handles.Store(component.ID, handle)
    return handle, nil
}

func (h *DockerHandle) WaitReady(ctx context.Context) error {
    // Wait for health probe to succeed or timeout
    select {
    case err := <-h.readyChan:
        return err
    case <-ctx.Done():
        return ctx.Err()
    }
}
```

#### 1.3 Implement Process Executor (for local binaries)
```go
// internal/executor/process_executor.go
type ProcessExecutor struct {
    handles sync.Map // componentID -> *ProcessHandle
}

type ProcessHandle struct {
    id        string
    cmd       *exec.Cmd
    process   *os.Process
    readyChan chan error
    exitChan  chan error
    healthProbe HealthProbe
}

func (e *ProcessExecutor) Start(ctx context.Context, component model.Component) (ComponentHandle, error) {
    // 1. Resolve binary path
    binaryPath, err := e.resolveBinaryPath(component.Binary)
    if err != nil {
        return nil, err
    }
    
    // 2. Generate component token for control plane auth
    token, err := generateComponentToken(component.ID)
    if err != nil {
        return nil, err
    }
    
    // 3. Set up command with environment
    cmd := exec.CommandContext(ctx, binaryPath, component.Args...)
    cmd.Env = append(os.Environ(),
        fmt.Sprintf("CONTROL_PLANE_ENDPOINT=%s", e.controlPlaneEndpoint),
        fmt.Sprintf("COMPONENT_TOKEN=%s", token),
    )
    for k, v := range component.Env {
        cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
    }
    
    // 4. Start process
    if err := cmd.Start(); err != nil {
        return nil, err
    }
    
    handle := &ProcessHandle{
        id:        component.ID,
        cmd:       cmd,
        process:   cmd.Process,
        readyChan: make(chan error, 1),
        exitChan:  make(chan error, 1),
        healthProbe: createHealthProbe(component.HealthCheck, component.HealthPort),
    }
    
    // Start monitoring
    go handle.monitorHealth(ctx)
    go handle.monitorExit()
    
    e.handles.Store(component.ID, handle)
    return handle, nil
}
```

#### 1.4 Implement Nomad Executor (Priority 2)
```go
// internal/executor/nomad_executor.go
type NomadExecutor struct {
    client     *nomad.Client
    dataCenter string
    namespace  string
    handles    sync.Map
}

func (e *NomadExecutor) Start(ctx context.Context, component model.Component) (ComponentHandle, error) {
    // 1. Create Nomad job specification
    job := &nomad.Job{
        ID:         stringPtr(component.ID),
        Name:       stringPtr(component.ID),
        Type:       stringPtr("service"),
        Datacenters: []string{e.dataCenter},
        TaskGroups: []*nomad.TaskGroup{
            {
                Name: stringPtr(component.ID),
                Tasks: []*nomad.Task{
                    {
                        Name:   component.ID,
                        Driver: "docker",
                        Config: map[string]interface{}{
                            "image": component.Image,
                            "command": component.Command[0],
                            "args": component.Command[1:],
                        },
                        Env: e.buildEnvMap(component),
                        Resources: &nomad.Resources{
                            CPU:      intPtr(component.Resources.CPU),
                            MemoryMB: intPtr(component.Resources.Memory),
                        },
                        Services: []*nomad.Service{
                            {
                                Name: component.ID,
                                Port: component.HealthPort,
                                Checks: []*nomad.ServiceCheck{
                                    {
                                        Type:     "http",
                                        Path:     component.HealthCheck,
                                        Interval: time.Duration(10 * time.Second),
                                        Timeout:  time.Duration(5 * time.Second),
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
    }
    
    // 2. Submit job to Nomad
    resp, _, err := e.client.Jobs().Register(job, nil)
    if err != nil {
        return nil, err
    }
    
    handle := &NomadHandle{
        id:           component.ID,
        client:       e.client,
        evaluationID: resp.EvalID,
        readyChan:    make(chan error, 1),
        exitChan:     make(chan error, 1),
    }
    
    // 3. Monitor job status
    go handle.monitorStatus(ctx)
    
    e.handles.Store(component.ID, handle)
    return handle, nil
}
```

### Phase 2: Enhanced Component Factory (Priority 1)

#### 2.1 Update Factory Functions with Proper Lifecycle Management
```go
// internal/core/component_factory.go
func createSource(component model.Component, executor ComponentExecutor, controlPlane ControlPlaneClient) (source.Source, error) {
    // 1. Start the component using executor
    handle, err := executor.Start(context.Background(), component)
    if err != nil {
        return nil, fmt.Errorf("failed to start source %s: %w", component.ID, err)
    }
    
    // 2. Wait for component to be ready with timeout
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    
    if err := handle.WaitReady(ctx); err != nil {
        // Cleanup on failure
        handle.Stop()
        return nil, fmt.Errorf("source %s failed to become ready: %w", component.ID, err)
    }
    
    // 3. Wait for component to register with control plane
    if err := controlPlane.WaitForComponent(component.ID, 15*time.Second); err != nil {
        handle.Stop()
        return nil, fmt.Errorf("source %s failed to register: %w", component.ID, err)
    }
    
    // 4. Return a proxy source that communicates with the real component
    return &RemoteSource{
        componentID:  component.ID,
        handle:       handle,
        controlPlane: controlPlane,
    }, nil
}

func createProcessor(component model.Component, executor ComponentExecutor, controlPlane ControlPlaneClient) (processor.Processor, error) {
    // Similar implementation with processor-specific logic
    handle, err := executor.Start(context.Background(), component)
    if err != nil {
        return nil, fmt.Errorf("failed to start processor %s: %w", component.ID, err)
    }
    
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    
    if err := handle.WaitReady(ctx); err != nil {
        handle.Stop()
        return nil, fmt.Errorf("processor %s failed to become ready: %w", component.ID, err)
    }
    
    if err := controlPlane.WaitForComponent(component.ID, 15*time.Second); err != nil {
        handle.Stop()
        return nil, fmt.Errorf("processor %s failed to register: %w", component.ID, err)
    }
    
    return &RemoteProcessor{
        componentID:  component.ID,
        handle:       handle,
        controlPlane: controlPlane,
    }, nil
}
```

#### 2.2 Create Remote Component Proxies
```go
// internal/core/remote_components.go
type RemoteSource struct {
    componentID string
    controlPlane ControlPlaneClient
}

func (r *RemoteSource) Events(ctx context.Context, out chan<- source.EventEnvelope) error {
    // Subscribe to events from the component via control plane
    stream, err := r.controlPlane.SubscribeToComponent(r.componentID)
    if err != nil {
        return err
    }
    
    // Forward events to the output channel
    for {
        event, err := stream.Recv()
        if err != nil {
            return err
        }
        out <- event
    }
}
```

### Phase 3: Enhanced Control Plane Integration (Priority 2)

#### 3.1 Richer Component Status Tracking
```go
// internal/api/control_plane.go
type ComponentRegistration struct {
    ID               string
    Type            string // "source", "processor", "sink"
    Status          ComponentState
    HealthEndpoint  string
    LastHeartbeat   time.Time
    StartTime       time.Time
    Metadata        map[string]string
    AuthToken       string
    RestartCount    int
}

func (cp *ControlPlane) RegisterComponent(req *RegisterRequest) error {
    // Validate component token
    if !cp.validateComponentToken(req.ComponentID, req.Token) {
        return fmt.Errorf("invalid component token")
    }
    
    // Enhanced registration tracking
    cp.mu.Lock()
    defer cp.mu.Unlock()
    
    cp.components[req.ComponentID] = &ComponentRegistration{
        ID:              req.ComponentID,
        Type:           req.ComponentType,
        Status:         StateRunning,
        HealthEndpoint: req.HealthEndpoint,
        LastHeartbeat:  time.Now(),
        StartTime:      time.Now(),
        Metadata:       req.Metadata,
        AuthToken:      req.Token,
    }
    
    // Notify waiters
    if waitChan, exists := cp.registrationWaiters[req.ComponentID]; exists {
        close(waitChan)
        delete(cp.registrationWaiters, req.ComponentID)
    }
    
    return nil
}

func (cp *ControlPlane) HandleHeartbeat(req *HeartbeatRequest) error {
    cp.mu.Lock()
    defer cp.mu.Unlock()
    
    if comp, exists := cp.components[req.ComponentID]; exists {
        comp.LastHeartbeat = time.Now()
        comp.Status = req.Status
        return nil
    }
    return fmt.Errorf("component not registered: %s", req.ComponentID)
}

func (cp *ControlPlane) WaitForComponent(componentID string, timeout time.Duration) error {
    cp.mu.Lock()
    if _, exists := cp.components[componentID]; exists {
        cp.mu.Unlock()
        return nil // Already registered
    }
    
    // Create waiter channel
    waitChan := make(chan struct{})
    cp.registrationWaiters[componentID] = waitChan
    cp.mu.Unlock()
    
    // Wait with timeout
    select {
    case <-waitChan:
        return nil
    case <-time.After(timeout):
        // Cleanup waiter
        cp.mu.Lock()
        delete(cp.registrationWaiters, componentID)
        cp.mu.Unlock()
        return fmt.Errorf("timeout waiting for component %s", componentID)
    }
}
```

### Phase 4: DAG Orchestration Improvements (Priority 1)

#### 4.1 Enhanced Pipeline Start Logic with Topological Sorting
```go
// internal/core/pipeline_dag.go
func (p *DAGPipeline) buildPipelineGraph() error {
    // Initialize executor based on configuration
    executor, err := createExecutor(p.cfg.ExecutorType, p.controlPlane)
    if err != nil {
        return fmt.Errorf("failed to create executor: %w", err)
    }
    
    // Perform topological sort to determine startup order
    startupOrder, err := p.computeStartupOrder()
    if err != nil {
        return fmt.Errorf("failed to compute startup order: %w", err)
    }
    
    // Start components in dependency order
    for _, componentID := range startupOrder {
        if err := p.startComponent(componentID, executor); err != nil {
            // Cleanup already started components
            p.cleanupStartedComponents()
            return fmt.Errorf("failed to start component %s: %w", componentID, err)
        }
    }
    
    return nil
}

func (p *DAGPipeline) startComponent(componentID string, executor ComponentExecutor) error {
    // Find component configuration
    component, err := p.findComponentByID(componentID)
    if err != nil {
        return err
    }
    
    // Create component based on type
    switch component.Type {
    case "source":
        src, err := createSource(component, executor, p.controlPlane)
        if err != nil {
            return err
        }
        p.sources[componentID] = src
        
    case "processor":
        proc, err := createProcessor(component, executor, p.controlPlane)
        if err != nil {
            return err
        }
        p.processors[componentID] = proc
        
    case "sink":
        sink, err := createSink(component, executor, p.controlPlane)
        if err != nil {
            return err
        }
        p.sinks[componentID] = sink
    }
    
    return nil
}

func (p *DAGPipeline) computeStartupOrder() ([]string, error) {
    // Build dependency graph
    graph := make(map[string][]string)
    inDegree := make(map[string]int)
    
    // Add all components
    allComponents := make([]string, 0)
    for _, src := range p.pipeline.Spec.Sources {
        allComponents = append(allComponents, src.ID)
        inDegree[src.ID] = 0
    }
    for _, proc := range p.pipeline.Spec.Processors {
        allComponents = append(allComponents, proc.ID)
        inDegree[proc.ID] = len(proc.Inputs)
        for _, input := range proc.Inputs {
            graph[input] = append(graph[input], proc.ID)
        }
    }
    for _, sink := range p.pipeline.Spec.Sinks {
        allComponents = append(allComponents, sink.ID)
        inDegree[sink.ID] = len(sink.Inputs)
        for _, input := range sink.Inputs {
            graph[input] = append(graph[input], sink.ID)
        }
    }
    
    // Kahn's algorithm for topological sort
    queue := make([]string, 0)
    for _, comp := range allComponents {
        if inDegree[comp] == 0 {
            queue = append(queue, comp)
        }
    }
    
    result := make([]string, 0)
    for len(queue) > 0 {
        current := queue[0]
        queue = queue[1:]
        result = append(result, current)
        
        for _, neighbor := range graph[current] {
            inDegree[neighbor]--
            if inDegree[neighbor] == 0 {
                queue = append(queue, neighbor)
            }
        }
    }
    
    if len(result) != len(allComponents) {
        return nil, fmt.Errorf("circular dependency detected in pipeline")
    }
    
    return result, nil
}
```

#### 4.2 Restart Policies and Failure Handling
```go
// internal/core/restart_manager.go
type RestartManager struct {
    policies   map[string]RestartPolicy
    executor   ComponentExecutor
    handles    sync.Map // componentID -> ComponentHandle
}

type RestartPolicy struct {
    Policy      string        // "always", "on-failure", "unless-stopped"
    MaxRetries  int
    BackoffType string        // "exponential", "linear", "fixed"
    InitialDelay time.Duration
    MaxDelay    time.Duration
}

func (rm *RestartManager) MonitorComponent(componentID string, handle ComponentHandle) {
    go func() {
        retryCount := 0
        policy := rm.policies[componentID]
        
        for {
            select {
            case err := <-handle.WaitExit():
                if err == nil {
                    // Clean exit, don't restart unless policy is "always"
                    if policy.Policy != "always" {
                        return
                    }
                } else {
                    // Error exit, check restart policy
                    if policy.Policy == "never" {
                        return
                    }
                    if retryCount >= policy.MaxRetries {
                        logger.Error("Component exceeded max retries", 
                            zap.String("component", componentID),
                            zap.Int("retries", retryCount))
                        return
                    }
                }
                
                // Calculate backoff delay
                delay := rm.calculateBackoff(policy, retryCount)
                time.Sleep(delay)
                
                // Attempt restart
                newHandle, err := handle.Restart(context.Background())
                if err != nil {
                    logger.Error("Failed to restart component",
                        zap.String("component", componentID),
                        zap.Error(err))
                    retryCount++
                    continue
                }
                
                // Update handle and reset retry count on successful restart
                rm.handles.Store(componentID, newHandle)
                handle = newHandle
                retryCount = 0
                
                logger.Info("Component restarted successfully",
                    zap.String("component", componentID))
            }
        }
    }()
}
```

### Phase 5: Security and Image Verification (Priority 2)

#### 5.1 Container Image Security
```go
// internal/executor/image_security.go
type ImageVerifier struct {
    trustedRegistries []string
    publicKeyPath     string
    requireSignature  bool
}

func (iv *ImageVerifier) VerifyImage(image string) error {
    // 1. Check if image is from trusted registry
    if !iv.isFromTrustedRegistry(image) {
        if iv.requireSignature {
            return fmt.Errorf("image %s not from trusted registry", image)
        }
        logger.Warn("Using image from untrusted registry", zap.String("image", image))
    }
    
    // 2. Verify image signature if required
    if iv.requireSignature {
        if err := iv.verifySignature(image); err != nil {
            return fmt.Errorf("image signature verification failed: %w", err)
        }
    }
    
    // 3. Check for known vulnerabilities (placeholder)
    if err := iv.scanForVulnerabilities(image); err != nil {
        logger.Warn("Vulnerability scan failed", 
            zap.String("image", image),
            zap.Error(err))
    }
    
    return nil
}

func generateComponentToken(componentID string) (string, error) {
    // Generate a JWT token with component ID and expiration
    token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
        "component_id": componentID,
        "issued_at":    time.Now().Unix(),
        "expires_at":   time.Now().Add(24 * time.Hour).Unix(),
        "scope":        "component:register",
    })
    
    // Sign with secret key (should be from config)
    tokenString, err := token.SignedString([]byte(getComponentSecret()))
    if err != nil {
        return "", fmt.Errorf("failed to sign token: %w", err)
    }
    
    return tokenString, nil
}
```

### Phase 6: Developer Experience Improvements (Priority 3)

#### 6.1 Unified Logging and Hot Reload
```go
// internal/executor/dev_features.go
type DeveloperFeatures struct {
    hotReloadEnabled bool
    logAggregator    *LogAggregator
    fileWatcher      *fsnotify.Watcher
}

type LogAggregator struct {
    streams    sync.Map // componentID -> io.ReadCloser
    output     io.Writer
    timestamps bool
    colorize   bool
}

func (la *LogAggregator) AddComponentLogs(componentID string, logs io.ReadCloser) {
    la.streams.Store(componentID, logs)
    
    go func() {
        scanner := bufio.NewScanner(logs)
        for scanner.Scan() {
            line := scanner.Text()
            if la.timestamps {
                line = fmt.Sprintf("[%s] %s: %s", 
                    time.Now().Format("15:04:05"), componentID, line)
            }
            if la.colorize {
                line = la.colorizeLog(componentID, line)
            }
            fmt.Fprintln(la.output, line)
        }
    }()
}

func (df *DeveloperFeatures) EnableHotReload(pipelineFile string, pipeline *DAGPipeline) error {
    if !df.hotReloadEnabled {
        return nil
    }
    
    return df.fileWatcher.Add(pipelineFile)
}
```

### Phase 7: Quick MVP for ttp-processor-demo (Priority 0 - Immediate)

#### 7.1 Simplified Docker Executor with Enhanced Networking
```go
// internal/core/quick_docker_executor.go
func startDockerComponent(component model.Component, controlPlaneEndpoint string) (*QuickHandle, error) {
    // Create bridge network if needed
    networkName := "flowctl-bridge"
    if err := ensureDockerNetwork(networkName); err != nil {
        return nil, err
    }
    
    // Generate component token
    token, err := generateComponentToken(component.ID)
    if err != nil {
        return nil, err
    }
    
    // Build docker run command with proper networking
    args := []string{
        "run", "-d",
        "--name", component.ID,
        "--network", networkName,
        "-e", fmt.Sprintf("CONTROL_PLANE_ENDPOINT=%s", controlPlaneEndpoint),
        "-e", fmt.Sprintf("COMPONENT_TOKEN=%s", token),
        "--restart", "unless-stopped",
    }
    
    // Add environment variables
    for k, v := range component.Env {
        args = append(args, "-e", fmt.Sprintf("%s=%s", k, v))
    }
    
    // Add port bindings
    for _, port := range component.Ports {
        args = append(args, "-p", fmt.Sprintf("%d:%d", port.HostPort, port.ContainerPort))
    }
    
    // Add image and command
    args = append(args, component.Image)
    if len(component.Command) > 0 {
        args = append(args, component.Command...)
    }
    
    cmd := exec.Command("docker", args...)
    output, err := cmd.CombinedOutput()
    if err != nil {
        return nil, fmt.Errorf("failed to start container: %w, output: %s", err, output)
    }
    
    containerID := strings.TrimSpace(string(output))
    
    return &QuickHandle{
        componentID: component.ID,
        containerID: containerID,
        healthCheck: component.HealthCheck,
        healthPort:  component.HealthPort,
    }, nil
}

type QuickHandle struct {
    componentID string
    containerID string
    healthCheck string
    healthPort  int
}

func (h *QuickHandle) WaitReady(ctx context.Context) error {
    // Simple health check polling
    if h.healthCheck == "" {
        // No health check, wait a bit and assume ready
        time.Sleep(3 * time.Second)
        return nil
    }
    
    client := &http.Client{Timeout: 5 * time.Second}
    url := fmt.Sprintf("http://localhost:%d%s", h.healthPort, h.healthCheck)
    
    for i := 0; i < 30; i++ { // 30 attempts = 30 seconds
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
            // Continue polling
        }
    }
    
    return fmt.Errorf("component %s failed to become healthy", h.componentID)
}

func ensureDockerNetwork(networkName string) error {
    // Check if network exists
    cmd := exec.Command("docker", "network", "inspect", networkName)
    if err := cmd.Run(); err == nil {
        return nil // Network already exists
    }
    
    // Create bridge network
    cmd = exec.Command("docker", "network", "create", "--driver", "bridge", networkName)
    return cmd.Run()
}
```

## Testing Plan

1. **Unit Tests**: Test executors with mock Docker/process interfaces
2. **Integration Tests**: Test with simple echo components
3. **E2E Test**: Run actual ttp-processor-demo pipeline

## Rollout Strategy

### Week 1: Minimal MVP
- Implement quick Docker executor
- Update factory functions to start containers
- Test with ttp-processor-demo

### Week 2: Proper Executor Architecture
- Implement executor interface
- Add Docker and process executors
- Add proper lifecycle management

### Week 3: Control Plane Integration
- Enhance control plane for component management
- Implement proper event streaming
- Add health checking

### Week 4: Production Features
- Add Kubernetes executor
- Implement proper logging/monitoring
- Add resource management

## Configuration Examples

### Example 1: Running ttp-processor-demo
```yaml
apiVersion: flowctl/v1
kind: Pipeline
metadata:
  name: ttp-processing
spec:
  executor: docker
  
  sources:
    - id: stellar-live-source
      image: withobsrvr/stellar-live-source:latest
      env:
        NETWORK_PASSPHRASE: "Test SDF Network ; September 2015"
        RPC_ENDPOINT: "https://soroban-testnet.stellar.org"
        
  processors:
    - id: ttp-processor
      image: withobsrvr/ttp-processor:latest
      inputs: ["stellar-live-source"]
      env:
        LOG_LEVEL: info
        
  sinks:
    - id: consumer-app
      image: withobsrvr/consumer-app:latest
      inputs: ["ttp-processor"]
```

### Example 2: Local Binary Execution
```yaml
apiVersion: flowctl/v1
kind: Pipeline
metadata:
  name: local-pipeline
spec:
  executor: process
  
  sources:
    - id: file-source
      binary: ./bin/file-source
      args: ["--watch", "/data/input"]
      health_check: "/health"
      health_port: 9090
      
  processors:
    - id: transform
      binary: ./bin/transform
      inputs: ["file-source"]
      health_check: "/health"
      health_port: 9091
```

### Example 3: Nomad Deployment
```yaml
apiVersion: flowctl/v1
kind: Pipeline
metadata:
  name: nomad-pipeline
spec:
  executor: nomad
  nomad:
    address: "http://nomad.example.com:4646"
    datacenter: "dc1"
    namespace: "default"
  
  sources:
    - id: data-source
      image: data-source:latest
      resources:
        cpu: 200  # MHz
        memory: 256  # MB
```

## Success Criteria

1. `flowctl run ttp-pipeline.yaml` starts all ttp-processor-demo components with proper networking
2. Components register with control plane automatically using authentication tokens
3. Data flows through the pipeline without manual intervention
4. `flowctl status` shows comprehensive component health and lifecycle state
5. `flowctl logs <component>` shows unified component logs with timestamps
6. Components restart automatically on failure according to restart policies
7. Pipeline startup follows proper dependency order (topological sorting)
8. Component cleanup occurs properly on pipeline shutdown

## Risks and Mitigations

| Risk | Mitigation | Status |
|------|------------|--------|
| Docker dependency | Provide process and Nomad executors for flexibility | ✅ Addressed |
| Component startup race conditions | Implement health probes, topological sorting, and proper timeouts | ✅ Addressed |
| Resource consumption | Add resource limits, cleanup on shutdown, and restart policies | ✅ Addressed |
| Network configuration complexity | Use bridge networks with proper port allocation | ✅ Addressed |
| Security vulnerabilities | Implement component tokens, image verification, and trusted registries | ✅ Addressed |
| Concurrency issues | Use sync.Map and proper synchronization primitives | ✅ Addressed |
| Component authentication | Generate JWT tokens for control plane communication | ✅ Addressed |
| Lifecycle management complexity | Implement ComponentHandle interface for async operations | ✅ Addressed |

## Next Steps

1. **Immediate**: Implement enhanced Docker executor with bridge networking and security (Phase 7.1)
2. **This Week**: Test with ttp-processor-demo using proper lifecycle management
3. **Next Week**: Build proper executor architecture with ComponentHandle interface
4. **Following Week**: Enhance control plane integration with richer status tracking
5. **Month 2**: Add Nomad executor and production security features

## Summary of Red-Team Review Improvements

This updated plan addresses all major concerns from the red-team review:

1. **Executor Interface**: Enhanced with ComponentHandle for proper async lifecycle management
2. **Concurrency Safety**: Added sync.Map and proper synchronization throughout
3. **Networking**: Replaced host networking with bridge networks and proper port allocation
4. **Security**: Added component token authentication and image verification
5. **Lifecycle Management**: Implemented comprehensive restart policies and failure handling
6. **Orchestration**: Added topological sorting for proper startup order
7. **Nomad Support**: Prioritized Nomad executor before Kubernetes
8. **Developer Experience**: Added unified logging, hot reload, and better error handling

This plan provides a robust path from the current mock-only implementation to a production-ready component orchestration system while maintaining security, reliability, and developer productivity.