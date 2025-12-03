# Troubleshooting Guide

**Last Updated**: December 3, 2025
**Version**: Cycle 3 - Enhanced Observability

## Quick Diagnosis

```bash
# Check pipeline status
flowctl status

# Check component logs
ls -lh logs/

# View recent logs
tail -f logs/*.log

# Validate pipeline configuration
flowctl validate pipeline.yaml
```

## Common Issues

### Issue 1: Pipeline Won't Start

**Symptoms**:
- `flowctl run` fails immediately
- Error: "pipeline file not found"
- Error: "failed to load pipeline"

**Diagnosis**:
```bash
# 1. Check file exists
ls -la pipeline.yaml

# 2. Validate YAML syntax
flowctl validate pipeline.yaml

# 3. Check for configuration errors
cat pipeline.yaml
```

**Resolution**:

**If file doesn't exist**:
```bash
# Use correct path
flowctl run /path/to/pipeline.yaml
```

**If validation fails**:
```bash
# Fix validation errors reported by flowctl validate
# Common fixes:
# - Assign unique ports
# - Fix duplicate component IDs
# - Add required metadata
```

**If YAML syntax error**:
```bash
# Check YAML formatting
# - Ensure correct indentation (2 spaces)
# - No tabs
# - Proper list syntax (- items)
```

---

### Issue 2: Component Not Registering

**Symptoms**:
- Pipeline starts but hangs
- Error: "timeout waiting for component X to register"
- `flowctl status` shows component missing

**Diagnosis**:
```bash
# 1. Check component process is running
ps aux | grep <component-name>

# 2. Check component logs
cat logs/<component-id>.log

# 3. Check control plane is reachable
curl http://127.0.0.1:8080/health

# 4. Check status
flowctl status
```

**Resolution**:

**If component crashed**:
```bash
# Check error in logs
tail -100 logs/<component-id>.log

# Common causes:
# - Binary doesn't exist → Build: make build
# - Port already in use → Change port in pipeline.yaml
# - Missing dependencies → Check environment
```

**If component not built**:
```bash
# Build all binaries
cd ttp-processor-sdk && make build
cd stellar-live-source-sdk/go && make build
cd consumer_app/node && npm install && npm run build
```

**If port conflict**:
```bash
# Find process using port
lsof -i :50051

# Kill conflicting process
kill <PID>

# Or change port in pipeline.yaml
```

**If control plane unreachable**:
```bash
# Check control plane is running
ps aux | grep flowctl

# Restart pipeline
pkill flowctl
flowctl run pipeline.yaml
```

---

### Issue 3: Consumer Not Receiving Events

**Symptoms**:
- Consumer shows "waiting for events"
- Consumer metrics show 0 events received
- Other consumers in fan-out are working

**Diagnosis**:
```bash
# 1. Check consumer registration
flowctl status | grep <consumer-id>

# 2. Check event type matching
flowctl status

# 3. Check routing statistics in logs
grep "routing statistics" logs/flowctl.log
```

**Resolution**:

**If not registered**:
```bash
# Check consumer started
ps aux | grep <consumer-name>

# Check consumer logs
tail logs/<consumer-id>.log

# Restart consumer component
```

**If event type mismatch**:

Event types must match between processor output and consumer input.

```yaml
# Processor must output types that consumer accepts
processors:
  - id: processor
    # Component reports: output_event_types: ["stellar.token.transfer.v1"]

sinks:
  - id: consumer
    # Component reports: input_event_types: ["stellar.token.transfer.v1"]  # Match!
```

**Fix**: Update consumer to accept correct event types (configured in consumer code, not YAML)

**If routing filtered**:
```bash
# Check processor output types
flowctl status | grep -A5 "processor-id"

# Check consumer input types
flowctl status | grep -A5 "consumer-id"

# Ensure overlap between output and input types
```

---

### Issue 4: High Latency / Slow Processing

**Symptoms**:
- Events taking too long to process
- Backlog building up
- Consumer lag increasing

**Diagnosis**:
```bash
# 1. Check component status
flowctl status

# 2. Check processor chain length
flowctl validate pipeline.yaml

# 3. Check resource usage
top -p $(pgrep -d',' processor)

# 4. Check metrics in status
flowctl status | grep -A10 "PROCESSORS"
```

**Resolution**:

**If long processor chain**:
```bash
# Validation warning:
# "Long processor chain detected (6 processors)"
# "Expected latency: ~120ms"

# Solution: Split into multiple pipelines
# Pipeline 1: Initial processing (3 processors)
# Pipeline 2: Final processing (3 processors)
```

**If resource constrained**:
```bash
# Check CPU/memory
top

# Increase resources or scale horizontally
# (Future: Auto-scaling support)
```

**If batching disabled**:
```yaml
# Enable batching to improve throughput
processors:
  - id: batch-processor
    env:
      ENABLE_BATCHING: "true"
      BATCH_SIZE: "10"
```

---

### Issue 5: Component Keeps Crashing

**Symptoms**:
- Component restarts repeatedly
- Error logs show crashes
- Pipeline becomes unhealthy

**Diagnosis**:
```bash
# 1. Check crash logs
tail -200 logs/<component-id>.log

# 2. Check for OOM (Out of Memory)
dmesg | grep -i "killed process"

# 3. Check for resource limits
ulimit -a

# 4. Check for port conflicts
lsof -i :<port>
```

**Resolution**:

**If Out of Memory**:
```bash
# Check memory usage
free -h

# Increase available memory
# Or reduce batch sizes:
# BATCH_SIZE: "5"  # instead of 10
```

**If Binary Corrupted**:
```bash
# Rebuild binary
cd <component-dir>
make clean
make build

# Verify binary
./<binary> --version
```

**If Environment Issues**:
```bash
# Check required environment variables
env | grep COMPONENT_ID
env | grep PORT
env | grep HEALTH_PORT

# Ensure all required vars set in pipeline.yaml
```

---

### Issue 6: Pipeline Validation Fails

**Symptoms**:
- `flowctl validate` reports errors
- Can't start pipeline

**Diagnosis**:
```bash
# Run validation
flowctl validate pipeline.yaml

# Check for specific errors:
# - Port conflicts
# - Duplicate IDs
# - Missing commands
```

**Resolution**:

**Port Conflict**:
```yaml
# BAD
processors:
  - id: proc1
    env:
      PORT: ":50051"
  - id: proc2
    env:
      PORT: ":50051"  # Conflict!

# GOOD
processors:
  - id: proc1
    env:
      PORT: ":50051"
  - id: proc2
    env:
      PORT: ":50052"  # Unique
```

**Duplicate ID**:
```yaml
# BAD
processors:
  - id: processor-1
  - id: processor-1  # Duplicate!

# GOOD
processors:
  - id: processor-1
  - id: processor-2  # Unique
```

**Missing Command**:
```yaml
# BAD
processors:
  - id: processor
    command: ["/path/to/missing/binary"]

# GOOD - Build first
# make build
processors:
  - id: processor
    command: ["/home/user/project/bin/processor"]
```

---

### Issue 7: Control Plane Not Responding

**Symptoms**:
- `flowctl status` hangs or fails
- Components can't register
- Error: "failed to connect to control plane"

**Diagnosis**:
```bash
# 1. Check control plane process
ps aux | grep flowctl

# 2. Check control plane port
lsof -i :8080

# 3. Try connecting
curl http://127.0.0.1:8080/health
```

**Resolution**:

**If control plane not running**:
```bash
# Start pipeline (starts embedded control plane)
flowctl run pipeline.yaml
```

**If port in use**:
```bash
# Check what's using port 8080
lsof -i :8080

# Kill conflicting process
kill <PID>

# Or use different port
flowctl run --control-plane-port 9090 pipeline.yaml
```

**If network issue**:
```bash
# Check localhost connectivity
ping 127.0.0.1

# Check firewall rules
sudo iptables -L

# Ensure 127.0.0.1:8080 is accessible
```

---

### Issue 8: Events Being Filtered Unexpectedly

**Symptoms**:
- Fewer events than expected
- Consumer not receiving all events
- Metrics show high filter rate

**Diagnosis**:
```bash
# 1. Check processor configuration
cat pipeline.yaml | grep -A10 "processors"

# 2. Check filter settings
grep "FILTER_" pipeline.yaml

# 3. Check metrics
flowctl status | grep -A10 "PROCESSORS"
```

**Resolution**:

**If event type filtering too aggressive**:
```yaml
# Check FILTER_EVENT_TYPES
processors:
  - id: filter
    env:
      FILTER_EVENT_TYPES: "transfer"  # Only transfer events pass

# Solution: Broaden filter or remove if needed
      FILTER_EVENT_TYPES: "transfer,mint,burn"
```

**If amount filtering too strict**:
```yaml
# Check MIN_AMOUNT
processors:
  - id: filter
    env:
      MIN_AMOUNT: "10000000"  # 10 XLM minimum

# Solution: Lower threshold
      MIN_AMOUNT: "1000000"  # 1 XLM minimum
```

**If contract filtering**:
```yaml
# Check FILTER_CONTRACT_IDS
processors:
  - id: filter
    env:
      FILTER_CONTRACT_IDS: "CABC..."

# Solution: Add more contract IDs or remove filter
      FILTER_CONTRACT_IDS: "CABC...,CDEF...,CGHI..."
```

---

## Health Check Procedures

### Quick Health Check

```bash
# 1. Check all components
flowctl status

# 2. All should be HEALTHY (green)
# 3. Last heartbeat should be recent (<30s ago)
# 4. No components missing
```

### Detailed Health Check

```bash
# 1. Validate configuration
flowctl validate pipeline.yaml

# 2. Check component status
flowctl status --include-unhealthy

# 3. Check logs for errors
grep -i "error" logs/*.log | tail -50

# 4. Check resource usage
top -b -n 1 | grep -E "processor|consumer|source"

# 5. Check connectivity
curl http://127.0.0.1:8080/health  # Control plane
curl http://127.0.0.1:8089/health  # Processor
curl http://127.0.0.1:8090/health  # Source
curl http://127.0.0.1:9089/health  # Consumer
```

---

## Escalation

### When to Escalate

Escalate if:
- Pipeline completely down for >15 minutes
- Data loss occurring
- Multiple components failing
- Unknown error not covered in this guide
- Security incident suspected

### Escalation Procedure

1. **Gather Information**:
```bash
# Create incident report
cat > incident-$(date +%Y%m%d-%H%M%S).txt <<EOF
Time: $(date)
Pipeline: <pipeline-name>
Issue: <brief description>

Status:
$(flowctl status)

Validation:
$(flowctl validate pipeline.yaml)

Recent Errors:
$(grep -i "error" logs/*.log | tail -100)

Resource Usage:
$(top -b -n 1 | head -20)
EOF
```

2. **Contact**: [Your escalation contact/team]

3. **Provide**:
- Incident report file
- Pipeline YAML configuration
- Recent log files (logs/*.log)

---

## Useful Commands

```bash
# Pipeline Management
flowctl run pipeline.yaml          # Start pipeline
flowctl validate pipeline.yaml     # Validate before running
flowctl status                      # Check component status
flowctl status --watch              # Watch status (refreshes)

# Component Logs
tail -f logs/*.log                  # Follow all logs
tail -f logs/<component-id>.log     # Follow specific component
grep "ERROR" logs/*.log             # Find errors

# Process Management
ps aux | grep flowctl               # Find flowctl processes
pkill flowctl                       # Stop all flowctl processes
pkill <component-name>              # Stop specific component

# Port Debugging
lsof -i :8080                       # Check control plane port
lsof -i :50051                      # Check processor port
netstat -tulpn | grep LISTEN        # List all listening ports

# Resource Monitoring
top -p $(pgrep -d',' processor)     # Monitor processor CPU/mem
free -h                             # Check available memory
df -h                               # Check disk space
```

---

## Log Locations

```
logs/
├── flowctl.log              # Control plane logs
├── <component-id>.log       # Component-specific logs
└── <component-id>.err       # Component error logs
```

**Important Logs**:
- `flowctl.log`: Registration, heartbeats, routing statistics
- `<source-id>.log`: Ledger fetching, event generation
- `<processor-id>.log`: Event processing, filtering, batching
- `<consumer-id>.log`: Event consumption, delivery status

---

## Prevention

### Pre-Deployment Checklist

- [ ] Run `flowctl validate pipeline.yaml`
- [ ] Build all binaries (`make build`)
- [ ] Test with small ledger range first
- [ ] Check ports are available
- [ ] Review resource requirements
- [ ] Test failure scenarios
- [ ] Document any custom configuration

### Monitoring Best Practices

- Monitor `flowctl status` regularly
- Set up alerts for component failures
- Track event processing rates
- Monitor resource usage trends
- Review logs daily for warnings

---

## See Also

- [Pipeline Validation Guide](../PIPELINE_VALIDATION_GUIDE.md)
- [Deployment Guide](DEPLOYMENT.md)
- [Monitoring Guide](MONITORING.md)
- [Cycle 3 Shape Up](../CYCLE3_SHAPE.md)
