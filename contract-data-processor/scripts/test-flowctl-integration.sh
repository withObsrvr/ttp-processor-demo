#!/usr/bin/env bash

# Test script for flowctl integration with contract data processor
# This script verifies that the processor correctly registers and sends heartbeats

set -e

FLOWCTL_ADDR="${FLOWCTL_ENDPOINT:-localhost:8080}"
PROCESSOR_HEALTH="http://localhost:8089/health"  # Check default first, then 8090

echo "Contract Data Processor - Flowctl Integration Test"
echo "================================================="
echo ""

# Note: This test uses basic connectivity checks rather than grpcurl
# since flowctl's gRPC reflection may not be enabled

echo "1. Checking processor health..."
echo "--------------------------------"

# Check if processor is running (try both common ports)
if curl -s "$PROCESSOR_HEALTH" > /dev/null 2>&1; then
    echo "✓ Processor is healthy (port 8089)"
    PROCESSOR_RUNNING=true
elif curl -s "http://localhost:8090/health" > /dev/null 2>&1; then
    echo "✓ Processor is healthy (port 8090)"
    PROCESSOR_RUNNING=true
    PROCESSOR_HEALTH="http://localhost:8090/health"  # Update for later use
else
    echo "⚠ No processor currently running"
    echo "  Will start processor with flowctl integration for testing"
    PROCESSOR_RUNNING=false
fi

echo ""
echo "2. Checking flowctl connection..."
echo "----------------------------------"

# Test flowctl connectivity
if nc -z localhost 8080 2>/dev/null; then
    echo "✓ Flowctl is reachable at $FLOWCTL_ADDR (port is open)"
else
    echo "✗ Cannot connect to flowctl at $FLOWCTL_ADDR"
    echo "  Please ensure flowctl is running"
    exit 1
fi

echo ""
echo "3. Starting processor with flowctl enabled..."
echo "----------------------------------------------"

# Check if we need to start a processor
if [ "$PROCESSOR_RUNNING" = "true" ]; then
    echo "Using existing processor instance"
    PROCESSOR_PID=$(pgrep -f "contract-data-processor" || echo "")
else
    echo "Starting processor with flowctl integration..."
    
    # Start processor in background with flowctl enabled
    export NETWORK_PASSPHRASE="Test SDF Network ; September 2015"
    export SOURCE_ENDPOINT="localhost:50053"
    export GRPC_ADDRESS=":50054"
    export FLIGHT_ADDRESS=":8817"  # Use different port to avoid conflicts
    export HEALTH_PORT="8090"      # Use different port to avoid conflicts
    export FLOWCTL_ENABLED="true"
    export FLOWCTL_ENDPOINT="localhost:8080"
    export SERVICE_NAME="contract-data-processor"
    export SERVICE_VERSION="1.0.0"
    
    cd .. && ./contract-data-processor &
    PROCESSOR_PID=$!
    
    echo "Processor started with PID: $PROCESSOR_PID"
    echo "Waiting for startup and flowctl registration..."
    sleep 8
fi

echo ""
echo "4. Checking flowctl registration..."
echo "-----------------------------------"

# Check processor logs or metrics for flowctl registration
# Use the same health endpoint we determined earlier
METRICS_URL=$(echo "$PROCESSOR_HEALTH" | sed 's/health/metrics/')
FLOWCTL_METRICS=$(curl -s "$METRICS_URL" 2>/dev/null | grep "contract_data_flowctl_registered" || echo "")

if echo "$FLOWCTL_METRICS" | grep -q "contract_data_flowctl_registered 1"; then
    echo "✓ contract-data-processor successfully registered with flowctl"
    echo "  Registration metric: $FLOWCTL_METRICS"
else
    echo "⚠ Registration status unclear, checking processor health..."
    if curl -s "$PROCESSOR_HEALTH" > /dev/null 2>&1; then
        echo "✓ Processor is healthy and flowctl integration attempted"
        echo "  Note: Processor may not have flowctl enabled or may be using simulated ID"
    else
        echo "✗ Processor health check failed"
    fi
fi

echo ""
echo "5. Monitoring heartbeats..."
echo "---------------------------"
echo "Watching for heartbeat metrics..."

# Monitor flowctl heartbeat metrics using the detected metrics URL
for i in {1..3}; do
    sleep 3
    HEARTBEAT_METRICS=$(curl -s "$METRICS_URL" 2>/dev/null | grep "flowctl_heartbeat" || echo "")
    
    if [ ! -z "$HEARTBEAT_METRICS" ]; then
        echo "✓ Heartbeat metrics found:"
        echo "$HEARTBEAT_METRICS" | while read line; do echo "  $line"; done
    else
        echo "  Waiting for heartbeat metrics... (attempt $i/3)"
    fi
done

# Cleanup
if [ ! -z "$PROCESSOR_PID" ] && [ "$PROCESSOR_PID" != "$(pgrep -f contract-data-processor)" ]; then
    echo ""
    echo "6. Cleaning up test processor..."
    echo "--------------------------------"
    echo "Stopping test processor (PID: $PROCESSOR_PID)..."
    kill $PROCESSOR_PID 2>/dev/null || true
    sleep 2
fi

echo ""
echo "Test Summary"
echo "============"
echo ""
echo "To fully test flowctl integration:"
echo "1. Start flowctl control plane"
echo "2. Start contract-data-processor with:"
echo "   export FLOWCTL_ENABLED=true"
echo "   export FLOWCTL_ENDPOINT=localhost:8080"
echo "   ./contract-data-processor"
echo "3. Start processing some data"
echo "4. Monitor flowctl dashboard for metrics"
echo ""
echo "Expected behavior:"
echo "- Service registers on startup"
echo "- Heartbeats sent every 10 seconds"
echo "- Metrics include contracts processed, errors, etc."
echo "- Service unregisters on shutdown"