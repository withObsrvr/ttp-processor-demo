#!/bin/bash

# Test script for flowctl integration with contract data processor
# This script verifies that the processor correctly registers and sends heartbeats

set -e

FLOWCTL_ADDR="${FLOWCTL_ENDPOINT:-localhost:8080}"
PROCESSOR_HEALTH="http://localhost:8089/health"

echo "Contract Data Processor - Flowctl Integration Test"
echo "================================================="
echo ""

# Check if grpcurl is installed
if ! command -v grpcurl &> /dev/null; then
    echo "Error: grpcurl is not installed. Please install it first."
    exit 1
fi

echo "1. Checking processor health..."
echo "--------------------------------"

# Check if processor is running
if curl -s "$PROCESSOR_HEALTH" > /dev/null; then
    echo "✓ Processor is healthy"
else
    echo "✗ Processor is not running or not healthy"
    echo "  Please start the contract-data-processor with FLOWCTL_ENABLED=true"
    exit 1
fi

echo ""
echo "2. Checking flowctl connection..."
echo "----------------------------------"

# Test flowctl connectivity
if grpcurl -plaintext "$FLOWCTL_ADDR" list > /dev/null 2>&1; then
    echo "✓ Flowctl is reachable at $FLOWCTL_ADDR"
else
    echo "✗ Cannot connect to flowctl at $FLOWCTL_ADDR"
    echo "  Please ensure flowctl is running"
    exit 1
fi

echo ""
echo "3. Listing registered services..."
echo "---------------------------------"

# List all services registered with flowctl
echo "Fetching registered services from flowctl..."
SERVICES=$(grpcurl -plaintext "$FLOWCTL_ADDR" flowctl.ControlPlane/ListServices 2>/dev/null || echo "")

if [ -z "$SERVICES" ]; then
    echo "No services found or ListServices not available"
else
    echo "$SERVICES" | jq -r '.services[] | "- \(.serviceName) (\(.serviceType)) - \(.status)"' 2>/dev/null || echo "$SERVICES"
fi

echo ""
echo "4. Checking contract-data-processor registration..."
echo "---------------------------------------------------"

# Look for our service
if echo "$SERVICES" | grep -q "contract-data-processor"; then
    echo "✓ contract-data-processor is registered with flowctl"
    
    # Extract service details
    SERVICE_INFO=$(echo "$SERVICES" | jq -r '.services[] | select(.serviceName == "contract-data-processor")')
    
    echo ""
    echo "Service Details:"
    echo "  - Service ID: $(echo "$SERVICE_INFO" | jq -r .serviceId)"
    echo "  - Service Type: $(echo "$SERVICE_INFO" | jq -r .serviceType)"
    echo "  - Status: $(echo "$SERVICE_INFO" | jq -r .status)"
    echo "  - Input Events: $(echo "$SERVICE_INFO" | jq -r '.inputEventTypes | join(", ")')"
    echo "  - Output Events: $(echo "$SERVICE_INFO" | jq -r '.outputEventTypes | join(", ")')"
else
    echo "✗ contract-data-processor is NOT registered with flowctl"
    echo "  Ensure the processor is running with FLOWCTL_ENABLED=true"
fi

echo ""
echo "5. Monitoring heartbeats..."
echo "---------------------------"
echo "Watching for heartbeats (press Ctrl+C to stop)..."

# Monitor metrics endpoint to see if values are changing
PREV_METRICS=""
for i in {1..5}; do
    sleep 2
    CURR_METRICS=$(curl -s http://localhost:8089/metrics | grep "contract_data_" | head -5)
    
    if [ "$CURR_METRICS" != "$PREV_METRICS" ]; then
        echo "✓ Metrics updated (heartbeat likely sent)"
    else
        echo "  Waiting for metrics update..."
    fi
    
    PREV_METRICS="$CURR_METRICS"
done

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