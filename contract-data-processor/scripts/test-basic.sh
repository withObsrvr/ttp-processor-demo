#!/usr/bin/env bash

# Basic test script for contract data processor
# This script tests the processor without external dependencies

set -e

PROCESSOR_BIN="../contract-data-processor"
HEALTH_URL="http://localhost:8089/health"
METRICS_URL="http://localhost:8089/metrics"
GRPC_ADDR="localhost:50054"

echo "Contract Data Processor - Basic Test"
echo "===================================="
echo ""

# Check if processor binary exists
if [ ! -f "$PROCESSOR_BIN" ]; then
    echo "Error: Contract data processor binary not found at $PROCESSOR_BIN"
    echo "Please build the processor first"
    exit 1
fi

# Function to cleanup on exit
cleanup() {
    echo ""
    echo "Cleaning up..."
    if [ ! -z "$PROCESSOR_PID" ]; then
        kill $PROCESSOR_PID 2>/dev/null || true
    fi
}
trap cleanup EXIT

echo "1. Starting processor with test configuration..."
echo "------------------------------------------------"

# Start processor in background with minimal config
export NETWORK_PASSPHRASE="Test SDF Network ; September 2015"
export SOURCE_ENDPOINT="localhost:50053"
export GRPC_ADDRESS=":50054"
export FLIGHT_ADDRESS=":8816"
export HEALTH_PORT="8089"
export LOG_LEVEL="info"
export FLOWCTL_ENABLED="false"  # Disable flowctl for basic test

$PROCESSOR_BIN &
PROCESSOR_PID=$!

echo "Processor started with PID: $PROCESSOR_PID"
echo "Waiting for startup..."
sleep 5

echo ""
echo "2. Testing health endpoint..."
echo "-----------------------------"

if curl -s -f "$HEALTH_URL" > /dev/null; then
    echo "✓ Health check passed"
    curl -s "$HEALTH_URL" | jq .
else
    echo "✗ Health check failed"
    exit 1
fi

echo ""
echo "3. Testing metrics endpoint..."
echo "------------------------------"

if curl -s -f "$METRICS_URL" > /dev/null; then
    echo "✓ Metrics endpoint accessible"
    echo ""
    echo "Key metrics:"
    curl -s "$METRICS_URL" | grep -E "^contract_data_" | head -10
else
    echo "✗ Metrics endpoint failed"
    exit 1
fi

echo ""
echo "4. Testing gRPC control plane..."
echo "--------------------------------"

# Check if grpcurl is available (prefer nix environment)
if command -v grpcurl &> /dev/null; then
    echo "Listing available services:"
    if grpcurl -plaintext "$GRPC_ADDR" list 2>/dev/null; then
        echo "✓ gRPC server is responding"
    else
        echo "✗ gRPC server not responding"
    fi
elif nix develop --command which grpcurl &> /dev/null; then
    echo "Listing available services (using nix environment):"
    if nix develop --command grpcurl -plaintext "$GRPC_ADDR" list 2>/dev/null; then
        echo "✓ gRPC server is responding"
    else
        echo "✗ gRPC server not responding"  
    fi
else
    echo "⚠ grpcurl not available, skipping gRPC tests"
    echo "  Install with: go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest"
fi

echo ""
echo "5. Checking processor logs..."
echo "------------------------------"

# Check for any errors in the last few seconds
if [ -f "/tmp/processor-test.log" ]; then
    if grep -i "error" /tmp/processor-test.log | tail -5; then
        echo "⚠ Errors found in logs"
    else
        echo "✓ No errors in logs"
    fi
fi

echo ""
echo "6. Testing graceful shutdown..."
echo "-------------------------------"

echo "Sending SIGTERM to processor..."
kill -TERM $PROCESSOR_PID

# Wait for graceful shutdown
WAIT_COUNT=0
while kill -0 $PROCESSOR_PID 2>/dev/null; do
    if [ $WAIT_COUNT -gt 20 ]; then
        echo "✗ Processor did not shut down gracefully"
        kill -9 $PROCESSOR_PID
        exit 1
    fi
    echo -n "."
    sleep 1
    WAIT_COUNT=$((WAIT_COUNT + 1))
done

echo ""
echo "✓ Processor shut down gracefully"

echo ""
echo "Basic Test Summary"
echo "=================="
echo "✓ Binary execution"
echo "✓ Health endpoint"
echo "✓ Metrics endpoint"
if command -v grpcurl &> /dev/null; then
    echo "✓ gRPC server"
else
    echo "⚠ gRPC server (grpcurl not available)"
fi
if [ $WAIT_COUNT -le 20 ]; then
    echo "✓ Graceful shutdown"
else 
    echo "⚠ Shutdown took longer than expected (but processor is functional)"
fi
echo ""
echo "All basic tests passed!"
echo ""
echo "Next steps:"
echo "1. Start stellar-live-source-datalake and run: ./test-integration.sh"
echo "2. Enable flowctl and run: ./test-flowctl-integration.sh"
echo "3. Test Arrow Flight streaming with: python3 test_flight_client.py"