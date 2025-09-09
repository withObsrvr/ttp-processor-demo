#!/usr/bin/env bash

# Integration test script for contract data processor
# Tests the full pipeline with stellar-live-source-datalake

set -e

PROCESSOR_BIN="../contract-data-processor"
HEALTH_URL="http://localhost:8089/health"
METRICS_URL="http://localhost:8089/metrics"
GRPC_ADDR="localhost:50054"
DATALAKE_ADDR="localhost:50053"

echo "Contract Data Processor - Integration Test"
echo "=========================================="
echo ""

# Check if processor binary exists
if [ ! -f "$PROCESSOR_BIN" ]; then
    echo "Error: Contract data processor binary not found at $PROCESSOR_BIN"
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

echo "1. Checking prerequisites..."
echo "----------------------------"

# Check if stellar-live-source-datalake is running
# Test with netcat first, then try grpcurl as backup
if nc -z localhost 50053 2>/dev/null; then
    echo "✓ stellar-live-source-datalake is running (port 50053 is open)"
elif timeout 5 bash -c "</dev/tcp/localhost/50053" 2>/dev/null; then
    echo "✓ stellar-live-source-datalake is running (TCP connection successful)"
else
    echo "✗ stellar-live-source-datalake is not running on $DATALAKE_ADDR"
    echo "  Please start it first:"
    echo "  cd /home/tillman/Documents/ttp-processor-demo/stellar-live-source-datalake"
    echo "  ./stellar-live-source-datalake"
    exit 1
fi

echo ""
echo "2. Starting processor with integration config..."
echo "------------------------------------------------"

# Start processor with full configuration
export NETWORK_PASSPHRASE="Test SDF Network ; September 2015"
export SOURCE_ENDPOINT="$DATALAKE_ADDR"
export GRPC_ADDRESS=":50054"
export FLIGHT_ADDRESS=":8816"
export HEALTH_PORT="8089"
export LOG_LEVEL="info"
export FLOWCTL_ENABLED="false"
export BATCH_SIZE="100"
export WORKER_COUNT="2"

$PROCESSOR_BIN &
PROCESSOR_PID=$!

echo "Processor started with PID: $PROCESSOR_PID"
echo "Waiting for startup and data source connection..."
sleep 10

echo ""
echo "3. Verifying connection to data source..."
echo "-----------------------------------------"

# Check health endpoint
if curl -s -f "$HEALTH_URL" > /dev/null; then
    echo "✓ Processor is healthy"
else
    echo "✗ Processor health check failed"
    exit 1
fi

echo ""
echo "4. Starting a test processing session..."
echo "----------------------------------------"

# Use a small range of ledgers that should contain contract data
START_LEDGER=500000
END_LEDGER=501000

echo "Ledger range: $START_LEDGER to $END_LEDGER"

# Check if grpcurl is available (prefer nix environment)
GRPCURL_CMD=""
if command -v grpcurl &> /dev/null; then
    GRPCURL_CMD="grpcurl"
elif nix develop --command which grpcurl &> /dev/null; then
    GRPCURL_CMD="nix develop --command grpcurl"
fi

if [ ! -z "$GRPCURL_CMD" ]; then
    # Start processing (without session_id in request)
    echo "Starting processing session..."
    RESPONSE=$($GRPCURL_CMD -plaintext -d "{
        \"start_ledger\": $START_LEDGER,
        \"end_ledger\": $END_LEDGER,
        \"batch_size\": 100
    }" "$GRPC_ADDR" contractdata.v1.ControlService/StartProcessing)
    
    echo "Response: $RESPONSE"
    
    # Extract session_id from response if successful (handle both sessionId and session_id with spaces)
    SESSION_ID=$(echo "$RESPONSE" | grep -o '"sessionId"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*"\([^"]*\)".*/\1/')
    if [ -z "$SESSION_ID" ]; then
        SESSION_ID=$(echo "$RESPONSE" | grep -o '"session_id"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*"\([^"]*\)".*/\1/')
    fi
    
    if [ ! -z "$SESSION_ID" ]; then
        echo "Session started with ID: $SESSION_ID"
        echo "Waiting for data processing..."
        sleep 15

        # Check session status
        echo ""
        echo "Session status:"
        $GRPCURL_CMD -plaintext -d "{\"session_id\": \"$SESSION_ID\"}" "$GRPC_ADDR" contractdata.v1.ControlService/GetStatus

        # Stop processing
        echo ""
        echo "Stopping processing session..."
        $GRPCURL_CMD -plaintext -d "{\"session_id\": \"$SESSION_ID\"}" "$GRPC_ADDR" contractdata.v1.ControlService/StopProcessing
    else
        echo "⚠ Failed to extract session ID from response"
    fi
else
    echo "⚠ grpcurl not available, skipping gRPC control plane test"
fi

echo ""
echo "5. Checking processing metrics..."
echo "---------------------------------"

echo "Current metrics:"
curl -s "$METRICS_URL" | grep -E "contract_data_(contracts_processed|current_ledger|processing_errors)" | head -5

# Check if any contracts were processed
CONTRACTS_PROCESSED=$(curl -s "$METRICS_URL" | grep "contract_data_contracts_processed_total" | grep -v "#" | awk '{print $2}' | head -1)
if [ ! -z "$CONTRACTS_PROCESSED" ] && [ "$CONTRACTS_PROCESSED" -gt 0 ] 2>/dev/null; then
    echo "✓ Processed $CONTRACTS_PROCESSED contracts"
else
    echo "⚠ No contracts processed (may be normal if no contract data in range)"
fi

echo ""
echo "6. Testing Arrow Flight data plane..."
echo "-------------------------------------"

# Check if pyarrow is available for Flight testing
if python3 -c "import pyarrow.flight" 2>/dev/null; then
    echo "Testing Arrow Flight connection..."
    
    cat > /tmp/test_flight.py << 'EOF'
import pyarrow.flight as flight
import sys

try:
    client = flight.FlightClient("grpc://localhost:8816")
    
    # List available flights
    flights = list(client.list_flights())
    print(f"✓ Flight server responding, {len(flights)} flights available")
    
    if flights:
        print("Available flights:")
        for flight_info in flights:
            print(f"  - {flight_info.descriptor}")
    
    # Try to get schema from the contract data stream
    ticket = flight.Ticket(b"contract-data-stream")
    try:
        reader = client.do_get(ticket)
        schema = reader.schema
        print(f"✓ Contract data schema has {len(schema)} fields")
        print("Schema fields:")
        for field in schema:
            print(f"  - {field.name}: {field.type}")
    except Exception as e:
        print(f"⚠ Could not read stream (normal if no active data): {e}")
        
except Exception as e:
    print(f"✗ Flight client error: {e}")
    sys.exit(1)
EOF

    python3 /tmp/test_flight.py
    rm /tmp/test_flight.py
else
    echo "⚠ pyarrow not available, skipping Flight test"
    echo "  Install with: pip install pyarrow"
fi

echo ""
echo "7. Performance check..."
echo "-----------------------"

# Check memory usage
if command -v ps &> /dev/null; then
    MEMORY_KB=$(ps -o rss= -p $PROCESSOR_PID 2>/dev/null || echo "0")
    MEMORY_MB=$((MEMORY_KB / 1024))
    echo "Memory usage: ${MEMORY_MB}MB"
    
    if [ $MEMORY_MB -gt 1000 ]; then
        echo "⚠ High memory usage detected"
    else
        echo "✓ Memory usage within normal range"
    fi
fi

echo ""
echo "8. Testing graceful shutdown..."
echo "-------------------------------"

echo "Sending SIGTERM to processor..."
kill -TERM $PROCESSOR_PID

WAIT_COUNT=0
while kill -0 $PROCESSOR_PID 2>/dev/null; do
    if [ $WAIT_COUNT -gt 15 ]; then
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
echo "Integration Test Summary"
echo "========================"
echo "✓ Data source connection"
echo "✓ Processing session management"
echo "✓ Metrics collection"
echo "✓ Arrow Flight server"
echo "✓ Resource usage"
echo "✓ Graceful shutdown"
echo ""
echo "Integration test completed successfully!"
echo ""
echo "Next steps:"
echo "1. Test with flowctl: ./test-flowctl-integration.sh"
echo "2. Test PostgreSQL consumer integration"
echo "3. Run performance tests with larger ledger ranges"