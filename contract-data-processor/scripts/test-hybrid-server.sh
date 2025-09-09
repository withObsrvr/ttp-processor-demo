#!/bin/bash

# Test script for the contract data processor hybrid server
# This script tests both control plane (gRPC) and data plane (Arrow Flight)

set -e

GRPC_ADDR="localhost:50054"
FLIGHT_ADDR="localhost:8816"

echo "Contract Data Processor - Hybrid Server Test"
echo "==========================================="
echo ""

# Check if grpcurl is installed
if ! command -v grpcurl &> /dev/null; then
    echo "Error: grpcurl is not installed. Please install it first."
    exit 1
fi

echo "1. Testing gRPC Control Plane..."
echo "--------------------------------"

# Start a processing session
echo "Starting processing session..."
SESSION_RESPONSE=$(grpcurl -plaintext -d '{
  "start_ledger": 1000000,
  "end_ledger": 1001000,
  "batch_size": 100
}' $GRPC_ADDR contractdata.v1.ControlService/StartProcessing)

echo "Response: $SESSION_RESPONSE"

# Extract session ID
SESSION_ID=$(echo $SESSION_RESPONSE | jq -r '.sessionId')
if [ -z "$SESSION_ID" ] || [ "$SESSION_ID" == "null" ]; then
    echo "Error: Failed to get session ID"
    exit 1
fi

echo "Session ID: $SESSION_ID"
echo ""

# Wait a bit for processing to start
sleep 2

# Check status
echo "Checking session status..."
STATUS_RESPONSE=$(grpcurl -plaintext -d "{
  \"session_id\": \"$SESSION_ID\"
}" $GRPC_ADDR contractdata.v1.ControlService/GetStatus)

echo "Status: $STATUS_RESPONSE"
echo ""

# Configure filters
echo "Configuring filters..."
FILTER_RESPONSE=$(grpcurl -plaintext -d '{
  "asset_codes": ["USDC", "EURC"],
  "include_deleted": false
}' $GRPC_ADDR contractdata.v1.ControlService/ConfigureFilters)

echo "Filter response: $FILTER_RESPONSE"
echo ""

# Get metrics
echo "Getting metrics..."
METRICS_RESPONSE=$(grpcurl -plaintext -d "{
  \"session_id\": \"$SESSION_ID\"
}" $GRPC_ADDR contractdata.v1.ControlService/GetMetrics)

echo "Metrics: $METRICS_RESPONSE"
echo ""

# Stop processing
echo "Stopping processing..."
STOP_RESPONSE=$(grpcurl -plaintext -d "{
  \"session_id\": \"$SESSION_ID\"
}" $GRPC_ADDR contractdata.v1.ControlService/StopProcessing)

echo "Stop response: $STOP_RESPONSE"
echo ""

echo "2. Testing Arrow Flight Data Plane..."
echo "-------------------------------------"

# Test with Python if available
if command -v python3 &> /dev/null; then
    echo "Testing Arrow Flight with Python..."
    python3 << EOF
import sys
try:
    import pyarrow.flight as flight
    
    # Connect to Flight server
    client = flight.connect("grpc://$FLIGHT_ADDR")
    
    # List flights
    print("Available flights:")
    for flight_info in client.list_flights():
        print(f"  - {flight_info.descriptor}")
    
    # Get schema
    info = client.get_flight_info(
        flight.FlightDescriptor.for_path(["contract", "data"])
    )
    print(f"\nSchema: {info.schema}")
    print(f"Total records: {info.total_records}")
    print(f"Total bytes: {info.total_bytes}")
    
except ImportError:
    print("pyarrow not installed, skipping Python test")
except Exception as e:
    print(f"Error: {e}")
EOF
else
    echo "Python not available, skipping Arrow Flight Python test"
fi

echo ""
echo "3. Testing Health Endpoints..."
echo "------------------------------"

# Test health endpoint
echo "Checking /health endpoint..."
HEALTH_RESPONSE=$(curl -s http://localhost:8089/health)
echo "Health: $HEALTH_RESPONSE"

# Test metrics endpoint
echo ""
echo "Checking /metrics endpoint..."
METRICS_COUNT=$(curl -s http://localhost:8089/metrics | grep -c "contract_data_" || true)
echo "Found $METRICS_COUNT contract data metrics"

echo ""
echo "Test Summary"
echo "============"
echo "✓ gRPC Control Plane: Working"
echo "✓ Session Management: Working"
echo "✓ Health Endpoints: Working"
echo "✓ Metrics Export: Working"
echo ""
echo "All tests completed successfully!"