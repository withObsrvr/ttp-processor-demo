# Account ID Filtering Integration Test

This document provides a test plan for verifying the account ID filtering functionality across the TTP processor pipeline.

## Test Scenarios

### 1. Source-Level Ledger Range Filtering

**Test**: Verify stellar-live-source services support end ledger parameter
- **stellar-live-source**: Start with `START_LEDGER=100` and `END_LEDGER=110` 
- **stellar-live-source-datalake**: Start with bounded range parameters
- **Expected**: Services should stop streaming after reaching end ledger

**Commands**:
```bash
# Test RPC source with range
cd stellar-live-source
RPC_ENDPOINT=https://soroban-testnet.stellar.org NETWORK_PASSPHRASE="Test SDF Network ; September 2015" ./stellar_live_source_server

# Test datalake source with range
cd stellar-live-source-datalake
STORAGE_TYPE="FS" BUCKET_NAME="./test-data" ./stellar_live_source_datalake
```

### 2. Processor-Level Account Filtering

**Test**: Verify ttp-processor filters events by account ID
- **Input**: TTP events with different account addresses
- **Filter**: Specific account IDs (e.g., "GACCOUNT1", "GACCOUNT2")
- **Expected**: Only events matching filter criteria are passed through

**Commands**:
```bash
cd ttp-processor
LIVE_SOURCE_ENDPOINT=localhost:50051 ./ttp_processor_server
```

### 3. Consumer Applications

**Test**: Verify all consumers can send account filter criteria

#### Node.js Consumer
```bash
cd consumer_app/node
npm run build
npm start -- 254838 254839 GACCOUNT1 GACCOUNT2
```

#### Go WASM Consumer (Native)
```bash
cd consumer_app/go_wasm
make build-native
./dist/bin/consumer_wasm localhost:50054 254838 254839 GACCOUNT1 GACCOUNT2
```

#### Go WASM Consumer (WebAssembly)
```bash
cd consumer_app/go_wasm
make serve-wasm
# In browser console:
# getTTPEvents("localhost:50054", "254838", "254839", "GACCOUNT1", "GACCOUNT2")
```

#### Rust WASM Consumer
```bash
cd consumer_app/rust_wasm
wasm-pack build --target web
# Load in browser and call:
# client.get_ttp_events(254838, 254839, ["GACCOUNT1", "GACCOUNT2"])
```

## Test Data

### Sample Account IDs for Testing
- `GACCOUNT1EXAMPLE234567890ABCDEFGHIJK` - Mock account 1
- `GACCOUNT2EXAMPLE234567890ABCDEFGHIJK` - Mock account 2  
- `GCONTRACTEXAMPLE234567890ABCDEFGHIJK` - Mock contract address

### Expected Filtering Behavior

1. **Transfer Events**: Match if `from` or `to` account matches filter
2. **Mint Events**: Match if `to` account matches filter
3. **Burn Events**: Match if `from` account matches filter
4. **Clawback Events**: Match if `from` account matches filter
5. **Fee Events**: Match if `from` account matches filter
6. **All Events**: Match if `contract_address` matches filter

## Verification Steps

1. **Protocol Updates**: Confirm all `.proto` files include `account_ids` field
2. **Code Generation**: Verify generated protobuf code includes new fields
3. **Service Implementation**: Test each service component individually
4. **End-to-End**: Run complete pipeline with test data
5. **Performance**: Measure filtering impact on throughput

## Success Criteria

- ✅ All proto definitions updated with account filtering
- ✅ All services regenerated with new protobuf code
- ✅ Source services support ledger range filtering
- ✅ Processor implements account-based filtering logic
- ✅ All consumer applications support filter parameters
- ✅ End-to-end filtering works correctly
- ✅ No significant performance degradation

## Implementation Status

- [x] Proto definitions updated
- [x] Protobuf code regenerated  
- [x] Source-level range filtering implemented
- [x] Processor-level account filtering implemented
- [x] Node.js consumer updated
- [x] Go WASM consumer updated
- [x] Rust WASM consumer updated (placeholder)
- [x] Integration test documented