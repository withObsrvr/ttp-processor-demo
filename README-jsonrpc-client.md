# Ledger JSON-RPC Client for Node.js

A minimal Node.js client for interacting with the ledger-jsonrpc-processor service.

## Installation

This client requires the following dependencies:

```bash
npm install @grpc/grpc-js @grpc/proto-loader google-protobuf
```

## Features

- Connect to a ledger-jsonrpc-processor gRPC service
- Execute single JSON-RPC method calls
- Stream JSON-RPC responses for a range of ledgers or continuously
- Properly handles protobuf message conversion to JavaScript objects

## Usage

### Initialization

```javascript
const { LedgerJsonRpcClient } = require('./ledger-jsonrpc-client');

// Create and initialize the client
const client = new LedgerJsonRpcClient('localhost:50051');
await client.initialize();
```

### Single JSON-RPC Method Call

```javascript
// Execute a single JSON-RPC method
const response = await client.executeJsonRpcMethod({
  method: 'ledger',
  params: { ledger_index: 'validated' },
  id: 'my-request-1'  // Optional - will generate random ID if not provided
});

console.log('Response:', response);
```

### Streaming JSON-RPC Responses (Finite Range)

```javascript
// Stream JSON-RPC responses for a specific range of ledgers
const finiteStream = client.streamJsonRpcResponses(
  {
    startLedger: 40000000,
    endLedger: 40000005,  // Specific end ledger for a finite range
    method: 'ledger', 
    params: { full: false }
  },
  (event) => {
    console.log(`Received event for ledger ${event.ledgerMeta.sequence}`);
    console.log('Response data:', event.response.result);
  },
  (error) => {
    console.error('Stream error:', error);
  },
  () => {
    console.log('Stream completed');
  }
);

// Cancel the stream if needed
finiteStream.cancel();
```

### Streaming JSON-RPC Responses (Continuous/Live)

```javascript
// Stream JSON-RPC responses continuously (live)
const liveStream = client.streamJsonRpcResponses(
  {
    startLedger: 40000000,
    endLedger: 0,  // Zero indicates continuous streaming
    method: 'account_info',
    params: { 
      account: 'rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh' 
    }
  },
  (event) => {
    console.log(`Received live event for ledger ${event.ledgerMeta.sequence}`);
    console.log('Account info:', event.response.result);
  },
  (error) => {
    console.error('Live stream error:', error);
  }
);

// Cancel the live stream when done
liveStream.cancel();
```

### Cleanup

```javascript
// Close the client connection when done
client.close();
```

## Response Format

The client normalizes the protobuf responses into plain JavaScript objects:

### Single Method Response

```javascript
{
  jsonrpc: "2.0",
  id: "request-123",
  result: {
    // Result fields
  }
  // OR if there was an error
  error: {
    code: -32000,
    message: "Error message",
    data: { /* Additional error data */ }
  }
}
```

### Streaming Event Response

```javascript
{
  requestContext: {
    id: "request-123",
    method: "ledger",
    params: { /* Method parameters */ }
  },
  response: {
    jsonrpc: "2.0",
    id: "request-123",
    result: { /* Result fields */ }
    // OR error object
  },
  ledgerMeta: {
    sequence: 40000000,
    closedAt: "2023-04-15T12:34:56Z", // Date object
    hash: "ABCDEF..."
  },
  processedAt: "2023-04-15T12:34:58Z" // Date object
}
```

## Error Handling

The client provides error callbacks for streaming and Promise rejection for single calls. Common errors include:
- Connection failures
- Invalid parameters
- Server-side processing errors

## Example

See `example-usage.js` for a complete working example.