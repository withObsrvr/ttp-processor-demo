# Ledger JSON-RPC Client for Node.js

This client library provides a convenient way to interact with the Obsrvr Ledger JSON-RPC processor service from Node.js applications. It handles the gRPC communication details and provides a simple, Promise-based API.

## Installation

```bash
npm install @obsrvr/ledger-jsonrpc-client
```

You'll also need to install the required dependencies:

```bash
npm install @grpc/grpc-js @grpc/proto-loader
```

## Quick Start

```javascript
const LedgerJsonRpcClient = require('@obsrvr/ledger-jsonrpc-client');

async function main() {
  // Create a client instance
  const client = new LedgerJsonRpcClient('localhost:50053');
  
  try {
    // Initialize the client
    await client.initialize();
    
    // Example: Make a single method call
    const networkInfo = await client.getJsonRpcMethod({
      method: 'getNetwork'
    });
    
    console.log('Network:', networkInfo.result);
    
    // Example: Stream ledger data
    const stream = client.getJsonRpcResponses({
      startLedger: 40000000,
      endLedger: 40000010,
      method: 'getLedgers',
      params: { limit: '10' }
    }, 
    (event) => {
      console.log(`Received ledger ${event.ledgerMeta.sequence}`);
    },
    (error) => {
      console.error('Stream error:', error);
    },
    () => {
      console.log('Stream ended');
    });
    
    // Later, when you're done with the stream
    stream.cancel();
    
  } finally {
    // Always close the client when done
    client.close();
  }
}

main().catch(console.error);
```

## API Reference

### Constructor

```javascript
const client = new LedgerJsonRpcClient(serverAddress, options);
```

Parameters:
- `serverAddress` (string): The address of the Ledger JSON-RPC server (e.g., 'localhost:50053')
- `options` (object): Optional configuration
  - `protoDir` (string): Directory containing the proto files (default: '../protos')
  - `secure` (boolean): Use secure connection (default: false)

### Methods

#### initialize()

Initializes the client by loading proto files and creating a gRPC client.

```javascript
await client.initialize();
```

#### getJsonRpcMethod(options)

Executes a single JSON-RPC method.

```javascript
const response = await client.getJsonRpcMethod({
  id: 'request-123',      // Optional: Request ID
  method: 'getNetwork',   // Required: Method name
  params: { ... }         // Optional: Method parameters
});
```

Returns a Promise that resolves to the JSON-RPC response object.

#### getJsonRpcResponses(options, onData, onError, onEnd)

Gets responses for a specific JSON-RPC method for a range of ledgers.

```javascript
const stream = client.getJsonRpcResponses({
  startLedger: 40000000,  // Required: First ledger sequence
  endLedger: 40000010,    // Optional: Last ledger (0 for live streaming)
  method: 'getLedgers',   // Required: JSON-RPC method
  params: { limit: '10' } // Optional: Method parameters
},
// Data handler - called for each ledger response
(event) => {
  console.log(`Received ledger ${event.ledgerMeta.sequence}`);
},
// Error handler
(error) => {
  console.error('Stream error:', error);
},
// End handler - called when the stream ends
() => {
  console.log('Stream ended');
});
```

Returns a stream object that can be used to cancel the stream:

```javascript
stream.cancel(); // Cancel the stream when done
```

#### close()

Closes the client and releases resources.

```javascript
client.close();
```

## Response Objects

### JSON-RPC Event

The data handler for `getJsonRpcResponses` receives event objects with this structure:

```javascript
{
  requestContext: {
    id: 'request-123',
    method: 'getLedgers',
    params: { ... }
  },
  
  response: {
    jsonrpc: '2.0',
    id: 'request-123',
    result: { ... } // or error: { code, message, data }
  },
  
  ledgerMeta: {
    sequence: 40000001,
    closedAt: Date,    // JavaScript Date object
    hash: 'abc123...'
  },
  
  processedAt: Date    // JavaScript Date object
}
```

### JSON-RPC Response

The `getJsonRpcMethod` method returns objects with this structure:

```javascript
{
  jsonrpc: '2.0',
  id: 'request-123',
  result: { ... } // For success
  // OR
  error: {
    code: -32601,
    message: 'Method not found',
    data: { ... } // Optional
  }
}
```

## Error Handling

The client throws errors in these cases:
- When `initialize()` is not called before using other methods
- When there are issues with the gRPC connection
- When there are protocol or server errors

For streaming, errors are passed to the `onError` callback.

## Available Methods

The Ledger JSON-RPC service provides these methods:

### getNetwork

Returns information about the network.

```javascript
const response = await client.getJsonRpcMethod({
  method: 'getNetwork'
});
```

### getHealth

Returns the health status of the service.

```javascript
const response = await client.getJsonRpcMethod({
  method: 'getHealth'
});
```

### getLedgers

Returns ledger data. When used with `getJsonRpcResponses`, this streams ledger data.

```javascript
const stream = client.getJsonRpcResponses({
  startLedger: 40000000,
  endLedger: 40000010, // 0 for live streaming
  method: 'getLedgers',
  params: {
    limit: '10',
    cursor: 'optional-cursor'
  }
}, onData, onError, onEnd);
```

### getTransactions

Returns transaction data. When used with `getJsonRpcResponses`, this streams transaction data.

```javascript
const stream = client.getJsonRpcResponses({
  startLedger: 40000000,
  endLedger: 0, // Live streaming
  method: 'getTransactions',
  params: {
    limit: '20',
    order: 'asc'
  }
}, onData, onError, onEnd);
```

## Best Practices

1. Always initialize the client before using it
2. Always close the client when you're done with it
3. Handle stream errors properly
4. Cancel streams when you're done with them
5. For long-running applications, consider implementing reconnection logic