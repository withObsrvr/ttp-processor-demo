# Contract Invocation Node.js Consumer

A TypeScript/Node.js client library for consuming real-time Soroban contract invocation events from the Contract Invocation Processor. This library provides a high-level, type-safe interface for streaming and filtering contract invocation events with comprehensive monitoring and error handling.

## Features

- **Real-time Streaming**: Stream contract invocation events in real-time from Stellar ledgers
- **Comprehensive Filtering**: Filter by contract IDs, function names, accounts, success status, and content patterns  
- **Type Safety**: Full TypeScript support with generated protobuf types
- **Automatic Reconnection**: Built-in connection resilience with exponential backoff
- **Advanced Monitoring**: Built-in metrics collection and status reporting
- **Multiple Event Types**: Support for contract calls, contract creation, and WASM uploads
- **Flowctl Integration**: Optional integration with flowctl control plane
- **Protocol 23 Support**: Full support for Stellar Protocol 23 features including archive restoration

## Installation

```bash
npm install @withobsrvr/contract-invocation-consumer
```

## Quick Start

### Basic Streaming Example

```typescript
import { ContractInvocationClient } from '@withobsrvr/contract-invocation-consumer';

const client = new ContractInvocationClient({
  endpoint: 'localhost:50051',
  debug: true
});

// Test connection
const connected = await client.testConnection();
if (!connected) {
  console.error('Failed to connect to processor');
  process.exit(1);
}

// Subscribe to all events
const subscription = client.subscribe(
  {
    startLedger: Math.floor(Date.now() / 1000) - 3600, // Last hour
    response: {
      includeDiagnosticEvents: true,
      includeStateChanges: true
    }
  },
  (event) => {
    console.log(`Contract: ${event.getContractId()}`);
    console.log(`Ledger: ${event.getLedgerSequence()}`);
    console.log(`Success: ${event.getSuccessful()}\n`);
  },
  (error) => {
    console.error('Stream error:', error.message);
  }
);

// Cleanup
process.on('SIGINT', () => {
  subscription.unsubscribe();
  client.close();
  process.exit(0);
});
```

### Advanced Filtering Example

```typescript
import { ContractInvocationClient, InvocationType } from '@withobsrvr/contract-invocation-consumer';

const client = new ContractInvocationClient({
  endpoint: process.env.PROCESSOR_ENDPOINT || 'localhost:50051',
  metrics: { enabled: true, reportingInterval: 10000 }
});

// Filter by specific criteria
const subscription = client.subscribe(
  {
    startLedger: 1000000,
    filter: {
      contractIds: ['CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQASOBNCUEXD'],
      functionNames: ['transfer', 'mint', 'approve'],
      successful: true,
      invocationTypes: [InvocationType.CONTRACT_CALL],
      contentFilter: {
        argumentCount: { min: 2, max: 5 },
        requireStateChanges: true,
        requiredDiagnosticEventTopics: ['Transfer']
      }
    },
    response: {
      includeDiagnosticEvents: true,
      includeStateChanges: true,
      includeSubCalls: true
    }
  },
  (event) => {
    const contractCall = event.getContractCall();
    if (contractCall) {
      console.log(`Function: ${contractCall.getFunctionName()}`);
      console.log(`Arguments: ${contractCall.getArgumentsList().length}`);
      console.log(`Diagnostic Events: ${contractCall.getDiagnosticEventsList().length}`);
      console.log(`State Changes: ${contractCall.getStateChangesList().length}`);
    }
  }
);
```

## Configuration

### Client Configuration

```typescript
interface ContractInvocationClientConfig {
  /** gRPC server endpoint */
  endpoint: string;
  
  /** TLS configuration */
  tls?: {
    enabled: boolean;
    cert?: Buffer;
    key?: Buffer;
    ca?: Buffer;
  };
  
  /** Connection timeout in milliseconds (default: 30000) */
  timeout?: number;
  
  /** Maximum retry attempts (default: 5) */
  maxRetries?: number;
  
  /** Retry backoff interval in milliseconds (default: 2000) */
  retryInterval?: number;
  
  /** Enable debug logging (default: false) */
  debug?: boolean;
  
  /** Metrics configuration */
  metrics?: {
    enabled: boolean;
    reportingInterval?: number;
    metricsCallback?: (metrics: ConsumerMetrics) => void;
  };
  
  /** Flowctl integration */
  flowctl?: {
    enabled: boolean;
    endpoint?: string;
    heartbeatInterval?: number;
    network?: string;
    healthPort?: number;
  };
}
```

### Subscription Options

```typescript
interface SubscriptionOptions {
  /** Starting ledger sequence number */
  startLedger?: number;
  
  /** Ending ledger sequence number (optional, 0 = live stream) */
  endLedger?: number;
  
  /** Filtering options */
  filter?: FilterOptions;
  
  /** Response customization */
  response?: ResponseOptions;
  
  /** Enable auto-reconnection (default: true) */
  autoReconnect?: boolean;
}
```

## Filtering Options

### Basic Filters

```typescript
interface FilterOptions {
  /** Filter by specific contract IDs */
  contractIds?: string[];
  
  /** Filter by invoking account addresses */
  invokingAccounts?: string[];
  
  /** Filter by function names */
  functionNames?: string[];
  
  /** Filter by success/failure */
  successful?: boolean;
  
  /** Filter by invocation types */
  invocationTypes?: InvocationType[];
  
  /** Advanced content filtering */
  contentFilter?: ContentFilterOptions;
}

enum InvocationType {
  CONTRACT_CALL = 'CONTRACT_CALL',
  CREATE_CONTRACT = 'CREATE_CONTRACT',
  UPLOAD_WASM = 'UPLOAD_WASM'
}
```

### Content Filtering

```typescript
interface ContentFilterOptions {
  /** Required argument count range */
  argumentCount?: {
    min?: number;
    max?: number;
  };
  
  /** Argument pattern matching with wildcards */
  argumentPatterns?: string[];
  
  /** Required diagnostic event topics */
  requiredDiagnosticEventTopics?: string[];
  
  /** Require state changes */
  requireStateChanges?: boolean;
  
  /** Require sub-calls */
  requireSubCalls?: boolean;
  
  /** Require TTL extensions */
  requireTtlExtensions?: boolean;
}
```

### Pattern Matching

Content filters support wildcard pattern matching:

- `*transfer*` - Contains 'transfer'
- `approve*` - Starts with 'approve'  
- `*_burn` - Ends with '_burn'
- `exact_match` - Exact string match

## Response Customization

```typescript
interface ResponseOptions {
  /** Include diagnostic events */
  includeDiagnosticEvents?: boolean;
  
  /** Include state changes */
  includeStateChanges?: boolean;
  
  /** Include contract-to-contract calls */
  includeSubCalls?: boolean;
  
  /** Include TTL extensions */
  includeTtlExtensions?: boolean;
  
  /** Include archive restoration events */
  includeArchiveRestorations?: boolean;
}
```

## Event Handling

### Event Types

The client receives `ContractInvocationEvent` objects with the following structure:

```typescript
interface ContractInvocationEvent {
  // Basic event information
  ledgerSequence: number;
  transactionHash: string;
  contractId: string;
  successful: boolean;
  timestamp: number;
  
  // Event type (one of):
  contractCall?: ContractCall;
  createContract?: CreateContract;
  uploadWasm?: UploadWasm;
}
```

### Contract Call Events

```typescript
interface ContractCall {
  functionName: string;
  invokingAccount: string;
  arguments: ScValue[];
  returnValue?: ScValue;
  
  // Optional enhanced data
  diagnosticEvents: DiagnosticEvent[];
  stateChanges: StateChange[];
  contractCalls: ContractToContractCall[];
  ttlExtensions: TtlExtension[];
}
```

### Event Handlers

Support both synchronous and asynchronous event handlers:

```typescript
// Synchronous handler
(event: ContractInvocationEvent) => {
  console.log('Sync handler:', event.getContractId());
}

// Asynchronous handler
async (event: ContractInvocationEvent) => {
  await processEvent(event);
  console.log('Async handler completed');
}
```

## Monitoring and Metrics

### Built-in Metrics

```typescript
interface ConsumerMetrics {
  totalEvents: number;
  eventsPerSecond: number;
  streamStatus: StreamStatus;
  uptime: number;
  lastError?: {
    message: string;
    timestamp: Date;
  };
}

interface StreamStatus {
  connected: boolean;
  lastEventTime?: Date;
  eventsProcessed: number;
  errors: number;
  reconnectAttempts: number;
}
```

### Monitoring Example

```typescript
// Enable metrics collection
const client = new ContractInvocationClient({
  endpoint: 'localhost:50051',
  metrics: {
    enabled: true,
    reportingInterval: 5000,
    metricsCallback: (metrics) => {
      console.log(`Rate: ${metrics.eventsPerSecond.toFixed(2)} events/sec`);
      console.log(`Total: ${metrics.totalEvents} events`);
      console.log(`Uptime: ${Math.floor(metrics.uptime / 1000)}s`);
    }
  }
});

// Manual metrics access
const currentMetrics = client.getMetrics();
const streamStatus = client.getStatus();
```

## Error Handling

### Automatic Reconnection

The client automatically handles connection failures with exponential backoff:

```typescript
const subscription = client.subscribe(
  { 
    startLedger: 1000,
    autoReconnect: true  // Default: true
  },
  eventHandler,
  (error) => {
    console.error('Stream error:', error.message);
    // Client will automatically attempt to reconnect
  },
  (status) => {
    if (status.connected) {
      console.log('Stream connected');
    } else {
      console.log(`Reconnect attempts: ${status.reconnectAttempts}`);
    }
  }
);
```

### Connection Testing

```typescript
// Test connection before subscribing
const connected = await client.testConnection();
if (!connected) {
  throw new Error('Cannot connect to Contract Invocation Processor');
}
```

## Environment Variables

The examples support these environment variables:

```bash
# Processor endpoint
export PROCESSOR_ENDPOINT=localhost:50051

# Flowctl integration
export ENABLE_FLOWCTL=true
export FLOWCTL_ENDPOINT=localhost:8080
export STELLAR_NETWORK=testnet
```

## Examples

The package includes several complete examples:

### 1. Basic Streaming (`src/examples/basic-streaming.ts`)
- Simple event streaming
- Basic error handling
- Metrics reporting
- Graceful shutdown

### 2. Advanced Filtering (`src/examples/filtered-events.ts`)
- Multiple filter types
- Content-based filtering
- Subscription management
- Pattern matching

### 3. Real-time Monitor (`src/examples/real-time-monitor.ts`)
- Dashboard-style output
- Live statistics
- Performance monitoring
- Event categorization

### Running Examples

```bash
# Install dependencies
npm install

# Run basic streaming example
npm run dev

# Or run specific examples
npm run build
node dist/examples/basic-streaming.js
node dist/examples/filtered-events.js
node dist/examples/real-time-monitor.js
```

## Development

### Building

```bash
npm run build
```

### Testing

```bash
# Run tests
npm test

# Run tests with coverage
npm test -- --coverage

# Watch mode
npm run test:watch
```

### Code Generation

```bash
# Regenerate protobuf definitions
npm run gen-proto
```

## Integration with Flowctl

When `flowctl` integration is enabled, the client reports metrics and status to the flowctl control plane:

```typescript
const client = new ContractInvocationClient({
  endpoint: 'localhost:50051',
  flowctl: {
    enabled: true,
    endpoint: 'localhost:8080',
    heartbeatInterval: 10000,
    network: 'testnet',
    healthPort: 8088
  }
});
```

## Protocol 23 Support

The client fully supports Stellar Protocol 23 features:

- **Archive Restoration**: Automatic detection of contract data restored from archive
- **Data Source Tracking**: Distinguish between live and archive data sources
- **Enhanced State Changes**: Track contract storage modifications across hot/archive buckets
- **TTL Management**: Monitor contract data TTL extensions and expirations

## Troubleshooting

### Common Issues

1. **Connection Refused**
   - Ensure the Contract Invocation Processor is running
   - Check the endpoint configuration
   - Verify network connectivity

2. **No Events Received**
   - Check ledger range (startLedger/endLedger)
   - Verify filtering criteria aren't too restrictive
   - Ensure contracts are active in the specified ledger range

3. **High Memory Usage**
   - The client uses streaming architecture to prevent memory accumulation
   - Check for event handler memory leaks
   - Monitor metrics for processing rates

4. **Frequent Reconnections**
   - Check network stability
   - Increase timeout values
   - Monitor processor logs for issues

### Debug Mode

Enable debug logging for troubleshooting:

```typescript
const client = new ContractInvocationClient({
  endpoint: 'localhost:50051',
  debug: true
});
```

## API Reference

### Class: ContractInvocationClient

#### Methods

- `constructor(config: ContractInvocationClientConfig)`
- `subscribe(options: SubscriptionOptions, eventHandler, errorHandler?, statusHandler?): EventSubscription`
- `unsubscribe(): void`
- `getMetrics(): ConsumerMetrics`
- `getStatus(): StreamStatus`  
- `testConnection(): Promise<boolean>`
- `close(): void`

#### Events

- `connected` - Stream successfully connected
- `disconnected` - Stream disconnected
- `error` - Stream or processing error
- `event` - New contract invocation event received
- `metrics` - Periodic metrics update (if enabled)

## License

Apache-2.0 License - see LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

---

For more information about the Contract Invocation Processor and the broader ttp-processor-demo ecosystem, see the [main project documentation](../../README.md).