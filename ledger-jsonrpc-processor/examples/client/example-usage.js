/**
 * Example usage of the Ledger JSON-RPC Client
 */

const LedgerJsonRpcClient = require('./ledger-jsonrpc-client');

// Create and initialize the client
async function runExample() {
  // Create a client instance
  const client = new LedgerJsonRpcClient('localhost:50053', {
    protoDir: '../../protos', // Adjust path to your proto files
    secure: false
  });

  try {
    // Initialize the client
    await client.initialize();
    console.log('Client initialized successfully');

    // Example 1: Call a single JSON-RPC method
    console.log('\n=== Example 1: Call a single JSON-RPC method ===');
    try {
      const response = await client.getJsonRpcMethod({
        id: 'request-123',
        method: 'getNetwork',
        params: {} // No parameters needed for this method
      });
      
      console.log('Network info:', response.result);
    } catch (error) {
      console.error('Error calling getNetwork:', error.message);
    }

    // Example 2: Fetch responses for a specific ledger range
    console.log('\n=== Example 2: Fetch responses for a specific ledger range ===');
    const startLedger = 40000000;
    const endLedger = 40000010; // Finite range
    
    console.log(`Getting ledger data from ${startLedger} to ${endLedger}...`);
    
    // Stream ledger data
    const stream = client.getJsonRpcResponses(
      {
        startLedger: startLedger,
        endLedger: endLedger,
        method: 'getLedgers',
        params: {
          limit: '10' // Parameters must be strings when sent over gRPC
        }
      },
      // Data handler
      (event) => {
        console.log(`Received ledger ${event.ledgerMeta.sequence} data:`, 
          event.response.result.ledgers.length, 'ledgers');
      },
      // Error handler
      (error) => {
        console.error('Stream error:', error.message);
      },
      // End handler
      () => {
        console.log('Stream ended');
      }
    );

    // Example 3: Set up a live streaming connection
    console.log('\n=== Example 3: Set up a live streaming connection ===');
    const liveStartLedger = 40000000;
    
    console.log(`Starting live stream from ledger ${liveStartLedger}...`);
    
    // Pass 0 as endLedger for live streaming
    const liveStream = client.getJsonRpcResponses(
      {
        startLedger: liveStartLedger,
        endLedger: 0, // 0 means live streaming
        method: 'getTransactions',
        params: {
          limit: '20',
          order: 'asc'
        }
      },
      // Data handler
      (event) => {
        console.log(`Received transactions for ledger ${event.ledgerMeta.sequence}`);
        // Process transactions from the response
        const txCount = event.response.result.transactions.length;
        console.log(`  Processing ${txCount} transactions`);
        
        // In a real application, you would process these transactions
        // For example, store them in your database, trigger notifications, etc.
      },
      // Error handler
      (error) => {
        console.error('Live stream error:', error.message);
      },
      // End handler
      () => {
        console.log('Live stream ended (this should only happen if the server closes the connection)');
      }
    );

    // Example usage: Wait for a few seconds and then cancel the streams
    console.log('\nWaiting for 10 seconds to receive some data...');
    await new Promise(resolve => setTimeout(resolve, 10000));
    
    console.log('\nCancelling streams...');
    stream.cancel();
    liveStream.cancel();
    
    console.log('Streams cancelled');

  } catch (error) {
    console.error('Error in example:', error);
  } finally {
    // Always close the client when done
    client.close();
    console.log('\nClient closed');
  }
}

// Run the example
runExample().catch(console.error);