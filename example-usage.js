const { LedgerJsonRpcClient } = require('./ledger-jsonrpc-client');

async function main() {
  // Create and initialize the client
  const client = new LedgerJsonRpcClient('localhost:50051');
  await client.initialize();

  console.log('Client initialized successfully');

  try {
    // Example 1: Execute a single JSON-RPC method
    console.log('\n--- Example 1: Single JSON-RPC method call ---');
    const singleResponse = await client.executeJsonRpcMethod({
      method: 'ledger',
      params: { ledger_index: 'validated' }
    });
    console.log('Single method response:', JSON.stringify(singleResponse, null, 2));

    // Example 2: Stream JSON-RPC responses for a specific range
    console.log('\n--- Example 2: Stream finite range of ledgers ---');
    console.log('Streaming 5 ledgers starting from sequence 40000000...');
    
    const finiteStream = client.streamJsonRpcResponses(
      {
        startLedger: 40000000,
        endLedger: 40000005,
        method: 'ledger', 
        params: { full: false }
      },
      (event) => {
        console.log(`Received event for ledger ${event.ledgerMeta.sequence}`);
        console.log('Event data:', JSON.stringify(event, null, 2));
      },
      (error) => {
        console.error('Stream error:', error);
      },
      () => {
        console.log('Finite stream completed');
      }
    );

    // Wait for 10 seconds to allow some events to be processed
    await new Promise(resolve => setTimeout(resolve, 10000));
    finiteStream.cancel();

    // Example 3: Stream JSON-RPC responses continuously (live)
    console.log('\n--- Example 3: Stream continuous (live) ledgers ---');
    console.log('Starting continuous stream from latest ledger...');
    
    const liveStream = client.streamJsonRpcResponses(
      {
        startLedger: 0,  // Use the latest ledger
        endLedger: 0,    // Continuous stream
        method: 'account_info',
        params: { 
          account: 'rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh', 
          strict: true 
        }
      },
      (event) => {
        console.log(`Received live event for ledger ${event.ledgerMeta.sequence}`);
        console.log('Account info:', JSON.stringify(event.response.result, null, 2));
      },
      (error) => {
        console.error('Live stream error:', error);
      }
    );

    // Wait for 20 seconds to allow some live events to be processed
    console.log('Waiting for live events (will cancel after 20 seconds)...');
    await new Promise(resolve => setTimeout(resolve, 20000));
    
    // Cancel the live stream
    console.log('Cancelling live stream...');
    liveStream.cancel();

  } catch (error) {
    console.error('Error in examples:', error);
  } finally {
    // Close the client connection
    client.close();
    console.log('Client connection closed');
  }
}

main().catch(console.error);