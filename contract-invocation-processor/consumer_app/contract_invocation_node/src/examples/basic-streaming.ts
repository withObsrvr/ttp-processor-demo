/**
 * @fileoverview Basic streaming example for Contract Invocation Consumer
 */

import { ContractInvocationClient } from '../client';
import { ContractInvocationEvent } from '../../gen/contract_invocation/contract_invocation_event_pb';
import { InvocationType } from '../types';

async function basicStreamingExample() {
  console.log('🚀 Starting Basic Contract Invocation Streaming Example\n');

  // Initialize client with basic configuration
  const client = new ContractInvocationClient({
    endpoint: 'localhost:50051',
    debug: true,
    timeout: 30000,
    maxRetries: 3,
    retryInterval: 2000
  });

  // Test connection before starting
  console.log('Testing connection...');
  const connected = await client.testConnection();
  if (!connected) {
    console.error('❌ Failed to connect to Contract Invocation Processor');
    process.exit(1);
  }
  console.log('✅ Connection successful\n');

  // Subscribe to all contract invocation events
  const subscription = client.subscribe(
    {
      startLedger: Math.floor(Date.now() / 1000) - 3600, // Start from 1 hour ago
      autoReconnect: true,
      response: {
        includeDiagnosticEvents: true,
        includeStateChanges: true,
        includeSubCalls: true,
        includeTtlExtensions: true
      }
    },
    
    // Event handler
    (event: ContractInvocationEvent) => {
      console.log('📦 Contract Invocation Event Received:');
      console.log(`  ├─ Ledger: ${event.getLedgerSequence()}`);
      console.log(`  ├─ Transaction: ${event.getTransactionHash()}`);
      console.log(`  ├─ Contract: ${event.getContractId()}`);
      console.log(`  ├─ Type: ${getInvocationType(event)}`);
      console.log(`  ├─ Success: ${event.getSuccessful()}`);
      
      // Display contract call details
      const contractCall = event.getContractCall();
      if (contractCall) {
        console.log(`  ├─ Function: ${contractCall.getFunctionName()}`);
        console.log(`  ├─ Invoker: ${contractCall.getInvokingAccount()}`);
        console.log(`  ├─ Arguments: ${contractCall.getArgumentsList().length} args`);
        
        // Show diagnostic events if present
        const diagnosticEvents = contractCall.getDiagnosticEventsList();
        if (diagnosticEvents.length > 0) {
          console.log(`  ├─ Diagnostic Events: ${diagnosticEvents.length}`);
          diagnosticEvents.forEach((diag, idx) => {
            console.log(`  │  └─ Event ${idx + 1}: Contract ${diag.getContractId()}, Topics: ${diag.getTopicsList().length}`);
          });
        }
        
        // Show state changes if present
        const stateChanges = contractCall.getStateChangesList();
        if (stateChanges.length > 0) {
          console.log(`  ├─ State Changes: ${stateChanges.length}`);
        }
        
        // Show sub-calls if present
        const subCalls = contractCall.getContractCallsList();
        if (subCalls.length > 0) {
          console.log(`  ├─ Sub-calls: ${subCalls.length}`);
        }
      }
      
      // Display contract creation details
      const createContract = event.getCreateContract();
      if (createContract) {
        console.log(`  ├─ Created Contract: ${createContract.getNewContractId()}`);
        console.log(`  ├─ WASM Hash: ${createContract.getWasmHash()}`);
        console.log(`  ├─ Creator: ${createContract.getCreatingAccount()}`);
      }
      
      // Display WASM upload details
      const uploadWasm = event.getUploadWasm();
      if (uploadWasm) {
        console.log(`  ├─ WASM Hash: ${uploadWasm.getWasmHash()}`);
        console.log(`  ├─ Size: ${uploadWasm.getWasmSize()} bytes`);
        console.log(`  ├─ Uploader: ${uploadWasm.getUploadingAccount()}`);
      }
      
      console.log(`  └─ Timestamp: ${new Date(event.getTimestamp() * 1000).toISOString()}\n`);
    },
    
    // Error handler
    (error: Error) => {
      console.error(`❌ Stream error: ${error.message}`);
    },
    
    // Status handler
    (status) => {
      if (status.connected) {
        console.log(`✅ Stream connected - Events processed: ${status.eventsProcessed}`);
      } else {
        console.log(`⚠️  Stream disconnected - Reconnect attempts: ${status.reconnectAttempts}`);
      }
    }
  );

  // Setup metrics reporting
  client.on('metrics', (metrics) => {
    console.log(`📊 Metrics - Total events: ${metrics.totalEvents}, Rate: ${metrics.eventsPerSecond.toFixed(2)} events/sec, Uptime: ${Math.floor(metrics.uptime / 1000)}s`);
  });

  // Setup graceful shutdown
  process.on('SIGINT', () => {
    console.log('\n🛑 Shutting down gracefully...');
    
    const finalMetrics = client.getMetrics();
    console.log(`📈 Final metrics:`);
    console.log(`  ├─ Total events processed: ${finalMetrics.totalEvents}`);
    console.log(`  ├─ Total uptime: ${Math.floor(finalMetrics.uptime / 1000)} seconds`);
    console.log(`  └─ Average rate: ${(finalMetrics.totalEvents / (finalMetrics.uptime / 1000)).toFixed(2)} events/sec`);
    
    subscription.unsubscribe();
    client.close();
    process.exit(0);
  });

  console.log('🎯 Listening for contract invocation events... (Press Ctrl+C to stop)\n');
}

function getInvocationType(event: ContractInvocationEvent): string {
  if (event.getContractCall()) return 'CONTRACT_CALL';
  if (event.getCreateContract()) return 'CREATE_CONTRACT';
  if (event.getUploadWasm()) return 'UPLOAD_WASM';
  return 'UNKNOWN';
}

// Run example if this file is executed directly
if (require.main === module) {
  basicStreamingExample().catch((error) => {
    console.error('❌ Example failed:', error);
    process.exit(1);
  });
}