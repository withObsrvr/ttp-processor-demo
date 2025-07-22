/**
 * @fileoverview Basic streaming example for Contract Invocation Consumer (Fixed version)
 */

import { ContractInvocationClient } from '../client';
import { ContractInvocationEvent } from '../../gen/contract_invocation/contract_invocation_event_pb';

async function basicStreamingExample() {
  console.log('🚀 Starting Basic Contract Invocation Streaming Example\n');

  const client = new ContractInvocationClient({
    endpoint: 'localhost:50051',
    debug: true,
    timeout: 30000,
    maxRetries: 3,
    retryInterval: 2000
  });

  console.log('Testing connection...');
  const connected = await client.testConnection();
  if (!connected) {
    console.error('❌ Failed to connect to Contract Invocation Processor');
    process.exit(1);
  }
  console.log('✅ Connection successful\n');

  const subscription = client.subscribe(
    {
      startLedger: Math.floor(Date.now() / 1000) - 3600,
      autoReconnect: true
    },
    
    (event: ContractInvocationEvent) => {
      const meta = event.getMeta();
      if (!meta) return;

      console.log('📦 Contract Invocation Event Received:');
      console.log(`  ├─ Ledger: ${meta.getLedgerSequence()}`);
      console.log(`  ├─ Transaction: ${meta.getTxHash()}`);
      console.log(`  ├─ Success: ${meta.getSuccessful()}`);
      console.log(`  ├─ Type: ${getInvocationType(event)}`);
      
      const contractCall = event.getContractCall();
      if (contractCall) {
        console.log(`  ├─ Function: ${contractCall.getFunctionName()}`);
        console.log(`  ├─ Invoker: ${contractCall.getInvokingAccount()}`);
        console.log(`  ├─ Arguments: ${contractCall.getArgumentsList().length} args`);
        
        const diagnosticEvents = contractCall.getDiagnosticEventsList();
        if (diagnosticEvents.length > 0) {
          console.log(`  ├─ Diagnostic Events: ${diagnosticEvents.length}`);
        }
      }
      
      const createContract = event.getCreateContract();
      if (createContract) {
        console.log(`  ├─ Created Contract: ${createContract.getContractId()}`);
        console.log(`  ├─ WASM Hash: ${createContract.getWasmHash()}`);
        console.log(`  ├─ Creator: ${createContract.getCreatorAccount()}`);
      }
      
      const uploadWasm = event.getUploadWasm();
      if (uploadWasm) {
        console.log(`  ├─ WASM Hash: ${uploadWasm.getWasmHash()}`);
        console.log(`  ├─ Uploader: ${uploadWasm.getUploaderAccount()}`);
      }
      
      const closedAt = meta.getClosedAt();
      if (closedAt) {
        console.log(`  └─ Timestamp: ${new Date(closedAt.getSeconds() * 1000).toISOString()}\n`);
      }
    },
    
    (error: Error) => {
      console.error(`❌ Stream error: ${error.message}`);
    }
  );

  client.on('metrics', (metrics) => {
    console.log(`📊 Metrics - Events: ${metrics.totalEvents}, Rate: ${metrics.eventsPerSecond.toFixed(2)}/sec`);
  });

  process.on('SIGINT', () => {
    console.log('\n🛑 Shutting down...');
    const finalMetrics = client.getMetrics();
    console.log(`📈 Total events: ${finalMetrics.totalEvents}`);
    subscription.unsubscribe();
    client.close();
    process.exit(0);
  });

  console.log('🎯 Listening for events... (Press Ctrl+C to stop)\n');
}

function getInvocationType(event: ContractInvocationEvent): string {
  if (event.getContractCall()) return 'CONTRACT_CALL';
  if (event.getCreateContract()) return 'CREATE_CONTRACT';
  if (event.getUploadWasm()) return 'UPLOAD_WASM';
  return 'UNKNOWN';
}

if (require.main === module) {
  basicStreamingExample().catch(console.error);
}