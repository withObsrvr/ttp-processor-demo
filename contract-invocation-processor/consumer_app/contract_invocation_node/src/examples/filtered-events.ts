/**
 * @fileoverview Advanced filtering example for Contract Invocation Consumer
 */

import { ContractInvocationClient } from '../client';
import { ContractInvocationEvent } from '../../gen/contract_invocation/contract_invocation_event_pb';
import { InvocationType } from '../types';

async function filteredEventsExample() {
  console.log('🎯 Starting Advanced Filtering Example\n');

  // Initialize client
  const client = new ContractInvocationClient({
    endpoint: process.env.PROCESSOR_ENDPOINT || 'localhost:50051',
    debug: true,
    metrics: {
      enabled: true,
      reportingInterval: 15000
    }
  });

  // Test connection
  console.log('Testing connection...');
  const connected = await client.testConnection();
  if (!connected) {
    console.error('❌ Failed to connect to Contract Invocation Processor');
    process.exit(1);
  }
  console.log('✅ Connection successful\n');

  // Example 1: Filter by specific contract IDs
  console.log('📋 Example 1: Filtering by specific contract IDs');
  const contractSubscription = client.subscribe(
    {
      startLedger: Math.floor(Date.now() / 1000) - 1800, // Start from 30 minutes ago
      filter: {
        contractIds: [
          'CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQASOBNCUEXD',
          'CBQHNAXSI55GX2GN6D67GK7BHVPSLJUGZQEU7WJ5LKR5PNUCGLIMAO4K'
        ],
        successful: true // Only successful invocations
      },
      response: {
        includeDiagnosticEvents: true,
        includeStateChanges: true
      }
    },
    (event: ContractInvocationEvent) => {
      console.log(`🎯 Contract-filtered event: ${event.getContractId()} in ledger ${event.getLedgerSequence()}`);
      
      const contractCall = event.getContractCall();
      if (contractCall) {
        console.log(`   Function: ${contractCall.getFunctionName()}`);
        console.log(`   Diagnostic Events: ${contractCall.getDiagnosticEventsList().length}`);
        console.log(`   State Changes: ${contractCall.getStateChangesList().length}\n`);
      }
    },
    (error) => console.error('Contract filter error:', error.message)
  );

  // Example 2: Filter by function names and invoking accounts
  console.log('📋 Example 2: Filtering by function names and accounts');
  const functionSubscription = client.subscribe(
    {
      startLedger: Math.floor(Date.now() / 1000) - 1800,
      filter: {
        functionNames: ['transfer', 'mint', 'approve', 'burn'],
        invokingAccounts: [
          'GAHK7EEG2WWHVKDNT4CEQFZGKF2LGDSW2IVM4S5DP42RBW3K6BTODB4A',
          'GA2HGBJIJKI6O4XEM7CZWY5PS6GKSXL6D34ERAJYQSPYA6X6AI7HYW36'
        ]
      },
      response: {
        includeDiagnosticEvents: true,
        includeSubCalls: true
      }
    },
    (event: ContractInvocationEvent) => {
      const contractCall = event.getContractCall();
      if (contractCall) {
        console.log(`⚡ Function-filtered event: ${contractCall.getFunctionName()}`);
        console.log(`   Contract: ${event.getContractId()}`);
        console.log(`   Invoker: ${contractCall.getInvokingAccount()}`);
        console.log(`   Sub-calls: ${contractCall.getContractCallsList().length}\n`);
      }
    }
  );

  // Example 3: Content-based filtering with advanced patterns
  console.log('📋 Example 3: Advanced content filtering');
  const contentSubscription = client.subscribe(
    {
      startLedger: Math.floor(Date.now() / 1000) - 1800,
      filter: {
        invocationTypes: [InvocationType.CONTRACT_CALL],
        contentFilter: {
          argumentCount: {
            min: 2,
            max: 5
          },
          argumentPatterns: [
            '*transfer*',  // Contains 'transfer'
            'approve*',    // Starts with 'approve'
            '*_burn'       // Ends with '_burn'
          ],
          requiredDiagnosticEventTopics: ['Transfer', 'Approval'],
          requireStateChanges: true,
          requireSubCalls: false
        }
      },
      response: {
        includeDiagnosticEvents: true,
        includeStateChanges: true,
        includeSubCalls: true
      }
    },
    (event: ContractInvocationEvent) => {
      const contractCall = event.getContractCall();
      if (contractCall) {
        console.log(`🔍 Content-filtered event: ${contractCall.getFunctionName()}`);
        console.log(`   Contract: ${event.getContractId()}`);
        console.log(`   Arguments: ${contractCall.getArgumentsList().length}`);
        
        // Show matching diagnostic events
        const diagnosticEvents = contractCall.getDiagnosticEventsList();
        const matchingTopics = diagnosticEvents
          .flatMap(diag => diag.getTopicsList())
          .filter(topic => ['Transfer', 'Approval'].includes(topic.getScString()));
        
        console.log(`   Matching diagnostic topics: ${matchingTopics.length}`);
        console.log(`   State changes: ${contractCall.getStateChangesList().length}\n`);
      }
    }
  );

  // Example 4: Type-specific filtering (contract creation only)
  console.log('📋 Example 4: Contract creation events only');
  const creationSubscription = client.subscribe(
    {
      startLedger: Math.floor(Date.now() / 1000) - 3600, // Last hour
      filter: {
        invocationTypes: [InvocationType.CREATE_CONTRACT],
        successful: true
      }
    },
    (event: ContractInvocationEvent) => {
      const createContract = event.getCreateContract();
      if (createContract) {
        console.log(`🏗️  New contract created: ${createContract.getNewContractId()}`);
        console.log(`   WASM Hash: ${createContract.getWasmHash()}`);
        console.log(`   Creator: ${createContract.getCreatingAccount()}`);
        console.log(`   Ledger: ${event.getLedgerSequence()}\n`);
      }
    }
  );

  // Setup metrics reporting for all subscriptions
  client.on('metrics', (metrics) => {
    console.log(`\n📊 Combined Metrics:`);
    console.log(`   ├─ Total events: ${metrics.totalEvents}`);
    console.log(`   ├─ Rate: ${metrics.eventsPerSecond.toFixed(2)} events/sec`);
    console.log(`   ├─ Uptime: ${Math.floor(metrics.uptime / 1000)}s`);
    console.log(`   └─ Errors: ${metrics.streamStatus.errors}\n`);
  });

  // Demonstrate subscription management
  setTimeout(() => {
    console.log('🔄 Unsubscribing from contract ID filter after 30 seconds...');
    contractSubscription.unsubscribe();
  }, 30000);

  setTimeout(() => {
    console.log('🔄 Unsubscribing from function filter after 60 seconds...');
    functionSubscription.unsubscribe();
  }, 60000);

  // Setup graceful shutdown
  process.on('SIGINT', () => {
    console.log('\n🛑 Shutting down all filtered streams...');
    
    console.log('📊 Final subscription metrics:');
    console.log(`   Contract filter: ${contractSubscription.getMetrics().totalEvents} events`);
    console.log(`   Function filter: ${functionSubscription.getMetrics().totalEvents} events`);
    console.log(`   Content filter: ${contentSubscription.getMetrics().totalEvents} events`);
    console.log(`   Creation filter: ${creationSubscription.getMetrics().totalEvents} events`);
    
    contractSubscription.unsubscribe();
    functionSubscription.unsubscribe();
    contentSubscription.unsubscribe();
    creationSubscription.unsubscribe();
    client.close();
    process.exit(0);
  });

  console.log('🎯 Multiple filtered streams active... (Press Ctrl+C to stop)\n');
}

// Run example if this file is executed directly
if (require.main === module) {
  filteredEventsExample().catch((error) => {
    console.error('❌ Filtered events example failed:', error);
    process.exit(1);
  });
}