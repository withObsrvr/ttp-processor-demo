/**
 * @fileoverview Real-time monitoring example with dashboard-style output
 */

import { ContractInvocationClient } from '../client';
import { ContractInvocationEvent } from '../../gen/contract_invocation/contract_invocation_event_pb';
import { InvocationType } from '../types';

interface MonitoringStats {
  totalEvents: number;
  contractCalls: number;
  contractCreations: number;
  wasmUploads: number;
  successfulInvocations: number;
  failedInvocations: number;
  uniqueContracts: Set<string>;
  uniqueInvokers: Set<string>;
  functionCalls: Map<string, number>;
  recentEvents: ContractInvocationEvent[];
  startTime: Date;
}

async function realTimeMonitorExample() {
  console.log('📊 Starting Real-time Contract Invocation Monitor\n');

  // Initialize monitoring statistics
  const stats: MonitoringStats = {
    totalEvents: 0,
    contractCalls: 0,
    contractCreations: 0,
    wasmUploads: 0,
    successfulInvocations: 0,
    failedInvocations: 0,
    uniqueContracts: new Set(),
    uniqueInvokers: new Set(),
    functionCalls: new Map(),
    recentEvents: [],
    startTime: new Date()
  };

  // Initialize client with enhanced monitoring
  const client = new ContractInvocationClient({
    endpoint: process.env.PROCESSOR_ENDPOINT || 'localhost:50051',
    debug: false, // Disable debug for cleaner dashboard output
    metrics: {
      enabled: true,
      reportingInterval: 5000 // Update every 5 seconds
    },
    flowctl: {
      enabled: process.env.ENABLE_FLOWCTL === 'true',
      endpoint: process.env.FLOWCTL_ENDPOINT || 'localhost:8080',
      network: process.env.STELLAR_NETWORK || 'testnet'
    }
  });

  // Test connection
  console.log('Testing connection...');
  const connected = await client.testConnection();
  if (!connected) {
    console.error('❌ Failed to connect to Contract Invocation Processor');
    console.log('💡 Make sure the processor is running on:', process.env.PROCESSOR_ENDPOINT || 'localhost:50051');
    process.exit(1);
  }
  console.log('✅ Connection successful');
  console.log('🔗 Processor endpoint:', process.env.PROCESSOR_ENDPOINT || 'localhost:50051');
  if (process.env.ENABLE_FLOWCTL === 'true') {
    console.log('🎯 Flowctl integration enabled');
  }
  console.log('\n');

  // Clear screen and setup dashboard
  console.clear();
  displayDashboardHeader();

  // Subscribe to all events for comprehensive monitoring
  const subscription = client.subscribe(
    {
      startLedger: Math.floor(Date.now() / 1000) - 300, // Start from 5 minutes ago
      autoReconnect: true,
      response: {
        includeDiagnosticEvents: true,
        includeStateChanges: true,
        includeSubCalls: true,
        includeTtlExtensions: true,
        includeArchiveRestorations: true
      }
    },
    
    // Event handler - update statistics
    (event: ContractInvocationEvent) => {
      updateStats(stats, event);
      displayDashboard(stats, client.getMetrics());
    },
    
    // Error handler
    (error: Error) => {
      console.log(`\n❌ Stream error: ${error.message}`);
      console.log(`🔄 Client will attempt to reconnect automatically...\n`);
    },
    
    // Status handler
    (status) => {
      if (status.connected) {
        console.log(`✅ Stream connected (Events: ${status.eventsProcessed}, Reconnects: ${status.reconnectAttempts})\n`);
      }
    }
  );

  // Setup periodic dashboard updates (even without new events)
  const dashboardInterval = setInterval(() => {
    displayDashboard(stats, client.getMetrics());
  }, 2000);

  // Setup detailed metrics reporting
  client.on('metrics', (metrics) => {
    // The dashboard will show this automatically
  });

  // Setup graceful shutdown
  process.on('SIGINT', () => {
    clearInterval(dashboardInterval);
    console.clear();
    
    console.log('🛑 Real-time Monitor Shutting Down\n');
    console.log('📈 Final Statistics:');
    console.log('═'.repeat(50));
    
    const runtime = Date.now() - stats.startTime.getTime();
    const runtimeMinutes = Math.floor(runtime / 60000);
    const runtimeSeconds = Math.floor((runtime % 60000) / 1000);
    
    console.log(`Runtime: ${runtimeMinutes}m ${runtimeSeconds}s`);
    console.log(`Total Events: ${stats.totalEvents}`);
    console.log(`Average Rate: ${(stats.totalEvents / (runtime / 1000)).toFixed(2)} events/sec`);
    console.log(`Contract Calls: ${stats.contractCalls} (${((stats.contractCalls / stats.totalEvents) * 100).toFixed(1)}%)`);
    console.log(`Contract Creations: ${stats.contractCreations} (${((stats.contractCreations / stats.totalEvents) * 100).toFixed(1)}%)`);
    console.log(`WASM Uploads: ${stats.wasmUploads} (${((stats.wasmUploads / stats.totalEvents) * 100).toFixed(1)}%)`);
    console.log(`Success Rate: ${((stats.successfulInvocations / stats.totalEvents) * 100).toFixed(1)}%`);
    console.log(`Unique Contracts: ${stats.uniqueContracts.size}`);
    console.log(`Unique Invokers: ${stats.uniqueInvokers.size}`);
    
    console.log('\n🔥 Top Functions Called:');
    const sortedFunctions = Array.from(stats.functionCalls.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10);
    
    sortedFunctions.forEach(([func, count], idx) => {
      console.log(`  ${idx + 1}. ${func}: ${count} calls`);
    });
    
    subscription.unsubscribe();
    client.close();
    process.exit(0);
  });

  console.log('📊 Real-time monitoring active... (Press Ctrl+C to stop)\n');
}

function displayDashboardHeader() {
  console.log('═'.repeat(80));
  console.log('🌟 CONTRACT INVOCATION PROCESSOR - REAL-TIME MONITOR');
  console.log('═'.repeat(80));
}

function displayDashboard(stats: MonitoringStats, clientMetrics: any) {
  // Move cursor to top and clear screen content
  process.stdout.write('\x1B[H\x1B[J');
  
  displayDashboardHeader();
  
  const runtime = Date.now() - stats.startTime.getTime();
  const runtimeStr = formatDuration(runtime);
  const currentRate = clientMetrics.eventsPerSecond || 0;
  const avgRate = stats.totalEvents / (runtime / 1000) || 0;
  
  // Main statistics
  console.log(`\n📊 LIVE STATISTICS (Runtime: ${runtimeStr})`);
  console.log('─'.repeat(50));
  console.log(`Total Events:       ${stats.totalEvents.toString().padStart(8)} │ Current Rate: ${currentRate.toFixed(1)} evt/s`);
  console.log(`Contract Calls:     ${stats.contractCalls.toString().padStart(8)} │ Average Rate: ${avgRate.toFixed(1)} evt/s`);
  console.log(`Contract Creations: ${stats.contractCreations.toString().padStart(8)} │ Uptime: ${formatDuration(clientMetrics.uptime)}`);
  console.log(`WASM Uploads:       ${stats.wasmUploads.toString().padStart(8)} │ Stream Errors: ${clientMetrics.streamStatus?.errors || 0}`);
  
  // Success metrics
  const successRate = stats.totalEvents > 0 ? (stats.successfulInvocations / stats.totalEvents) * 100 : 0;
  const failureRate = stats.totalEvents > 0 ? (stats.failedInvocations / stats.totalEvents) * 100 : 0;
  
  console.log('\n🎯 SUCCESS METRICS');
  console.log('─'.repeat(30));
  console.log(`Successful: ${stats.successfulInvocations.toString().padStart(6)} (${successRate.toFixed(1)}%)`);
  console.log(`Failed:     ${stats.failedInvocations.toString().padStart(6)} (${failureRate.toFixed(1)}%)`);
  
  // Unique entities
  console.log('\n🔍 UNIQUE ENTITIES');
  console.log('─'.repeat(25));
  console.log(`Contracts: ${stats.uniqueContracts.size.toString().padStart(6)}`);
  console.log(`Invokers:  ${stats.uniqueInvokers.size.toString().padStart(6)}`);
  
  // Top function calls
  console.log('\n🚀 TOP FUNCTIONS');
  console.log('─'.repeat(35));
  const topFunctions = Array.from(stats.functionCalls.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5);
  
  if (topFunctions.length === 0) {
    console.log('  No function calls yet...');
  } else {
    topFunctions.forEach(([func, count], idx) => {
      const truncatedFunc = func.length > 20 ? func.substring(0, 17) + '...' : func;
      console.log(`  ${(idx + 1).toString().padStart(2)}. ${truncatedFunc.padEnd(20)} │ ${count.toString().padStart(4)} calls`);
    });
  }
  
  // Recent events
  console.log('\n⚡ RECENT EVENTS');
  console.log('─'.repeat(70));
  if (stats.recentEvents.length === 0) {
    console.log('  Waiting for events...');
  } else {
    stats.recentEvents.slice(0, 5).forEach((event, idx) => {
      const type = getEventTypeShort(event);
      const contract = event.getContractId().substring(0, 8) + '...';
      const ledger = event.getLedgerSequence();
      const success = event.getSuccessful() ? '✅' : '❌';
      const timeAgo = formatTimeAgo(event.getTimestamp() * 1000);
      
      let detail = '';
      const contractCall = event.getContractCall();
      if (contractCall) {
        detail = contractCall.getFunctionName().substring(0, 12);
      } else if (event.getCreateContract()) {
        detail = 'create';
      } else if (event.getUploadWasm()) {
        detail = 'upload';
      }
      
      console.log(`  ${type} │ ${contract} │ L${ledger.toString().padStart(7)} │ ${success} │ ${detail.padEnd(12)} │ ${timeAgo}`);
    });
  }
  
  // Connection status
  const connectionStatus = clientMetrics.streamStatus?.connected ? '🟢 CONNECTED' : '🔴 DISCONNECTED';
  console.log(`\n${connectionStatus} │ Last update: ${new Date().toLocaleTimeString()}`);
  console.log('─'.repeat(80));
}

function updateStats(stats: MonitoringStats, event: ContractInvocationEvent) {
  stats.totalEvents++;
  
  // Track event types
  if (event.getContractCall()) {
    stats.contractCalls++;
    const contractCall = event.getContractCall();
    if (contractCall) {
      const functionName = contractCall.getFunctionName();
      stats.functionCalls.set(functionName, (stats.functionCalls.get(functionName) || 0) + 1);
      stats.uniqueInvokers.add(contractCall.getInvokingAccount());
    }
  } else if (event.getCreateContract()) {
    stats.contractCreations++;
  } else if (event.getUploadWasm()) {
    stats.wasmUploads++;
  }
  
  // Track success/failure
  if (event.getSuccessful()) {
    stats.successfulInvocations++;
  } else {
    stats.failedInvocations++;
  }
  
  // Track unique contracts
  stats.uniqueContracts.add(event.getContractId());
  
  // Maintain recent events list (keep last 10)
  stats.recentEvents.unshift(event);
  if (stats.recentEvents.length > 10) {
    stats.recentEvents.pop();
  }
}

function getEventTypeShort(event: ContractInvocationEvent): string {
  if (event.getContractCall()) return 'CALL';
  if (event.getCreateContract()) return 'CREATE';
  if (event.getUploadWasm()) return 'UPLOAD';
  return 'UNKNOWN';
}

function formatDuration(ms: number): string {
  const seconds = Math.floor(ms / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);
  
  if (hours > 0) {
    return `${hours}h ${minutes % 60}m`;
  } else if (minutes > 0) {
    return `${minutes}m ${seconds % 60}s`;
  } else {
    return `${seconds}s`;
  }
}

function formatTimeAgo(timestamp: number): string {
  const now = Date.now();
  const diff = now - timestamp;
  const seconds = Math.floor(diff / 1000);
  
  if (seconds < 60) {
    return `${seconds}s ago`;
  } else if (seconds < 3600) {
    return `${Math.floor(seconds / 60)}m ago`;
  } else {
    return `${Math.floor(seconds / 3600)}h ago`;
  }
}

// Run example if this file is executed directly
if (require.main === module) {
  realTimeMonitorExample().catch((error) => {
    console.error('❌ Real-time monitor failed:', error);
    process.exit(1);
  });
}