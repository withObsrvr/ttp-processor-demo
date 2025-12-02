/**
 * SDK-based TTP Events Consumer
 *
 * This is the main entry point for the SDK-based consumer that:
 * 1. Starts a gRPC server implementing ConsumerService
 * 2. Registers with flowctl control plane
 * 3. Receives events via Consume() RPC calls
 * 4. Processes TokenTransferEvent messages
 */

import { SDKConsumer, defaultSDKConsumerConfig } from './sdk-consumer';
import { createFlowctlClient } from './sdk-flowctl-client';
import { initHealthServer, getHealthServer } from './health-server';

// Proto imports
const commonPb = require('../gen/flowctl/v1/common_pb');
const stellarPb = require('../gen/stellar/v1/token_transfers_pb');

// Handle graceful shutdown
process.on('SIGINT', async () => {
    console.log('Received SIGINT, shutting down...');
    await shutdown();
    process.exit(0);
});

process.on('SIGTERM', async () => {
    console.log('Received SIGTERM, shutting down...');
    await shutdown();
    process.exit(0);
});

let consumer: SDKConsumer | null = null;
let flowctlClient: any | null = null;

async function shutdown() {
    // Stop flowctl client
    if (flowctlClient) {
        flowctlClient.stop();
    }

    // Stop health server
    const healthServer = getHealthServer();
    if (healthServer) {
        await healthServer.stop();
    }

    // Stop consumer
    if (consumer) {
        await consumer.stop();
    }
}

async function main() {
    console.log('═══════════════════════════════════════════════════');
    console.log('  TTP Events Consumer (SDK-based)');
    console.log('═══════════════════════════════════════════════════');
    console.log();

    // Load configuration
    const config = defaultSDKConsumerConfig();

    console.log('Configuration:');
    console.log(`  Component ID:      ${config.componentId}`);
    console.log(`  Name:              ${config.name}`);
    console.log(`  Version:           ${config.version}`);
    console.log(`  gRPC Endpoint:     ${config.endpoint}`);
    console.log(`  Health Port:       ${config.healthPort}`);
    console.log(`  Max Concurrent:    ${config.maxConcurrent}`);
    console.log(`  Flowctl Enabled:   ${config.flowctlEnabled}`);
    if (config.flowctlEnabled) {
        console.log(`  Flowctl Endpoint:  ${config.flowctlEndpoint}`);
        console.log(`  Heartbeat:         ${config.heartbeatInterval}ms`);
    }
    console.log();

    // Start health server
    console.log(`Starting health server on port ${config.healthPort}...`);
    const healthServer = initHealthServer(config.healthPort);
    healthServer.start();

    // Create consumer
    consumer = new SDKConsumer(config);

    // Register event handler
    consumer.onConsume(async (event: any) => {
        // This is where we process incoming events from the flowctl pipeline
        // The event is a protobuf Event message from flowctl/v1/common.proto

        try {
            // Extract event properties using protobuf getters
            const eventType = event.getType ? event.getType() : 'unknown';
            const eventId = event.getId ? event.getId() : 'unknown';
            const payload = event.getPayload ? event.getPayload() : null;
            const metadata = event.getMetadataMap ? event.getMetadataMap() : null;
            const stellarCursor = event.getStellarCursor ? event.getStellarCursor() : null;

            console.log('═══════════════════════════════════════════════════');
            console.log('Received Event:');
            console.log(`  Type: ${eventType}`);
            console.log(`  ID: ${eventId}`);

            if (stellarCursor) {
                const ledgerSeq = stellarCursor.getLedgerSequence ? stellarCursor.getLedgerSequence() : 'unknown';
                console.log(`  Ledger: ${ledgerSeq}`);
            }

            if (metadata) {
                console.log('  Metadata:');
                const metadataObj = metadata.toObject ? metadata.toObject() : metadata;
                for (const [key, value] of Object.entries(metadataObj)) {
                    console.log(`    ${key}: ${value}`);
                }
            }

            // Only process token transfer events
            if (eventType === 'stellar.token.transfer.v1') {
                // Decode the TokenTransferBatch from payload
                if (payload) {
                    const payloadBytes = payload instanceof Uint8Array ? payload : new Uint8Array(payload);
                    console.log(`  Payload size: ${payloadBytes.length} bytes`);

                    try {
                        const batch = stellarPb.TokenTransferBatch.deserializeBinary(payloadBytes);
                        const events = batch.getEventsList();

                        console.log(`  Token Transfer Events: ${events.length} event(s)`);
                        console.log();

                        for (const ttpEvent of events) {
                            processTokenTransferEvent(ttpEvent);
                        }
                    } catch (decodeError) {
                        console.error('  Error decoding TokenTransferBatch:', decodeError);
                        // Show first 50 bytes in hex for debugging
                        const preview = Array.from(payloadBytes.slice(0, 50))
                            .map(b => b.toString(16).padStart(2, '0'))
                            .join(' ');
                        console.log(`  Payload preview: ${preview}...`);
                    }
                }
            } else {
                console.log(`  Skipping event of type: ${eventType}`);
            }

            console.log('═══════════════════════════════════════════════════');
            console.log();
        } catch (error) {
            console.error('Error processing event:', error);
            throw error; // Propagate error so SDK can handle it
        }
    });

    // Start consumer gRPC server
    console.log('Starting consumer gRPC server...');
    await consumer.start();

    // Register with flowctl if enabled
    if (config.flowctlEnabled) {
        console.log('Registering with flowctl control plane...');
        flowctlClient = createFlowctlClient(config);

        try {
            await flowctlClient.register();
            console.log('Successfully registered with flowctl');

            // Update metrics periodically
            setInterval(() => {
                const metrics = consumer!.getMetrics();
                flowctlClient.updateMetrics({
                    success_count: metrics.successCount,
                    error_count: metrics.errorCount,
                    processing_count: metrics.processingCount,
                });
            }, 5000);
        } catch (error) {
            console.warn('Failed to register with flowctl:', error);
            console.log('Continuing without flowctl integration');
        }
    }

    console.log();
    console.log('═══════════════════════════════════════════════════');
    console.log('  Consumer is running and waiting for events...');
    console.log('  Press Ctrl+C to stop');
    console.log('═══════════════════════════════════════════════════');
    console.log();

    // Keep process alive
    await new Promise(() => {}); // Never resolves
}

/**
 * Process a single TokenTransferEvent
 */
function processTokenTransferEvent(event: any): void {
    console.log('  ─────────────────────────────────────────────────');
    console.log('  Token Transfer Event:');

    const meta = event.getMeta();
    if (meta) {
        console.log(`    Ledger: ${meta.getLedgerSequence()}`);
        console.log(`    TX Hash: ${meta.getTxHash()}`);
        const closedAt = meta.getClosedAt();
        if (closedAt) {
            const timestamp = new Date(closedAt.getSeconds() * 1000);
            console.log(`    Closed At: ${timestamp.toISOString()}`);
        }
        const contractAddr = meta.getContractAddress();
        if (contractAddr) {
            console.log(`    Contract: ${contractAddr}`);
        }
    }

    // Determine event type using getter methods
    const transfer = event.getTransfer();
    const mint = event.getMint();
    const burn = event.getBurn();
    const clawback = event.getClawback();
    const fee = event.getFee();

    if (transfer) {
        console.log(`    Type: Transfer`);
        console.log(`    From: ${transfer.getFrom()}`);
        console.log(`    To: ${transfer.getTo()}`);
        console.log(`    Amount: ${transfer.getAmount()}`);
        console.log(`    Asset: ${formatAsset(transfer.getAsset())}`);
    } else if (mint) {
        console.log(`    Type: Mint`);
        console.log(`    To: ${mint.getTo()}`);
        console.log(`    Amount: ${mint.getAmount()}`);
        console.log(`    Asset: ${formatAsset(mint.getAsset())}`);
    } else if (burn) {
        console.log(`    Type: Burn`);
        console.log(`    From: ${burn.getFrom()}`);
        console.log(`    Amount: ${burn.getAmount()}`);
        console.log(`    Asset: ${formatAsset(burn.getAsset())}`);
    } else if (clawback) {
        console.log(`    Type: Clawback`);
        console.log(`    From: ${clawback.getFrom()}`);
        console.log(`    Amount: ${clawback.getAmount()}`);
        console.log(`    Asset: ${formatAsset(clawback.getAsset())}`);
    } else if (fee) {
        console.log(`    Type: Fee`);
        console.log(`    From: ${fee.getFrom()}`);
        console.log(`    Amount: ${fee.getAmount()}`);
        console.log(`    Asset: ${formatAsset(fee.getAsset())}`);
    }
}

/**
 * Format asset for display
 */
function formatAsset(asset: any): string {
    if (!asset) return 'unknown';

    // Check which asset type is set using getters
    const native = asset.getNative ? asset.getNative() : false;
    const issued = asset.getIssued ? asset.getIssued() : null;
    const contractId = asset.getContractId ? asset.getContractId() : null;

    if (native) {
        return 'XLM (native)';
    } else if (issued) {
        const code = issued.getAssetCode ? issued.getAssetCode() : '?';
        const issuer = issued.getAssetIssuer ? issued.getAssetIssuer() : '?';
        return `${code}:${issuer.substring(0, 8)}...`;
    } else if (contractId) {
        return `Contract:${contractId.substring(0, 16)}...`;
    }

    return 'unknown';
}

// Start the application
main().catch((error) => {
    console.error('Fatal error:', error);
    process.exit(1);
});
