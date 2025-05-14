import { EventServiceClient } from './client';
import { metrics } from './metrics';
import { initFlowctlClient, getFlowctlClient } from './flowctl-client';
import { initHealthServer, getHealthServer } from './health-server';

// Handle process termination gracefully
process.on('SIGINT', async () => {
    console.log('Received SIGINT, shutting down...');
    // Stop flowctl client if it's running
    const flowctlClient = getFlowctlClient();
    if (flowctlClient) {
        flowctlClient.stop();
    }
    
    // Stop health server if it's running
    const healthServer = getHealthServer();
    if (healthServer) {
        await healthServer.stop();
    }
    
    process.exit(0);
});

process.on('SIGTERM', async () => {
    console.log('Received SIGTERM, shutting down...');
    // Stop flowctl client if it's running
    const flowctlClient = getFlowctlClient();
    if (flowctlClient) {
        flowctlClient.stop();
    }
    
    // Stop health server if it's running
    const healthServer = getHealthServer();
    if (healthServer) {
        await healthServer.stop();
    }
    
    process.exit(0);
});

async function main() {
    console.log('Starting TTP events consumer application');
    
    // Initialize health server
    const healthPort = parseInt(process.env.HEALTH_PORT || '8093');
    const healthServer = initHealthServer(healthPort);
    healthServer.start();
    
    // Initialize flowctl integration if enabled
    if (process.env.ENABLE_FLOWCTL?.toLowerCase() === 'true') {
        console.log('Initializing flowctl integration');
        const flowctlEndpoint = process.env.FLOWCTL_ENDPOINT || 'localhost:8080';
        const heartbeatInterval = parseInt(process.env.FLOWCTL_HEARTBEAT_INTERVAL || '10000');
        
        const flowctlClient = initFlowctlClient(flowctlEndpoint, heartbeatInterval);
        try {
            await flowctlClient.register({
                network: process.env.STELLAR_NETWORK || 'testnet',
                consumer_type: 'node-application'
            });
        } catch (error) {
            console.warn(`Failed to register with flowctl: ${error}`);
        }
    }
    
    // an example of how to get a streamTTP events for a specific range of ledgers
    // from a GRPC serviceusing the protobufs generated code from github.comstellar/go/protos
    //
    const args = process.argv.slice(2);
    if (args.length !== 2) {
        console.error('Usage: node index.ts <startLedger> <endLedger>');
        process.exit(1);
    }

    const startLedger = parseInt(args[0]);
    // if the end ledger is less than the start ledger, the server will stream indefinitely
    const endLedger = parseInt(args[1]);

    if (isNaN(startLedger) || isNaN(endLedger)) {
        console.error('Error: startLedger and endLedger must be numbers');
        process.exit(1);
    }

    const serviceAddress = process.env.TTP_SERVICE_ADDRESS || 'localhost:50051';
    console.log(`Connecting to TTP service at ${serviceAddress}`);
    const client = new EventServiceClient(serviceAddress);
    
    try {
        console.log(`Requesting TTP events from ledger ${startLedger} to ${endLedger}`);
        client.getTTPEvents(
            startLedger,
            endLedger,
            (event) => {
                // Start timing for metrics
                const startTime = Date.now();
                
                try {
                    // Do cool Stuff with TTP events!
                    // ideally something more intresting than logging to console..
                    console.log('Received TTP event:');
                    console.log(JSON.stringify(event.toObject(), null, 2));
                    console.log('-------------------');
                    
                    // Record success and processing time in metrics
                    // Extract ledger sequence from event (get it from the event object structure)
                    const eventObj = event.toObject();
                    // Get ledger sequence from meta.ledger_sequence which is the correct structure
                    const ledgerSeq = eventObj.meta?.ledger_sequence || 0;
                    const processingTime = Date.now() - startTime;
                    metrics.recordSuccess(ledgerSeq, 1, processingTime);
                } catch (error) {
                    console.error('Error processing TTP event:', error);
                    if (error instanceof Error) {
                        metrics.recordError(error);
                    } else {
                        metrics.recordError(new Error(String(error)));
                    }
                }
            }
        );
        
    } catch (error) {
        console.error('Error getting TTP events:', error);
        if (error instanceof Error) {
            metrics.recordError(error);
        } else {
            metrics.recordError(new Error(String(error)));
        }
    }
}

main().catch(error => {
    console.error('Unhandled error in main:', error);
    process.exit(1);
}); 