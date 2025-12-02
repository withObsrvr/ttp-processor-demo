/**
 * SDK-based Consumer Implementation
 *
 * This consumer implements the flowctl SDK pattern by:
 * 1. Exposing a gRPC ConsumerService server
 * 2. Registering with the flowctl control plane
 * 3. Receiving events via the Consume() RPC call
 * 4. Processing TokenTransferEvent messages
 */

import * as grpc from '@grpc/grpc-js';
import { metrics } from './metrics';

// Import flowctl consumer service
const consumerGrpc = require('../gen/flowctl/v1/consumer_grpc_pb');
const consumerPb = require('../gen/flowctl/v1/consumer_pb');
const commonPb = require('../gen/flowctl/v1/common_pb');

export interface SDKConsumerConfig {
    componentId: string;
    name: string;
    description: string;
    version: string;
    endpoint: string;  // gRPC server endpoint (e.g., ":8090")
    healthPort: number;
    maxConcurrent: number;
    flowctlEnabled: boolean;
    flowctlEndpoint: string;
    heartbeatInterval: number; // milliseconds
}

export class SDKConsumer {
    private config: SDKConsumerConfig;
    private server: grpc.Server | null = null;
    private onConsumeHandler: ((event: any) => Promise<void>) | null = null;
    private processingCount = 0;
    private stopping = false;

    constructor(config: SDKConsumerConfig) {
        this.config = config;
    }

    /**
     * Register the event processing handler
     */
    onConsume(handler: (event: any) => Promise<void>): void {
        this.onConsumeHandler = handler;
    }

    /**
     * Start the consumer gRPC server
     */
    async start(): Promise<void> {
        if (!this.onConsumeHandler) {
            throw new Error('Must register onConsume handler before starting');
        }

        this.server = new grpc.Server();

        // Implement ConsumerService
        const consumerService = this.createConsumerService();

        // Register flowctl ConsumerService
        this.server.addService(consumerGrpc.ConsumerServiceService, consumerService);

        console.log(`[SDK Consumer] Starting gRPC server on ${this.config.endpoint}`);

        const endpoint = this.config.endpoint.startsWith(':')
            ? `0.0.0.0${this.config.endpoint}`
            : this.config.endpoint;

        await new Promise<void>((resolve, reject) => {
            this.server!.bindAsync(
                endpoint,
                grpc.ServerCredentials.createInsecure(),
                (err, port) => {
                    if (err) {
                        reject(err);
                        return;
                    }
                    console.log(`[SDK Consumer] gRPC server bound to port ${port}`);
                    resolve();
                }
            );
        });

        console.log(`[SDK Consumer] ${this.config.name} v${this.config.version} started`);
        console.log(`[SDK Consumer] Component ID: ${this.config.componentId}`);
        console.log(`[SDK Consumer] Endpoint: ${this.config.endpoint}`);
        console.log(`[SDK Consumer] Health Port: ${this.config.healthPort}`);
        console.log(`[SDK Consumer] Max Concurrent: ${this.config.maxConcurrent}`);
    }

    /**
     * Stop the consumer gracefully
     */
    async stop(): Promise<void> {
        this.stopping = true;
        console.log('[SDK Consumer] Stopping...');

        // Wait for in-flight processing to complete
        const maxWait = 30000; // 30 seconds
        const startTime = Date.now();

        while (this.processingCount > 0 && Date.now() - startTime < maxWait) {
            console.log(`[SDK Consumer] Waiting for ${this.processingCount} in-flight events...`);
            await new Promise(resolve => setTimeout(resolve, 100));
        }

        if (this.server) {
            await new Promise<void>((resolve) => {
                this.server!.tryShutdown(() => {
                    console.log('[SDK Consumer] gRPC server stopped');
                    resolve();
                });
            });
        }

        console.log('[SDK Consumer] Stopped successfully');
    }

    /**
     * Create the ConsumerService implementation
     */
    private createConsumerService(): any {
        const self = this;

        return {
            // getInfo returns component information
            getInfo(
                call: grpc.ServerUnaryCall<any, any>,
                callback: grpc.sendUnaryData<any>
            ): void {
                const componentInfo = new commonPb.ComponentInfo();
                componentInfo.setId(self.config.componentId);
                componentInfo.setName(self.config.name);
                componentInfo.setDescription(self.config.description);
                componentInfo.setVersion(self.config.version);
                componentInfo.setType(3); // COMPONENT_TYPE_CONSUMER
                componentInfo.setEndpoint(self.config.endpoint);
                componentInfo.setInputEventTypesList(['stellar.token.transfer.v1']);
                componentInfo.setOutputEventTypesList([]);

                callback(null, componentInfo);
            },

            // healthCheck returns health status
            healthCheck(
                call: grpc.ServerUnaryCall<any, any>,
                callback: grpc.sendUnaryData<any>
            ): void {
                const response = new commonPb.HealthCheckResponse();
                response.setStatus(self.stopping ? 5 : 2); // STOPPING or HEALTHY

                callback(null, response);
            },

            // consume handles client-streaming event consumption
            // Receives multiple events from the stream, processes them, and returns one final response
            consume(
                call: grpc.ServerReadableStream<any, any>,
                callback: grpc.sendUnaryData<any>
            ): void {
                console.log('[SDK Consumer] Consume stream started');

                let eventsConsumed = 0;
                let eventsFailed = 0;
                const errors: string[] = [];

                call.on('data', async (event: any) => {
                    if (self.stopping) {
                        console.warn('[SDK Consumer] Received event while stopping, ignoring');
                        return;
                    }

                    // Check concurrency limit
                    if (self.processingCount >= self.config.maxConcurrent) {
                        console.warn('[SDK Consumer] Max concurrent limit reached, skipping event');
                        eventsFailed++;
                        errors.push(`Max concurrent limit (${self.config.maxConcurrent}) reached`);
                        return;
                    }

                    self.processingCount++;
                    const startTime = Date.now();

                    try {
                        // Process the event
                        if (self.onConsumeHandler) {
                            await self.onConsumeHandler(event);
                        }

                        const processingTime = Date.now() - startTime;

                        // Get ledger sequence from event metadata if available
                        const ledgerSeq = event?.metadata?.ledger_sequence || 0;
                        metrics.recordSuccess(parseInt(ledgerSeq), 1, processingTime);

                        eventsConsumed++;
                    } catch (error) {
                        const processingTime = Date.now() - startTime;
                        console.error('[SDK Consumer] Error processing event:', error);

                        if (error instanceof Error) {
                            metrics.recordError(error);
                        }

                        eventsFailed++;
                        const errorMsg = error instanceof Error ? error.message : String(error);
                        errors.push(errorMsg);
                    } finally {
                        self.processingCount--;
                    }
                });

                call.on('end', () => {
                    console.log('[SDK Consumer] Consume stream ended', {
                        eventsConsumed,
                        eventsFailed,
                        errorCount: errors.length
                    });

                    // Send final response summarizing consumption
                    const response = new consumerPb.ConsumeResponse();
                    response.setEventsConsumed(eventsConsumed);
                    response.setEventsFailed(eventsFailed);
                    response.setErrorsList(errors);

                    callback(null, response);
                });

                call.on('error', (error) => {
                    console.error('[SDK Consumer] Consume stream error:', error);
                    callback(error);
                });
            }
        };
    }

    getMetrics() {
        return {
            ...metrics.getMetrics(),
            processingCount: this.processingCount,
        };
    }
}

/**
 * Default configuration factory
 */
export function defaultSDKConsumerConfig(): SDKConsumerConfig {
    return {
        componentId: process.env.COMPONENT_ID || 'ttp-consumer-node',
        name: 'TTP Events Consumer (Node.js)',
        description: 'Consumes and displays token transfer events from Stellar',
        version: '2.0.0-sdk',
        endpoint: process.env.PORT || ':8090',
        healthPort: parseInt(process.env.HEALTH_PORT || '8089'),
        maxConcurrent: parseInt(process.env.MAX_CONCURRENT || '10'),
        flowctlEnabled: process.env.ENABLE_FLOWCTL?.toLowerCase() === 'true',
        flowctlEndpoint: process.env.FLOWCTL_ENDPOINT || 'localhost:8080',
        heartbeatInterval: parseInt(process.env.FLOWCTL_HEARTBEAT_INTERVAL || '10000'),
    };
}
