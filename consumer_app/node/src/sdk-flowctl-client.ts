/**
 * Flowctl Control Plane Client for SDK-based Consumer
 *
 * Handles registration and heartbeats with the flowctl control plane
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import * as path from 'path';

export interface FlowctlConfig {
    endpoint: string;
    componentId: string;
    name: string;
    description: string;
    version: string;
    consumerEndpoint: string;
    heartbeatInterval: number;
}

export class SDKFlowctlClient {
    private config: FlowctlConfig;
    private client: any | null = null;
    private heartbeatTimer: NodeJS.Timeout | null = null;
    private registered = false;
    private assignedId: string | null = null;

    constructor(config: FlowctlConfig) {
        this.config = config;
    }

    /**
     * Register the consumer with flowctl control plane
     */
    async register(): Promise<void> {
        console.log(`[Flowctl] Connecting to control plane at ${this.config.endpoint}`);

        try {
            // Load control plane proto file
            const PROTO_PATH = '/home/tillman/Documents/flow-proto/proto/flowctl/v1/control_plane.proto';
            const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
                keepCase: true,
                longs: String,
                enums: String,
                defaults: true,
                oneofs: true,
                includeDirs: [
                    '/home/tillman/Documents/flow-proto/proto'
                ]
            });

            const protoDescriptor: any = grpc.loadPackageDefinition(packageDefinition);
            const ControlPlaneService = protoDescriptor.flowctl.v1.ControlPlaneService;

            // Create gRPC client
            this.client = new ControlPlaneService(
                this.config.endpoint,
                grpc.credentials.createInsecure()
            );

            const registerRequest = {
                component: {
                    id: this.config.componentId,
                    name: this.config.name,
                    description: this.config.description,
                    version: this.config.version,
                    type: 3, // COMPONENT_TYPE_CONSUMER
                    endpoint: this.config.consumerEndpoint,
                    input_event_types: ['stellar.token.transfer.v1'],
                    output_event_types: [],
                    metadata: {},
                    health_port: 9089,
                    health_check_path: '/health'
                }
            };

            console.log(`[Flowctl] Registering consumer: ${this.config.componentId}`);

            // Register with control plane
            const response: any = await new Promise((resolve, reject) => {
                this.client!.RegisterComponent(registerRequest, (err: any, response: any) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(response);
                    }
                });
            });

            this.registered = true;
            this.assignedId = response.service_id || this.config.componentId;

            console.log(`[Flowctl] Successfully registered with ID: ${this.assignedId}`);

            // Send immediate first heartbeat to mark as healthy
            await this.sendHeartbeat();
            console.log('[Flowctl] Sent initial heartbeat');

            // Wait a moment for flowctl to process the heartbeat
            await new Promise(resolve => setTimeout(resolve, 100));

            // Start heartbeat loop
            this.startHeartbeat();
        } catch (error) {
            console.error('[Flowctl] Failed to register with control plane:', error);
            throw error; // Throw so flowctl run knows registration failed
        }
    }

    /**
     * Start sending heartbeats to control plane
     */
    private startHeartbeat(): void {
        if (this.heartbeatTimer) {
            return;
        }

        console.log(`[Flowctl] Starting heartbeat every ${this.config.heartbeatInterval}ms`);

        this.heartbeatTimer = setInterval(async () => {
            try {
                await this.sendHeartbeat();
            } catch (error) {
                console.error('[Flowctl] Heartbeat failed:', error);
            }
        }, this.config.heartbeatInterval);
    }

    /**
     * Send a single heartbeat to control plane
     */
    private async sendHeartbeat(): Promise<void> {
        if (!this.registered || !this.client) {
            return;
        }

        const heartbeatRequest = {
            service_id: this.assignedId || this.config.componentId,
            status: 2, // HEALTH_STATUS_HEALTHY (correct value from proto)
            metrics: {
                // Metrics will be populated from consumer
            },
        };

        try {
            await new Promise((resolve, reject) => {
                this.client!.Heartbeat(heartbeatRequest, (err: any, response: any) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(response);
                    }
                });
            });
        } catch (error) {
            console.error('[Flowctl] Heartbeat failed:', error);
        }
    }

    /**
     * Update heartbeat metrics
     */
    updateMetrics(metrics: Record<string, number>): void {
        // Metrics will be sent in next heartbeat
    }

    /**
     * Stop heartbeat and disconnect
     */
    stop(): void {
        console.log('[Flowctl] Stopping flowctl client');

        if (this.heartbeatTimer) {
            clearInterval(this.heartbeatTimer);
            this.heartbeatTimer = null;
        }

        // Optionally: send unregister request to control plane

        this.registered = false;
        this.client = null;

        console.log('[Flowctl] Flowctl client stopped');
    }
}

/**
 * Create a flowctl client from consumer config
 */
export function createFlowctlClient(consumerConfig: {
    componentId: string;
    name: string;
    description: string;
    version: string;
    endpoint: string;
    flowctlEndpoint: string;
    heartbeatInterval: number;
}): SDKFlowctlClient {
    const config: FlowctlConfig = {
        endpoint: consumerConfig.flowctlEndpoint,
        componentId: consumerConfig.componentId,
        name: consumerConfig.name,
        description: consumerConfig.description,
        version: consumerConfig.version,
        consumerEndpoint: consumerConfig.endpoint,
        heartbeatInterval: consumerConfig.heartbeatInterval,
    };

    return new SDKFlowctlClient(config);
}
