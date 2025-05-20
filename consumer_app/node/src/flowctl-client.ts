import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { metrics } from './metrics';
import { randomUUID } from 'crypto';
import * as path from 'path';
import * as fs from 'fs';

// Proto definition for flowctl control plane
const PROTO_PATH = path.resolve(__dirname, '../protos/control_plane.proto');

// Create a temporary proto file if we can't find the actual one
if (!fs.existsSync(PROTO_PATH)) {
  const protoDir = path.dirname(PROTO_PATH);
  if (!fs.existsSync(protoDir)) {
    fs.mkdirSync(protoDir, { recursive: true });
  }
  
  // Define the flowctl proto based on the Go implementation we've seen
  const protoContent = `
syntax = "proto3";

package flowctl;

import "google/protobuf/empty.proto";
import "google/protobuf/timestamp.proto";

// Service types that can register with the control plane
enum ServiceType {
  SERVICE_TYPE_UNSPECIFIED = 0;
  SERVICE_TYPE_SOURCE = 1;
  SERVICE_TYPE_PROCESSOR = 2;
  SERVICE_TYPE_SINK = 3;
  SERVICE_TYPE_CONSUMER = 4;
}

// Service registration information
message ServiceInfo {
  string service_id = 1;
  ServiceType service_type = 2;
  repeated string input_event_types = 3;  // For processors and consumers
  repeated string output_event_types = 4; // For sources and processors
  string health_endpoint = 5;             // Health/metrics endpoint
  int32 max_inflight = 6;                 // Back-pressure credits
  map<string, string> metadata = 7;       // Additional service metadata
}

// Registration acknowledgment
message RegistrationAck {
  string service_id = 1;
  repeated string topic_names = 2;        // Kafka topics to consume/produce
  map<string, string> connection_info = 3; // Connection details (endpoints, creds)
}

// ServiceHeartbeat message
message ServiceHeartbeat {
  string service_id = 1;
  google.protobuf.Timestamp timestamp = 2;
  map<string, double> metrics = 3;        // Service-specific metrics
}

// Control plane service definition
service ControlPlane {
  // Register a new service with the control plane
  rpc Register(ServiceInfo) returns (RegistrationAck);
  
  // Send periodic heartbeats
  rpc Heartbeat(ServiceHeartbeat) returns (google.protobuf.Empty);
}
`;
  fs.writeFileSync(PROTO_PATH, protoContent);
  console.log(`Created proto file at ${PROTO_PATH}`);
}

// Load and configure proto
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

// Load service definition
const flowctlProto = grpc.loadPackageDefinition(packageDefinition).flowctl as any;

/**
 * FlowctlClient - Client for integrating with the flowctl control plane
 */
export class FlowctlClient {
  private client: any;
  private serviceId: string;
  private heartbeatInterval: NodeJS.Timeout | null = null;
  private intervalMs: number;
  private connected: boolean = false;

  /**
   * Constructor
   * @param endpoint flowctl control plane endpoint 
   * @param intervalMs heartbeat interval in milliseconds
   */
  constructor(
    private endpoint: string = 'localhost:8080',
    intervalMs: number = 10000
  ) {
    this.serviceId = 'consumer-' + randomUUID().slice(0, 8);
    this.intervalMs = intervalMs;
    
    // Create the gRPC client
    this.client = new flowctlProto.ControlPlane(
      endpoint,
      grpc.credentials.createInsecure()
    );

    console.log(`FlowctlClient created with service ID: ${this.serviceId}`);
  }

  /**
   * Register the consumer with the flowctl control plane
   * @param metadata Additional metadata to include in registration
   */
  public async register(metadata: Record<string, string> = {}): Promise<string> {
    try {
      // Create service info
      const serviceInfo = {
        service_type: 'SERVICE_TYPE_CONSUMER',
        input_event_types: ['event_service.TokenTransferEvent'],
        health_endpoint: process.env.HEALTH_ENDPOINT || 'http://localhost:8093/health',
        max_inflight: 100,
        metadata: {
          application: 'node-consumer',
          ...metadata
        }
      };

      // Register with the control plane
      const ack = await new Promise<any>((resolve, reject) => {
        this.client.Register(serviceInfo, (err: Error, response: any) => {
          if (err) {
            reject(err);
          } else {
            resolve(response);
          }
        });
      });

      // Store service ID from response
      if (ack && ack.service_id) {
        this.serviceId = ack.service_id;
      }
      
      this.connected = true;
      
      console.log(`Registered with flowctl control plane with service ID: ${this.serviceId}`);
      if (ack.topic_names && ack.topic_names.length > 0) {
        console.log(`Assigned topics: ${ack.topic_names.join(', ')}`);
      }

      // Start heartbeat loop
      this.startHeartbeatLoop();

      return this.serviceId;
    } catch (error) {
      console.warn(`Failed to register with flowctl: ${error}`);
      console.log(`Using simulated ID: ${this.serviceId}`);
      
      // Start heartbeat loop anyway with simulated ID
      this.startHeartbeatLoop();
      
      return this.serviceId;
    }
  }

  /**
   * Start the heartbeat loop
   */
  private startHeartbeatLoop(): void {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
    }

    this.heartbeatInterval = setInterval(() => {
      this.sendHeartbeat().catch(error => {
        console.warn(`Failed to send heartbeat: ${error}`);
      });
    }, this.intervalMs);
  }

  /**
   * Send a single heartbeat to the control plane
   */
  private async sendHeartbeat(): Promise<void> {
    if (!this.connected) return;

    try {
      const metricsData = metrics.toJSON();
      
      const heartbeat = {
        service_id: this.serviceId,
        timestamp: { seconds: Math.floor(Date.now() / 1000), nanos: 0 },
        metrics: {
          success_count: metricsData.success_count,
          error_count: metricsData.error_count,
          total_processed: metricsData.total_processed,
          total_events_received: metricsData.total_events_received,
          last_processed_ledger: metricsData.last_processed_ledger,
          processing_latency_ms: metricsData.processing_latency_ms,
          uptime_ms: metricsData.uptime_ms
        }
      };

      await new Promise<void>((resolve, reject) => {
        this.client.Heartbeat(heartbeat, (err: Error) => {
          if (err) {
            reject(err);
          } else {
            resolve();
          }
        });
      });

      console.log(`Sent heartbeat for service ${this.serviceId}`);
    } catch (error) {
      throw error;
    }
  }

  /**
   * Stop the flowctl client
   */
  public stop(): void {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }
    
    this.connected = false;
    console.log('Stopped flowctl client');
  }
}

// Create singleton instance
let flowctlClientInstance: FlowctlClient | null = null;

/**
 * Initialize the flowctl client
 * @param endpoint flowctl control plane endpoint
 * @param intervalMs heartbeat interval in milliseconds
 */
export function initFlowctlClient(
  endpoint: string = process.env.FLOWCTL_ENDPOINT || 'localhost:8080',
  intervalMs: number = parseInt(process.env.FLOWCTL_HEARTBEAT_INTERVAL || '10000')
): FlowctlClient {
  if (!flowctlClientInstance) {
    flowctlClientInstance = new FlowctlClient(endpoint, intervalMs);
  }
  return flowctlClientInstance;
}

/**
 * Get the flowctl client instance
 */
export function getFlowctlClient(): FlowctlClient | null {
  return flowctlClientInstance;
}