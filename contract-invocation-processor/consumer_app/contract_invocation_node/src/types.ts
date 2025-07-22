/**
 * @fileoverview Type definitions and interfaces for Contract Invocation Consumer
 */

import { ContractInvocationEvent } from '../gen/contract_invocation/contract_invocation_event_pb';
import { GetInvocationsRequest } from '../gen/contract_invocation_service/contract_invocation_service_pb';

/**
 * Configuration for the Contract Invocation Consumer client
 */
export interface ContractInvocationClientConfig {
  /** gRPC server endpoint (e.g., 'localhost:50051') */
  endpoint: string;
  /** TLS configuration */
  tls?: {
    enabled: boolean;
    cert?: Buffer;
    key?: Buffer;
    ca?: Buffer;
  };
  /** Connection timeout in milliseconds */
  timeout?: number;
  /** Maximum retry attempts for reconnection */
  maxRetries?: number;
  /** Retry backoff interval in milliseconds */
  retryInterval?: number;
  /** Enable detailed logging */
  debug?: boolean;
}

/**
 * Event handler function for contract invocation events
 */
export type ContractInvocationEventHandler = (event: ContractInvocationEvent) => void;

/**
 * Error handler function for stream errors
 */
export type ErrorHandler = (error: Error) => void;

/**
 * Stream status handler function
 */
export type StatusHandler = (status: StreamStatus) => void;

/**
 * Stream status information
 */
export interface StreamStatus {
  connected: boolean;
  lastEventTime?: Date;
  eventsProcessed: number;
  errors: number;
  reconnectAttempts: number;
}

/**
 * Filtering options for contract invocation events
 */
export interface FilterOptions {
  /** Filter by specific contract IDs */
  contractIds?: string[];
  /** Filter by invoking account addresses */
  invokingAccounts?: string[];
  /** Filter by function names */
  functionNames?: string[];
  /** Filter by invocation success/failure */
  successful?: boolean;
  /** Filter by invocation types */
  invocationTypes?: InvocationType[];
  /** Content-based filtering */
  contentFilter?: ContentFilterOptions;
}

/**
 * Content-based filtering options
 */
export interface ContentFilterOptions {
  /** Required argument count range */
  argumentCount?: {
    min?: number;
    max?: number;
  };
  /** Argument pattern matching */
  argumentPatterns?: string[];
  /** Required diagnostic event topics */
  requiredDiagnosticEventTopics?: string[];
  /** Require state changes */
  requireStateChanges?: boolean;
  /** Require sub-calls */
  requireSubCalls?: boolean;
  /** Require TTL extensions */
  requireTtlExtensions?: boolean;
}

/**
 * Response customization options
 */
export interface ResponseOptions {
  /** Include diagnostic events in response */
  includeDiagnosticEvents?: boolean;
  /** Include state changes in response */
  includeStateChanges?: boolean;
  /** Include contract-to-contract calls */
  includeSubCalls?: boolean;
  /** Include TTL extensions */
  includeTtlExtensions?: boolean;
  /** Include archive restoration events */
  includeArchiveRestorations?: boolean;
}

/**
 * Stream subscription options
 */
export interface SubscriptionOptions {
  /** Starting ledger sequence number */
  startLedger?: number;
  /** Ending ledger sequence number (optional) */
  endLedger?: number;
  /** Filtering options */
  filter?: FilterOptions;
  /** Response customization */
  response?: ResponseOptions;
  /** Enable auto-reconnection on stream failure */
  autoReconnect?: boolean;
}

/**
 * Contract invocation types
 */
export enum InvocationType {
  CONTRACT_CALL = 'CONTRACT_CALL',
  CREATE_CONTRACT = 'CREATE_CONTRACT',
  UPLOAD_WASM = 'UPLOAD_WASM'
}

/**
 * Metrics tracking for the consumer
 */
export interface ConsumerMetrics {
  /** Total events processed */
  totalEvents: number;
  /** Events processed per second */
  eventsPerSecond: number;
  /** Current stream status */
  streamStatus: StreamStatus;
  /** Connection uptime */
  uptime: number;
  /** Last error information */
  lastError?: {
    message: string;
    timestamp: Date;
  };
}

/**
 * Configuration for metrics reporting
 */
export interface MetricsConfig {
  /** Enable metrics collection */
  enabled: boolean;
  /** Metrics reporting interval in milliseconds */
  reportingInterval?: number;
  /** Metrics callback function */
  metricsCallback?: (metrics: ConsumerMetrics) => void;
}

/**
 * Flowctl integration configuration
 */
export interface FlowctlConfig {
  /** Enable flowctl integration */
  enabled: boolean;
  /** Flowctl control plane endpoint */
  endpoint?: string;
  /** Heartbeat interval in milliseconds */
  heartbeatInterval?: number;
  /** Stellar network name */
  network?: string;
  /** Health check port */
  healthPort?: number;
}

/**
 * Complete client configuration combining all options
 */
export interface CompleteClientConfig extends ContractInvocationClientConfig {
  /** Metrics configuration */
  metrics?: MetricsConfig;
  /** Flowctl integration configuration */
  flowctl?: FlowctlConfig;
}

/**
 * Utility type for promise-based event handlers
 */
export type AsyncContractInvocationEventHandler = (event: ContractInvocationEvent) => Promise<void>;

/**
 * Event subscription interface
 */
export interface EventSubscription {
  /** Unsubscribe from the event stream */
  unsubscribe(): void;
  /** Get current subscription status */
  getStatus(): StreamStatus;
  /** Get subscription metrics */
  getMetrics(): ConsumerMetrics;
}