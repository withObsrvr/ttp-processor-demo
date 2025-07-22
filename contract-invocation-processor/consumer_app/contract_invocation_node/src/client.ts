/**
 * @fileoverview Main Contract Invocation Consumer Client Implementation
 */

import * as grpc from '@grpc/grpc-js';
import { EventEmitter } from 'events';
import {
  ContractInvocationClientConfig,
  CompleteClientConfig,
  SubscriptionOptions,
  EventSubscription,
  ContractInvocationEventHandler,
  AsyncContractInvocationEventHandler,
  ErrorHandler,
  StatusHandler,
  StreamStatus,
  ConsumerMetrics,
  InvocationType,
  FilterOptions,
  ResponseOptions,
  ContentFilterOptions
} from './types';
import { ContractInvocationEvent } from '../gen/contract_invocation/contract_invocation_event_pb';
import {
  GetInvocationsRequest,
  EventContentFilter,
  InvocationTypeFilter,
  TopicFilter,
  DataPatternFilter,
  TimeFilter,
  StateChangeFilter,
  ResponseOptions as ProtoResponseOptions
} from '../gen/contract_invocation_service/contract_invocation_service_pb';
import { ContractInvocationServiceClient } from '../gen/contract_invocation_service/contract_invocation_service_grpc_pb';

/**
 * Main Contract Invocation Consumer Client
 * 
 * Provides a high-level TypeScript interface for consuming Soroban contract invocation events
 * from the Contract Invocation Processor with automatic reconnection, filtering, and monitoring.
 */
export class ContractInvocationClient extends EventEmitter {
  private client: ContractInvocationServiceClient;
  private config: CompleteClientConfig;
  private currentStream: grpc.ClientReadableStream<ContractInvocationEvent> | null = null;
  private status: StreamStatus = {
    connected: false,
    eventsProcessed: 0,
    errors: 0,
    reconnectAttempts: 0
  };
  private metrics: ConsumerMetrics;
  private metricsInterval: NodeJS.Timeout | null = null;
  private lastEventTime: Date | null = null;
  private startTime: Date = new Date();

  /**
   * Create a new Contract Invocation Consumer client
   */
  constructor(config: ContractInvocationClientConfig | CompleteClientConfig) {
    super();
    
    this.config = {
      timeout: 30000,
      maxRetries: 5,
      retryInterval: 2000,
      debug: false,
      metrics: {
        enabled: true,
        reportingInterval: 10000
      },
      flowctl: {
        enabled: false,
        endpoint: 'localhost:8080',
        heartbeatInterval: 10000,
        network: 'testnet',
        healthPort: 8088
      },
      ...config
    };

    // Initialize metrics
    this.metrics = {
      totalEvents: 0,
      eventsPerSecond: 0,
      streamStatus: this.status,
      uptime: 0
    };

    // Create gRPC client
    const credentials = this.config.tls?.enabled 
      ? grpc.credentials.createSsl(this.config.tls.ca, this.config.tls.key, this.config.tls.cert)
      : grpc.credentials.createInsecure();

    this.client = new ContractInvocationServiceClient(this.config.endpoint, credentials);

    // Setup metrics reporting
    if (this.config.metrics?.enabled) {
      this.setupMetricsReporting();
    }

    // Setup flowctl integration
    if (this.config.flowctl?.enabled) {
      this.setupFlowctlIntegration();
    }

    this.log('Client initialized with endpoint:', this.config.endpoint);
  }

  /**
   * Subscribe to contract invocation events with filtering and customization options
   */
  public subscribe(
    options: SubscriptionOptions = {},
    eventHandler: ContractInvocationEventHandler | AsyncContractInvocationEventHandler,
    errorHandler?: ErrorHandler,
    statusHandler?: StatusHandler
  ): EventSubscription {
    const request = this.buildRequest(options);
    
    this.log('Starting subscription with options:', options);
    
    const startStream = () => {
      this.status.connected = false;
      this.emit('status', this.status);
      
      this.currentStream = this.client.getContractInvocations(request);
      
      this.currentStream.on('data', async (event: ContractInvocationEvent) => {
        try {
          if (!this.status.connected) {
            this.status.connected = true;
            this.status.reconnectAttempts = 0;
            this.emit('connected');
            this.log('Successfully connected to stream');
          }

          this.status.eventsProcessed++;
          this.metrics.totalEvents++;
          this.lastEventTime = new Date();
          this.status.lastEventTime = this.lastEventTime;

          // Handle event (support both sync and async handlers)
          if (eventHandler.constructor.name === 'AsyncFunction') {
            await (eventHandler as AsyncContractInvocationEventHandler)(event);
          } else {
            (eventHandler as ContractInvocationEventHandler)(event);
          }

          this.emit('event', event);
        } catch (error) {
          this.handleError(error as Error, errorHandler);
        }
      });

      this.currentStream.on('error', (error: grpc.ServiceError) => {
        this.status.connected = false;
        this.handleError(new Error(`Stream error: ${error.message}`), errorHandler);
        
        if (options.autoReconnect !== false && this.status.reconnectAttempts < (this.config.maxRetries || 5)) {
          this.reconnect(startStream, options.autoReconnect);
        }
      });

      this.currentStream.on('end', () => {
        this.status.connected = false;
        this.emit('disconnected');
        this.log('Stream ended');
        
        if (options.autoReconnect !== false) {
          this.reconnect(startStream, options.autoReconnect);
        }
      });
    };

    startStream();

    // Setup status reporting
    if (statusHandler) {
      this.on('status', statusHandler);
    }

    return {
      unsubscribe: () => {
        this.unsubscribe();
      },
      getStatus: () => ({ ...this.status }),
      getMetrics: () => ({ ...this.metrics })
    };
  }

  /**
   * Unsubscribe from the event stream
   */
  public unsubscribe(): void {
    if (this.currentStream) {
      this.currentStream.cancel();
      this.currentStream = null;
      this.status.connected = false;
      this.emit('disconnected');
      this.log('Stream unsubscribed');
    }
  }

  /**
   * Get current consumer metrics
   */
  public getMetrics(): ConsumerMetrics {
    this.updateMetrics();
    return { ...this.metrics };
  }

  /**
   * Get current stream status
   */
  public getStatus(): StreamStatus {
    return { ...this.status };
  }

  /**
   * Test connection to the gRPC server
   */
  public async testConnection(): Promise<boolean> {
    return new Promise((resolve) => {
      const testRequest = new GetInvocationsRequest();
      testRequest.setStartLedger(1);
      testRequest.setEndLedger(1);

      const testStream = this.client.getContractInvocations(testRequest);
      
      const timeout = setTimeout(() => {
        testStream.cancel();
        resolve(false);
      }, this.config.timeout || 5000);

      testStream.on('data', () => {
        clearTimeout(timeout);
        testStream.cancel();
        resolve(true);
      });

      testStream.on('error', () => {
        clearTimeout(timeout);
        resolve(false);
      });
    });
  }

  /**
   * Close the client and cleanup resources
   */
  public close(): void {
    this.unsubscribe();
    
    if (this.metricsInterval) {
      clearInterval(this.metricsInterval);
      this.metricsInterval = null;
    }

    this.client.close();
    this.removeAllListeners();
    this.log('Client closed');
  }

  /**
   * Build gRPC request from subscription options
   */
  private buildRequest(options: SubscriptionOptions): GetInvocationsRequest {
    const request = new GetInvocationsRequest();

    // Set ledger range
    if (options.startLedger) {
      request.setStartLedger(options.startLedger);
    }
    if (options.endLedger) {
      request.setEndLedger(options.endLedger);
    }

    // Apply filters
    if (options.filter) {
      this.applyFilters(request, options.filter);
    }

    // Apply response customization
    if (options.response) {
      this.applyResponseOptions(request, options.response);
    }

    return request;
  }

  /**
   * Apply filtering options to the request
   */
  private applyFilters(request: GetInvocationsRequest, filter: FilterOptions): void {
    if (filter.contractIds?.length) {
      request.setContractIdsList(filter.contractIds);
    }

    if (filter.invokingAccounts?.length) {
      request.setInvokingAccountsList(filter.invokingAccounts);
    }

    if (filter.functionNames?.length) {
      request.setFunctionNamesList(filter.functionNames);
    }

    if (filter.successful !== undefined) {
      request.setSuccessfulOnly(filter.successful);
    }

    if (filter.invocationTypes?.length) {
      // Map TypeScript enum to protobuf enum
      let typeFilter = InvocationTypeFilter.INVOCATION_TYPE_FILTER_ALL;
      if (filter.invocationTypes.includes(InvocationType.CONTRACT_CALL)) {
        typeFilter = InvocationTypeFilter.INVOCATION_TYPE_FILTER_CONTRACT_CALL;
      } else if (filter.invocationTypes.includes(InvocationType.CREATE_CONTRACT)) {
        typeFilter = InvocationTypeFilter.INVOCATION_TYPE_FILTER_CREATE_CONTRACT;
      } else if (filter.invocationTypes.includes(InvocationType.UPLOAD_WASM)) {
        typeFilter = InvocationTypeFilter.INVOCATION_TYPE_FILTER_UPLOAD_WASM;
      }
      request.setTypeFilter(typeFilter);
    }

    if (filter.contentFilter) {
      this.applyContentFilter(request, filter.contentFilter);
    }
  }

  /**
   * Apply content filtering options to the request
   */
  private applyContentFilter(request: GetInvocationsRequest, contentFilter: ContentFilterOptions): void {
    const filter = new EventContentFilter();

    // Set basic content requirements
    if (contentFilter.requireStateChanges !== undefined) {
      filter.setHasStateChanges(contentFilter.requireStateChanges);
    }

    if (contentFilter.requiredDiagnosticEventTopics?.length) {
      filter.setHasDiagnosticEvents(true);
      // TODO: Add topic filters when needed
    }

    request.setContentFilter(filter);
  }

  /**
   * Apply response customization options to the request
   */
  private applyResponseOptions(request: GetInvocationsRequest, response: ResponseOptions): void {
    const options = new ProtoResponseOptions();

    // Note: The current protobuf definition may not have all these options
    // This is a simplified version that matches the available proto fields
    // TODO: Update when proto definitions are expanded
    
    request.setOptions(options);
  }

  /**
   * Handle stream errors with retry logic
   */
  private handleError(error: Error, errorHandler?: ErrorHandler): void {
    this.status.errors++;
    this.metrics.lastError = {
      message: error.message,
      timestamp: new Date()
    };

    this.log('Stream error:', error.message);
    this.emit('error', error);
    
    if (errorHandler) {
      errorHandler(error);
    }
  }

  /**
   * Reconnect to the stream with backoff
   */
  private reconnect(restartFunction: () => void, enabled: boolean | undefined): void {
    if (enabled === false) return;

    this.status.reconnectAttempts++;
    const backoffDelay = Math.min(
      (this.config.retryInterval || 2000) * Math.pow(2, this.status.reconnectAttempts - 1),
      30000
    );

    this.log(`Reconnecting in ${backoffDelay}ms (attempt ${this.status.reconnectAttempts})`);

    setTimeout(() => {
      if (this.status.reconnectAttempts <= (this.config.maxRetries || 5)) {
        restartFunction();
      }
    }, backoffDelay);
  }

  /**
   * Setup metrics reporting
   */
  private setupMetricsReporting(): void {
    if (this.metricsInterval) return;

    const interval = this.config.metrics?.reportingInterval || 10000;
    this.metricsInterval = setInterval(() => {
      this.updateMetrics();
      this.emit('metrics', this.metrics);
      
      if (this.config.metrics?.metricsCallback) {
        this.config.metrics.metricsCallback(this.metrics);
      }
    }, interval);
  }

  /**
   * Update internal metrics
   */
  private updateMetrics(): void {
    this.metrics.uptime = Date.now() - this.startTime.getTime();
    this.metrics.streamStatus = { ...this.status };
    
    // Calculate events per second over the last reporting period
    if (this.lastEventTime) {
      const timeSinceLastEvent = Date.now() - this.lastEventTime.getTime();
      const reportingInterval = this.config.metrics?.reportingInterval || 10000;
      
      if (timeSinceLastEvent <= reportingInterval) {
        this.metrics.eventsPerSecond = this.metrics.totalEvents / (this.metrics.uptime / 1000);
      } else {
        this.metrics.eventsPerSecond = 0;
      }
    }
  }

  /**
   * Setup flowctl integration
   */
  private setupFlowctlIntegration(): void {
    if (!this.config.flowctl?.enabled) return;

    // TODO: Implement flowctl heartbeat and health reporting
    // This would integrate with the flowctl control plane for monitoring
    this.log('Flowctl integration enabled (implementation pending)');
  }

  /**
   * Debug logging utility
   */
  private log(...args: any[]): void {
    if (this.config.debug) {
      console.log('[ContractInvocationClient]', ...args);
    }
  }
}