/**
 * @fileoverview Unit tests for ContractInvocationClient
 */

import { ContractInvocationClient } from '../client';
import { InvocationType } from '../types';
import { EventEmitter } from 'events';

// Mock the generated protobuf modules
jest.mock('../../gen/contract_invocation_service/contract_invocation_service_grpc_pb', () => ({
  ContractInvocationServiceClient: jest.fn().mockImplementation(() => ({
    getContractInvocations: jest.fn(),
    close: jest.fn()
  }))
}));

jest.mock('../../gen/contract_invocation_service/contract_invocation_service_pb', () => ({
  GetInvocationsRequest: jest.fn().mockImplementation(() => ({
    setStartLedger: jest.fn(),
    setEndLedger: jest.fn(),
    setContractIdsList: jest.fn(),
    setFunctionNamesList: jest.fn(),
    setInvokingAccountsList: jest.fn(),
    setSuccessfulOnly: jest.fn(),
    setTypeFilter: jest.fn(),
    setContentFilter: jest.fn(),
    setOptions: jest.fn()
  })),
  EventContentFilter: jest.fn().mockImplementation(() => ({
    setHasStateChanges: jest.fn(),
    setHasDiagnosticEvents: jest.fn()
  })),
  InvocationTypeFilter: {
    INVOCATION_TYPE_FILTER_ALL: 0,
    INVOCATION_TYPE_FILTER_CONTRACT_CALL: 1,
    INVOCATION_TYPE_FILTER_CREATE_CONTRACT: 2,
    INVOCATION_TYPE_FILTER_UPLOAD_WASM: 3
  },
  ResponseOptions: jest.fn().mockImplementation(() => ({}))
}));

describe('ContractInvocationClient', () => {
  let client: ContractInvocationClient;
  let mockGrpcClient: any;
  let mockStream: any;

  beforeEach(() => {
    // Create mock stream
    mockStream = new EventEmitter();
    (mockStream as any).cancel = jest.fn();

    // Create mock gRPC client
    mockGrpcClient = {
      getContractInvocations: jest.fn(() => mockStream),
      close: jest.fn()
    };

    // Mock the ContractInvocationServiceClient constructor
    const { ContractInvocationServiceClient } = require('../../gen/contract_invocation_service/contract_invocation_service_grpc_pb');
    ContractInvocationServiceClient.mockImplementation(() => mockGrpcClient);

    // Create client instance
    client = new ContractInvocationClient({
      endpoint: 'localhost:50051',
      debug: false
    });
  });

  afterEach(() => {
    client.close();
    jest.clearAllMocks();
  });

  describe('constructor', () => {
    it('should create client with default configuration', () => {
      const testClient = new ContractInvocationClient({
        endpoint: 'localhost:50051'
      });
      
      expect(testClient).toBeInstanceOf(ContractInvocationClient);
      testClient.close();
    });

    it('should create client with custom configuration', () => {
      const testClient = new ContractInvocationClient({
        endpoint: 'localhost:50051',
        timeout: 60000,
        maxRetries: 10,
        debug: true,
        metrics: {
          enabled: false
        }
      });
      
      expect(testClient).toBeInstanceOf(ContractInvocationClient);
      testClient.close();
    });
  });

  describe('subscribe', () => {
    it('should create subscription with basic options', () => {
      const eventHandler = jest.fn();
      const subscription = client.subscribe(
        { startLedger: 1000 },
        eventHandler
      );

      expect(mockGrpcClient.getContractInvocations).toHaveBeenCalled();
      expect(subscription).toHaveProperty('unsubscribe');
      expect(subscription).toHaveProperty('getStatus');
      expect(subscription).toHaveProperty('getMetrics');
    });

    it('should handle stream data events', async () => {
      const eventHandler = jest.fn();
      const mockEvent = (global as any).testUtils.createMockEvent();

      client.subscribe({ startLedger: 1000 }, eventHandler);

      // Simulate stream data event
      mockStream.emit('data', mockEvent);

      expect(eventHandler).toHaveBeenCalledWith(mockEvent);
    });

    it('should handle async event handlers', async () => {
      const eventHandler = jest.fn().mockResolvedValue(undefined);
      const mockEvent = (global as any).testUtils.createMockEvent();

      client.subscribe({ startLedger: 1000 }, eventHandler);

      // Simulate stream data event
      mockStream.emit('data', mockEvent);

      // Wait for async handler
      await new Promise(resolve => setTimeout(resolve, 0));

      expect(eventHandler).toHaveBeenCalledWith(mockEvent);
    });

    it('should handle stream errors', () => {
      const eventHandler = jest.fn();
      const errorHandler = jest.fn();
      const error = new Error('Stream error');

      // Suppress console output for this test
      const originalConsoleLog = console.log;
      const originalConsoleError = console.error;
      console.log = jest.fn();
      console.error = jest.fn();

      client.subscribe(
        { startLedger: 1000, autoReconnect: false },
        eventHandler,
        errorHandler
      );

      mockStream.emit('error', error);

      expect(errorHandler).toHaveBeenCalledWith(expect.any(Error));

      // Restore console
      console.log = originalConsoleLog;
      console.error = originalConsoleError;
    });

    it('should handle stream end events', () => {
      const eventHandler = jest.fn();
      
      client.subscribe(
        { startLedger: 1000, autoReconnect: false },
        eventHandler
      );

      mockStream.emit('end');

      // Should not attempt reconnection when autoReconnect is false
      expect(mockGrpcClient.getContractInvocations).toHaveBeenCalledTimes(1);
    });
  });

  describe('filtering', () => {
    it('should apply contract ID filters', () => {
      const eventHandler = jest.fn();
      const contractIds = ['contract1', 'contract2'];

      client.subscribe(
        {
          startLedger: 1000,
          filter: { contractIds }
        },
        eventHandler
      );

      expect(mockGrpcClient.getContractInvocations).toHaveBeenCalled();
    });

    it('should apply function name filters', () => {
      const eventHandler = jest.fn();
      const functionNames = ['transfer', 'mint'];

      client.subscribe(
        {
          startLedger: 1000,
          filter: { functionNames }
        },
        eventHandler
      );

      expect(mockGrpcClient.getContractInvocations).toHaveBeenCalled();
    });

    it('should apply success filters', () => {
      const eventHandler = jest.fn();

      client.subscribe(
        {
          startLedger: 1000,
          filter: { successful: true }
        },
        eventHandler
      );

      expect(mockGrpcClient.getContractInvocations).toHaveBeenCalled();
    });

    it('should apply invocation type filters', () => {
      const eventHandler = jest.fn();

      client.subscribe(
        {
          startLedger: 1000,
          filter: { invocationTypes: [InvocationType.CONTRACT_CALL] }
        },
        eventHandler
      );

      expect(mockGrpcClient.getContractInvocations).toHaveBeenCalled();
    });

    it('should apply content filters', () => {
      const eventHandler = jest.fn();

      client.subscribe(
        {
          startLedger: 1000,
          filter: {
            contentFilter: {
              argumentCount: { min: 1, max: 5 },
              argumentPatterns: ['*transfer*'],
              requireStateChanges: true
            }
          }
        },
        eventHandler
      );

      expect(mockGrpcClient.getContractInvocations).toHaveBeenCalled();
    });
  });

  describe('response customization', () => {
    it('should apply response customization options', () => {
      const eventHandler = jest.fn();

      client.subscribe(
        {
          startLedger: 1000,
          response: {
            includeDiagnosticEvents: true,
            includeStateChanges: true,
            includeSubCalls: false
          }
        },
        eventHandler
      );

      expect(mockGrpcClient.getContractInvocations).toHaveBeenCalled();
    });
  });

  describe('unsubscribe', () => {
    it('should cancel active stream', () => {
      const eventHandler = jest.fn();
      client.subscribe({ startLedger: 1000 }, eventHandler);

      client.unsubscribe();

      expect(mockStream.cancel).toHaveBeenCalled();
    });
  });

  describe('getMetrics', () => {
    it('should return current metrics', () => {
      const metrics = client.getMetrics();

      expect(metrics).toHaveProperty('totalEvents');
      expect(metrics).toHaveProperty('eventsPerSecond');
      expect(metrics).toHaveProperty('streamStatus');
      expect(metrics).toHaveProperty('uptime');
    });
  });

  describe('getStatus', () => {
    it('should return current stream status', () => {
      const status = client.getStatus();

      expect(status).toHaveProperty('connected');
      expect(status).toHaveProperty('eventsProcessed');
      expect(status).toHaveProperty('errors');
      expect(status).toHaveProperty('reconnectAttempts');
    });
  });

  describe('testConnection', () => {
    it('should test connection successfully', async () => {
      // Mock successful test stream
      const testStream = new EventEmitter();
      (testStream as any).cancel = jest.fn();
      mockGrpcClient.getContractInvocations.mockReturnValueOnce(testStream);

      const connectionPromise = client.testConnection();

      // Simulate successful data event
      setTimeout(() => {
        testStream.emit('data', {});
      }, 0);

      const result = await connectionPromise;
      expect(result).toBe(true);
    });

    it('should handle connection failure', async () => {
      // Mock failed test stream
      const testStream = new EventEmitter();
      (testStream as any).cancel = jest.fn();
      mockGrpcClient.getContractInvocations.mockReturnValueOnce(testStream);

      const connectionPromise = client.testConnection();

      // Simulate error event
      setTimeout(() => {
        testStream.emit('error', new Error('Connection failed'));
      }, 0);

      const result = await connectionPromise;
      expect(result).toBe(false);
    });

    it('should timeout connection test', async () => {
      // Mock hanging test stream
      const testStream = new EventEmitter();
      (testStream as any).cancel = jest.fn();
      mockGrpcClient.getContractInvocations.mockReturnValueOnce(testStream);

      const result = await client.testConnection();
      expect(result).toBe(false);
    }, 6000); // 6 second timeout for this test
  });

  describe('close', () => {
    it('should cleanup resources', () => {
      client.close();

      expect(mockGrpcClient.close).toHaveBeenCalled();
    });
  });

  describe('event emission', () => {
    it('should emit connected event', () => {
      const connectedHandler = jest.fn();
      client.on('connected', connectedHandler);

      client.subscribe({ startLedger: 1000 }, jest.fn());

      // Simulate stream data to trigger connected state
      const mockEvent = (global as any).testUtils.createMockEvent();
      mockStream.emit('data', mockEvent);

      expect(connectedHandler).toHaveBeenCalled();
    });

    it('should emit disconnected event', () => {
      const disconnectedHandler = jest.fn();
      client.on('disconnected', disconnectedHandler);

      client.subscribe({ startLedger: 1000 }, jest.fn());
      mockStream.emit('end');

      expect(disconnectedHandler).toHaveBeenCalled();
    });

    it('should emit error events', () => {
      const errorHandler = jest.fn();
      client.on('error', errorHandler);

      client.subscribe({ startLedger: 1000, autoReconnect: false }, jest.fn());
      mockStream.emit('error', new Error('Test error'));

      expect(errorHandler).toHaveBeenCalled();
    });

    it('should emit event events', () => {
      const eventHandler = jest.fn();
      client.on('event', eventHandler);

      client.subscribe({ startLedger: 1000 }, jest.fn());

      const mockEvent = (global as any).testUtils.createMockEvent();
      mockStream.emit('data', mockEvent);

      expect(eventHandler).toHaveBeenCalledWith(mockEvent);
    });
  });
});