/**
 * @fileoverview Unit tests for type definitions
 */

import {
  InvocationType,
  ContractInvocationClientConfig,
  SubscriptionOptions,
  FilterOptions,
  ContentFilterOptions,
  ResponseOptions
} from '../types';

describe('Types', () => {
  describe('InvocationType enum', () => {
    it('should have correct enum values', () => {
      expect(InvocationType.CONTRACT_CALL).toBe('CONTRACT_CALL');
      expect(InvocationType.CREATE_CONTRACT).toBe('CREATE_CONTRACT');
      expect(InvocationType.UPLOAD_WASM).toBe('UPLOAD_WASM');
    });
  });

  describe('ContractInvocationClientConfig interface', () => {
    it('should accept minimal configuration', () => {
      const config: ContractInvocationClientConfig = {
        endpoint: 'localhost:50051'
      };

      expect(config.endpoint).toBe('localhost:50051');
    });

    it('should accept full configuration', () => {
      const config: ContractInvocationClientConfig = {
        endpoint: 'localhost:50051',
        tls: {
          enabled: true,
          cert: Buffer.from('cert'),
          key: Buffer.from('key'),
          ca: Buffer.from('ca')
        },
        timeout: 30000,
        maxRetries: 5,
        retryInterval: 2000,
        debug: true
      };

      expect(config.endpoint).toBe('localhost:50051');
      expect(config.tls?.enabled).toBe(true);
      expect(config.timeout).toBe(30000);
      expect(config.maxRetries).toBe(5);
      expect(config.retryInterval).toBe(2000);
      expect(config.debug).toBe(true);
    });
  });

  describe('SubscriptionOptions interface', () => {
    it('should accept empty options', () => {
      const options: SubscriptionOptions = {};
      expect(options).toBeDefined();
    });

    it('should accept ledger range', () => {
      const options: SubscriptionOptions = {
        startLedger: 1000,
        endLedger: 2000
      };

      expect(options.startLedger).toBe(1000);
      expect(options.endLedger).toBe(2000);
    });

    it('should accept auto-reconnect option', () => {
      const options: SubscriptionOptions = {
        autoReconnect: true
      };

      expect(options.autoReconnect).toBe(true);
    });
  });

  describe('FilterOptions interface', () => {
    it('should accept empty filter options', () => {
      const filter: FilterOptions = {};
      expect(filter).toBeDefined();
    });

    it('should accept contract ID filters', () => {
      const filter: FilterOptions = {
        contractIds: ['contract1', 'contract2']
      };

      expect(filter.contractIds).toEqual(['contract1', 'contract2']);
    });

    it('should accept function name filters', () => {
      const filter: FilterOptions = {
        functionNames: ['transfer', 'mint', 'burn']
      };

      expect(filter.functionNames).toEqual(['transfer', 'mint', 'burn']);
    });

    it('should accept invoking account filters', () => {
      const filter: FilterOptions = {
        invokingAccounts: ['account1', 'account2']
      };

      expect(filter.invokingAccounts).toEqual(['account1', 'account2']);
    });

    it('should accept success filter', () => {
      const filter: FilterOptions = {
        successful: true
      };

      expect(filter.successful).toBe(true);
    });

    it('should accept invocation type filters', () => {
      const filter: FilterOptions = {
        invocationTypes: [InvocationType.CONTRACT_CALL, InvocationType.CREATE_CONTRACT]
      };

      expect(filter.invocationTypes).toEqual([
        InvocationType.CONTRACT_CALL,
        InvocationType.CREATE_CONTRACT
      ]);
    });
  });

  describe('ContentFilterOptions interface', () => {
    it('should accept argument count filters', () => {
      const contentFilter: ContentFilterOptions = {
        argumentCount: {
          min: 1,
          max: 5
        }
      };

      expect(contentFilter.argumentCount?.min).toBe(1);
      expect(contentFilter.argumentCount?.max).toBe(5);
    });

    it('should accept argument patterns', () => {
      const contentFilter: ContentFilterOptions = {
        argumentPatterns: ['*transfer*', 'mint*', '*burn']
      };

      expect(contentFilter.argumentPatterns).toEqual(['*transfer*', 'mint*', '*burn']);
    });

    it('should accept diagnostic event topic requirements', () => {
      const contentFilter: ContentFilterOptions = {
        requiredDiagnosticEventTopics: ['Transfer', 'Approval']
      };

      expect(contentFilter.requiredDiagnosticEventTopics).toEqual(['Transfer', 'Approval']);
    });

    it('should accept boolean requirements', () => {
      const contentFilter: ContentFilterOptions = {
        requireStateChanges: true,
        requireSubCalls: false,
        requireTtlExtensions: true
      };

      expect(contentFilter.requireStateChanges).toBe(true);
      expect(contentFilter.requireSubCalls).toBe(false);
      expect(contentFilter.requireTtlExtensions).toBe(true);
    });
  });

  describe('ResponseOptions interface', () => {
    it('should accept empty response options', () => {
      const response: ResponseOptions = {};
      expect(response).toBeDefined();
    });

    it('should accept inclusion options', () => {
      const response: ResponseOptions = {
        includeDiagnosticEvents: true,
        includeStateChanges: false,
        includeSubCalls: true,
        includeTtlExtensions: false,
        includeArchiveRestorations: true
      };

      expect(response.includeDiagnosticEvents).toBe(true);
      expect(response.includeStateChanges).toBe(false);
      expect(response.includeSubCalls).toBe(true);
      expect(response.includeTtlExtensions).toBe(false);
      expect(response.includeArchiveRestorations).toBe(true);
    });
  });

  describe('Complex filter combinations', () => {
    it('should accept comprehensive filter options', () => {
      const filter: FilterOptions = {
        contractIds: ['contract1'],
        functionNames: ['transfer'],
        invokingAccounts: ['account1'],
        successful: true,
        invocationTypes: [InvocationType.CONTRACT_CALL],
        contentFilter: {
          argumentCount: {
            min: 1,
            max: 3
          },
          argumentPatterns: ['*amount*'],
          requiredDiagnosticEventTopics: ['Transfer'],
          requireStateChanges: true,
          requireSubCalls: false,
          requireTtlExtensions: false
        }
      };

      expect(filter).toBeDefined();
      expect(filter.contractIds).toEqual(['contract1']);
      expect(filter.functionNames).toEqual(['transfer']);
      expect(filter.invokingAccounts).toEqual(['account1']);
      expect(filter.successful).toBe(true);
      expect(filter.invocationTypes).toEqual([InvocationType.CONTRACT_CALL]);
      expect(filter.contentFilter?.argumentCount?.min).toBe(1);
      expect(filter.contentFilter?.argumentCount?.max).toBe(3);
      expect(filter.contentFilter?.argumentPatterns).toEqual(['*amount*']);
      expect(filter.contentFilter?.requiredDiagnosticEventTopics).toEqual(['Transfer']);
      expect(filter.contentFilter?.requireStateChanges).toBe(true);
    });

    it('should accept comprehensive subscription options', () => {
      const options: SubscriptionOptions = {
        startLedger: 1000,
        endLedger: 2000,
        autoReconnect: true,
        filter: {
          contractIds: ['contract1'],
          successful: true
        },
        response: {
          includeDiagnosticEvents: true,
          includeStateChanges: true
        }
      };

      expect(options).toBeDefined();
      expect(options.startLedger).toBe(1000);
      expect(options.endLedger).toBe(2000);
      expect(options.autoReconnect).toBe(true);
      expect(options.filter?.contractIds).toEqual(['contract1']);
      expect(options.filter?.successful).toBe(true);
      expect(options.response?.includeDiagnosticEvents).toBe(true);
      expect(options.response?.includeStateChanges).toBe(true);
    });
  });
});