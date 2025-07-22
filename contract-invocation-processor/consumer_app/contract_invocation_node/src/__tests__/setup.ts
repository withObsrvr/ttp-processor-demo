/**
 * @fileoverview Jest test setup and configuration
 */

// Increase test timeout for integration tests
jest.setTimeout(15000);

// Mock grpc module for unit tests
jest.mock('@grpc/grpc-js', () => ({
  credentials: {
    createInsecure: jest.fn(() => ({})),
    createSsl: jest.fn(() => ({}))
  },
  status: {
    OK: 0,
    CANCELLED: 1,
    UNKNOWN: 2,
    INVALID_ARGUMENT: 3,
    DEADLINE_EXCEEDED: 4,
    NOT_FOUND: 5,
    ALREADY_EXISTS: 6,
    PERMISSION_DENIED: 7,
    RESOURCE_EXHAUSTED: 8,
    FAILED_PRECONDITION: 9,
    ABORTED: 10,
    OUT_OF_RANGE: 11,
    UNIMPLEMENTED: 12,
    INTERNAL: 13,
    UNAVAILABLE: 14,
    DATA_LOSS: 15,
    UNAUTHENTICATED: 16
  }
}));

// Global test utilities
(global as any).testUtils = {
  createMockEvent: () => {
    // Mock ContractInvocationEvent for testing
    return {
      getLedgerSequence: () => 12345,
      getTransactionHash: () => 'mock-tx-hash',
      getContractId: () => 'mock-contract-id',
      getSuccessful: () => true,
      getTimestamp: () => Math.floor(Date.now() / 1000),
      getContractCall: () => null,
      getCreateContract: () => null,
      getUploadWasm: () => null
    };
  }
};

// Console log suppression for tests
const originalConsoleLog = console.log;
const originalConsoleError = console.error;

beforeEach(() => {
  // Suppress console output during tests unless explicitly testing it
  if (!process.env.JEST_VERBOSE) {
    console.log = jest.fn();
    console.error = jest.fn();
  }
});

afterEach(() => {
  // Restore console output
  console.log = originalConsoleLog;
  console.error = originalConsoleError;
});

// Empty test to satisfy Jest's requirement
describe('Setup', () => {
  it('should configure test environment', () => {
    expect(true).toBe(true);
  });
});