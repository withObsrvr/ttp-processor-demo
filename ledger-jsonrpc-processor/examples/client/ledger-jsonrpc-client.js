/**
 * Ledger JSON-RPC Client for Node.js
 * This client provides methods to interact with the Ledger JSON-RPC service.
 */

const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

class LedgerJsonRpcClient {
  /**
   * Create a new LedgerJsonRpcClient
   * 
   * @param {string} serverAddress - The address of the Ledger JSON-RPC server (e.g., 'localhost:50053')
   * @param {Object} options - Optional configuration
   * @param {string} options.protoDir - Directory containing the proto files (default: '../protos')
   * @param {boolean} options.secure - Use secure connection (default: false)
   */
  constructor(serverAddress, options = {}) {
    this.serverAddress = serverAddress;
    this.options = Object.assign({
      protoDir: '../protos',
      secure: false
    }, options);
    
    this.client = null;
    this.initialized = false;
  }

  /**
   * Initialize the client by loading proto files and creating a gRPC client
   * 
   * @returns {Promise<void>}
   */
  async initialize() {
    if (this.initialized) return;

    // Define the proto file paths
    const serviceProtoPath = path.join(this.options.protoDir, 'ledger_jsonrpc_service/ledger_jsonrpc_service.proto');
    const eventProtoPath = path.join(this.options.protoDir, 'ingest/processors/ledger_jsonrpc/ledger_jsonrpc_event.proto');
    
    // Load the protos
    const packageDefinition = await protoLoader.load(
      [serviceProtoPath, eventProtoPath],
      {
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true,
        includeDirs: [this.options.protoDir]
      }
    );
    
    // Load the service proto
    const protoDescriptor = grpc.loadPackageDefinition(packageDefinition);
    const serviceProto = protoDescriptor.ledger_jsonrpc_service;
    
    // Create the client
    const credentials = this.options.secure 
      ? grpc.credentials.createSsl() 
      : grpc.credentials.createInsecure();
    
    this.client = new serviceProto.LedgerJsonRpcService(this.serverAddress, credentials);
    this.initialized = true;
  }

  /**
   * Get responses for a specific JSON-RPC method for a range of ledgers
   * 
   * @param {Object} options - Request options
   * @param {number} options.startLedger - The first ledger sequence to include
   * @param {number} options.endLedger - The last ledger sequence to include (0 for live streaming)
   * @param {string} options.method - The JSON-RPC method to execute
   * @param {Object} options.params - Parameters for the method
   * @param {function} onData - Callback for each ledger response
   * @param {function} onError - Callback for errors
   * @param {function} onEnd - Callback when stream ends
   * @returns {Object} - The stream object
   */
  getJsonRpcResponses(options, onData, onError, onEnd) {
    if (!this.initialized) {
      throw new Error('Client not initialized. Call initialize() first.');
    }

    // Convert params object to string key-value pairs
    const stringParams = {};
    if (options.params) {
      Object.keys(options.params).forEach(key => {
        const value = options.params[key];
        stringParams[key] = typeof value === 'string' 
          ? value 
          : JSON.stringify(value);
      });
    }

    const request = {
      start_ledger: options.startLedger,
      end_ledger: options.endLedger || 0,
      method: options.method,
      params: stringParams
    };

    const stream = this.client.GetJsonRpcResponses(request);
    
    stream.on('data', (response) => {
      onData(this._processJsonRpcEvent(response));
    });
    
    stream.on('error', (error) => {
      onError(error);
    });
    
    stream.on('end', () => {
      onEnd();
    });

    return stream;
  }

  /**
   * Execute a single JSON-RPC method
   * 
   * @param {Object} options - Request options
   * @param {string} options.id - Request ID
   * @param {string} options.method - Method to execute
   * @param {Object} options.params - Parameters for the method
   * @returns {Promise<Object>} - The JSON-RPC response
   */
  getJsonRpcMethod(options) {
    if (!this.initialized) {
      throw new Error('Client not initialized. Call initialize() first.');
    }

    return new Promise((resolve, reject) => {
      // Convert params to JSON bytes if provided
      let paramsJson = Buffer.from('{}');
      if (options.params) {
        paramsJson = Buffer.from(JSON.stringify(options.params));
      }

      const request = {
        jsonrpc: '2.0',
        id: options.id || Math.random().toString(36).substring(2, 15),
        method: options.method,
        params_json: paramsJson
      };

      this.client.GetJsonRpcMethod(request, (error, response) => {
        if (error) {
          reject(error);
          return;
        }
        
        resolve(this._processJsonRpcResponse(response));
      });
    });
  }

  /**
   * Process a JSON-RPC event into a more usable JavaScript object
   * 
   * @param {Object} event - The raw JSON-RPC event from the gRPC response
   * @returns {Object} - A processed JavaScript object
   * @private
   */
  _processJsonRpcEvent(event) {
    // Convert the protobuf timestamp to JavaScript Date
    const processedAt = event.processed_at ? 
      new Date(Number(event.processed_at.seconds) * 1000 + event.processed_at.nanos / 1000000) : 
      new Date();

    const ledgerClosedAt = event.ledger_meta && event.ledger_meta.closed_at ? 
      new Date(Number(event.ledger_meta.closed_at.seconds) * 1000 + event.ledger_meta.closed_at.nanos / 1000000) : 
      null;

    // Process the event into a more usable format
    return {
      requestContext: event.request_ctx ? {
        id: event.request_ctx.id,
        method: event.request_ctx.method,
        params: event.request_ctx.params ? event.request_ctx.params : {}
      } : null,
      
      response: this._processJsonRpcResponse(event.response),
      
      ledgerMeta: event.ledger_meta ? {
        sequence: event.ledger_meta.sequence,
        closedAt: ledgerClosedAt,
        hash: event.ledger_meta.hash
      } : null,
      
      processedAt: processedAt
    };
  }

  /**
   * Process a JSON-RPC response into a more usable JavaScript object
   * 
   * @param {Object} response - The raw JSON-RPC response from the gRPC response
   * @returns {Object} - A processed JavaScript object
   * @private
   */
  _processJsonRpcResponse(response) {
    if (!response) return null;

    const result = {
      jsonrpc: response.jsonrpc,
      id: response.id
    };

    // Handle result or error
    if (response.result) {
      result.result = response.result;
    } else if (response.error) {
      result.error = {
        code: response.error.code,
        message: response.error.message
      };
      
      if (response.error.data) {
        result.error.data = response.error.data;
      }
    }

    return result;
  }

  /**
   * Close the client and release resources
   */
  close() {
    if (this.client) {
      grpc.closeClient(this.client);
      this.client = null;
      this.initialized = false;
    }
  }
}

module.exports = LedgerJsonRpcClient;