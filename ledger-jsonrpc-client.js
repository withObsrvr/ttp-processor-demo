const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

class LedgerJsonRpcClient {
  constructor(serverAddress = 'localhost:50051') {
    this.serverAddress = serverAddress;
    this.client = null;
    this.initialized = false;
  }

  /**
   * Initialize the client by loading the proto definitions
   */
  async initialize() {
    if (this.initialized) return;

    const PROTO_PATH = path.join(__dirname, 'ledger-jsonrpc-processor/protos/ledger_jsonrpc_service/ledger_jsonrpc_service.proto');
    
    // Additional paths for imports in the proto files
    const includeDirs = [
      path.join(__dirname, 'ledger-jsonrpc-processor/protos'),
      // Add path to google/protobuf protos which come with the protobufjs package
      path.dirname(require.resolve('google-protobuf/package.json'))
    ];

    const packageDefinition = await protoLoader.load(PROTO_PATH, {
      keepCase: true,
      longs: String,
      enums: String,
      defaults: true,
      oneofs: true,
      includeDirs
    });

    const protoDescriptor = grpc.loadPackageDefinition(packageDefinition);
    const ledgerJsonRpcService = protoDescriptor.ledger_jsonrpc_service;

    this.client = new ledgerJsonRpcService.LedgerJsonRpcService(
      this.serverAddress, 
      grpc.credentials.createInsecure()
    );

    this.initialized = true;
  }

  /**
   * Stream JSON-RPC responses for a range of ledgers
   * 
   * @param {Object} options
   * @param {number} options.startLedger - First ledger sequence number to include
   * @param {number} options.endLedger - Last ledger sequence number (0 for continuous)
   * @param {string} options.method - JSON-RPC method to execute
   * @param {Object} options.params - Parameters for the method
   * @param {function} onEvent - Callback for each event received
   * @param {function} onError - Callback for errors
   * @param {function} onEnd - Callback when stream ends (only with finite range)
   * @returns {Object} The call object that can be used to cancel the stream
   */
  streamJsonRpcResponses(options, onEvent, onError, onEnd) {
    if (!this.initialized) {
      throw new Error('Client not initialized. Call initialize() first.');
    }

    const { startLedger, endLedger = 0, method, params = {} } = options;

    const request = {
      start_ledger: startLedger,
      end_ledger: endLedger,
      method: method,
      params: params
    };

    const call = this.client.GetJsonRpcResponses(request);
    
    call.on('data', (event) => {
      // Convert protobuf response to a plain JavaScript object
      const response = this._normalizeEvent(event);
      onEvent(response);
    });

    call.on('error', (error) => {
      if (onError) onError(error);
    });

    call.on('end', () => {
      if (onEnd) onEnd();
    });

    // Return the call object so the caller can cancel if needed
    return call;
  }

  /**
   * Execute a single JSON-RPC method
   * 
   * @param {Object} options
   * @param {string} options.method - JSON-RPC method to execute
   * @param {Object} options.params - Method parameters
   * @param {string} options.id - Request ID (defaults to random UUID)
   * @returns {Promise<Object>} The JSON-RPC response
   */
  async executeJsonRpcMethod(options) {
    if (!this.initialized) {
      throw new Error('Client not initialized. Call initialize() first.');
    }

    const { method, params = {}, id = this._generateRequestId() } = options;

    // Convert params to JSON string and then to Buffer
    const paramsJson = Buffer.from(JSON.stringify(params));

    const request = {
      jsonrpc: '2.0',
      id: id,
      method: method,
      params_json: paramsJson
    };

    return new Promise((resolve, reject) => {
      this.client.GetJsonRpcMethod(request, (error, response) => {
        if (error) {
          reject(error);
          return;
        }

        resolve(this._normalizeResponse(response));
      });
    });
  }

  /**
   * Normalize the LedgerJsonRpcEvent protobuf message to a plain JavaScript object
   */
  _normalizeEvent(event) {
    const result = {
      requestContext: {
        id: event.request_ctx?.id,
        method: event.request_ctx?.method,
        params: event.request_ctx?.params ? this._convertStructToObject(event.request_ctx.params) : {}
      },
      response: this._normalizeResponse(event.response),
      ledgerMeta: {
        sequence: event.ledger_meta?.sequence,
        closedAt: event.ledger_meta?.closed_at ? new Date(event.ledger_meta.closed_at.seconds * 1000) : null,
        hash: event.ledger_meta?.hash
      },
      processedAt: event.processed_at ? new Date(event.processed_at.seconds * 1000) : null
    };

    return result;
  }

  /**
   * Normalize the JsonRpcResponse protobuf message to a plain JavaScript object
   */
  _normalizeResponse(response) {
    if (!response) return null;

    const result = {
      jsonrpc: response.jsonrpc,
      id: response.id
    };

    // Handle oneofs for result or error
    if (response.result) {
      result.result = this._convertStructToObject(response.result);
    } else if (response.error) {
      result.error = {
        code: response.error.code,
        message: response.error.message
      };

      if (response.error.data) {
        result.error.data = this._convertStructToObject(response.error.data);
      }
    }

    return result;
  }

  /**
   * Convert a protobuf Struct to a plain JavaScript object
   */
  _convertStructToObject(struct) {
    if (!struct || !struct.fields) return {};

    const result = {};
    for (const [key, value] of Object.entries(struct.fields)) {
      if (value.null_value !== undefined) {
        result[key] = null;
      } else if (value.number_value !== undefined) {
        result[key] = value.number_value;
      } else if (value.string_value !== undefined) {
        result[key] = value.string_value;
      } else if (value.bool_value !== undefined) {
        result[key] = value.bool_value;
      } else if (value.struct_value !== undefined) {
        result[key] = this._convertStructToObject(value.struct_value);
      } else if (value.list_value !== undefined) {
        result[key] = value.list_value.values.map(v => {
          if (v.null_value !== undefined) return null;
          if (v.number_value !== undefined) return v.number_value;
          if (v.string_value !== undefined) return v.string_value;
          if (v.bool_value !== undefined) return v.bool_value;
          if (v.struct_value !== undefined) return this._convertStructToObject(v.struct_value);
          if (v.list_value !== undefined) {
            // Recursive handling for nested lists not implemented for simplicity
            return v.list_value.values;
          }
          return null;
        });
      }
    }
    return result;
  }

  /**
   * Generate a random request ID
   */
  _generateRequestId() {
    return Math.random().toString(36).substring(2, 15) + 
           Math.random().toString(36).substring(2, 15);
  }

  /**
   * Close the client connection
   */
  close() {
    if (this.client) {
      grpc.closeClient(this.client);
      this.client = null;
      this.initialized = false;
    }
  }
}

module.exports = { LedgerJsonRpcClient };