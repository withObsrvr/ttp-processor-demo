// Example Node.js consumer for Contract Invocation Processor
// This is a placeholder implementation showing how to connect and consume events

const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

// Load the proto file
const PROTO_PATH = path.join(__dirname, '../protos/contract_invocation_service/contract_invocation_service.proto');

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
    includeDirs: [path.join(__dirname, '../protos')]
});

const contractInvocationProto = grpc.loadPackageDefinition(packageDefinition).contract_invocation_service;

class ContractInvocationClient {
    constructor(serverAddress = 'localhost:50054') {
        this.client = new contractInvocationProto.ContractInvocationService(
            serverAddress,
            grpc.credentials.createInsecure()
        );
    }

    async getContractInvocations(options = {}) {
        const request = {
            start_ledger: options.startLedger || 1000000,
            end_ledger: options.endLedger || 0, // 0 = live stream
            contract_ids: options.contractIds || [],
            function_names: options.functionNames || [],
            invoking_accounts: options.invokingAccounts || [],
            successful_only: options.successfulOnly || false,
            type_filter: options.typeFilter || 0 // ALL
        };

        console.log('Requesting contract invocations:', request);

        const stream = this.client.getContractInvocations(request);

        stream.on('data', (event) => {
            console.log('Received contract invocation event:');
            console.log(JSON.stringify(event, null, 2));
        });

        stream.on('error', (error) => {
            console.error('Stream error:', error);
        });

        stream.on('end', () => {
            console.log('Stream ended');
        });

        return stream;
    }
}

// Example usage
async function main() {
    const client = new ContractInvocationClient();

    try {
        // Example 1: Stream all contract invocations from ledger 1000000
        console.log('=== Example 1: All contract invocations ===');
        await client.getContractInvocations({
            startLedger: 1000000,
            endLedger: 1000010
        });

        // Example 2: Filter by contract ID
        console.log('\n=== Example 2: Specific contract only ===');
        await client.getContractInvocations({
            startLedger: 1000000,
            endLedger: 1000010,
            contractIds: ['CBIELTK6YBZJU5UP2WWQEUCYKLPU6AUNZ2BQ4WWFEIE3USCIHMXQDAMA']
        });

        // Example 3: Filter by function name
        console.log('\n=== Example 3: Specific function only ===');
        await client.getContractInvocations({
            startLedger: 1000000,
            endLedger: 1000010,
            functionNames: ['transfer', 'mint']
        });

    } catch (error) {
        console.error('Failed to connect to contract invocation processor:', error);
        console.log('\nNote: This example requires the contract invocation processor to be running.');
        console.log('Start the processor with: make run');
    }
}

if (require.main === module) {
    main().catch(console.error);
}

module.exports = { ContractInvocationClient };