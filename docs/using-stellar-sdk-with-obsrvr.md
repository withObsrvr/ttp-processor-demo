# Using Stellar SDK with OBSRVR RPC

For raw Stellar ledger data and standard RPC methods, use the official Stellar SDK pointed at OBSRVR RPC endpoints.

## Installation

```bash
npm install @stellar/stellar-sdk
```

## Configuration

### Testnet

```typescript
import { SorobanRpc } from '@stellar/stellar-sdk';

const server = new SorobanRpc.Server('https://rpc-testnet.nodeswithobsrvr.co', {
  headers: {
    'Authorization': 'Api-Key HonqtGIC.HpwAVdiszK8LMLZv0y4nvuwRp2oPytQ4'
  }
});
```

### Mainnet

```typescript
const server = new SorobanRpc.Server('https://rpc.nodeswithobsrvr.co', {
  headers: {
    'Authorization': 'Api-Key YOUR_API_KEY_HERE'
  }
});
```

## Usage Examples

### Get Latest Ledger

```typescript
const latestLedger = await server.getLatestLedger();
console.log('Latest ledger sequence:', latestLedger.sequence);
console.log('Ledger hash:', latestLedger.hash);
console.log('Protocol version:', latestLedger.protocolVersion);
```

### Get Specific Ledger

```typescript
const ledger = await server.getLedger(1000000);
console.log('Ledger 1000000:', ledger);
```

### Get Transaction

```typescript
const tx = await server.getTransaction('abc123...');
console.log('Transaction status:', tx.status);
console.log('Result XDR:', tx.resultXdr);
```

### Get Contract Events

```typescript
import { Contract } from '@stellar/stellar-sdk';

const events = await server.getEvents({
  startLedger: 1000000,
  filters: [
    {
      type: 'contract',
      contractIds: ['CBGTW...']
    }
  ],
  pagination: { limit: 100 }
});

console.log('Events:', events.events);
```

### Simulate Transaction

```typescript
import { Transaction, Networks, Account, Keypair } from '@stellar/stellar-sdk';

// Build your transaction
const account = await server.getAccount(keypair.publicKey());
const transaction = new TransactionBuilder(account, {
  fee: '100',
  networkPassphrase: Networks.TESTNET
})
  .addOperation(...)
  .setTimeout(30)
  .build();

// Simulate before submitting
const simulation = await server.simulateTransaction(transaction);
console.log('Simulation result:', simulation);

if (simulation.error) {
  console.error('Simulation failed:', simulation.error);
} else {
  // Sign and submit
  transaction.sign(keypair);
  const result = await server.sendTransaction(transaction);
  console.log('Transaction hash:', result.hash);
}
```

## Full Example: Stream Events

```typescript
import { SorobanRpc } from '@stellar/stellar-sdk';

const server = new SorobanRpc.Server('https://rpc-testnet.nodeswithobsrvr.co', {
  headers: {
    'Authorization': 'Api-Key HonqtGIC.HpwAVdiszK8LMLZv0y4nvuwRp2oPytQ4'
  }
});

async function streamEvents(contractId: string, startLedger: number) {
  let cursor = undefined;

  while (true) {
    const response = await server.getEvents({
      startLedger: cursor ? undefined : startLedger,
      filters: [{ type: 'contract', contractIds: [contractId] }],
      cursor,
      pagination: { limit: 100 }
    });

    for (const event of response.events) {
      console.log('Event:', event);
    }

    if (response.events.length === 0) {
      // Wait before polling again
      await new Promise(resolve => setTimeout(resolve, 5000));
      continue;
    }

    cursor = response.latestLedger;
  }
}

streamEvents('CBGTW...', 1000000);
```

## API Reference

For complete Stellar RPC documentation, see:
- [Stellar RPC Methods](https://developers.stellar.org/network/soroban-rpc/api-reference)
- [Stellar SDK Documentation](https://stellar.github.io/js-stellar-sdk/)

## When to Use OBSRVR Client SDK Instead

For **processed, parsed data** (token transfers, contract events, analytics), use the OBSRVR Client SDK:

```typescript
// Coming soon: @obsrvr/client
import { ObsrvrClient } from '@obsrvr/client';

const client = new ObsrvrClient({
  apiKey: 'xxx',
  network: 'testnet'
});

// Get clean, parsed token transfers (no XDR parsing required)
for await (const event of client.streamTokenTransfers({ startLedger: 1000000 })) {
  console.log(`${event.from} → ${event.to}: ${event.amount} ${event.asset.code}`);
}
```

See [OBSRVR Client SDK documentation](../shape-up/OBSRVR_CLIENT_SDK_REVISED.md) for more details.

## Support

- OBSRVR RPC Issues: https://github.com/withObsrvr/obsrvr-rpc/issues
- Stellar SDK Issues: https://github.com/stellar/js-stellar-sdk/issues
- Documentation: https://developers.stellar.org
