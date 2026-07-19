# Authoritative Contract Interface and WASM API

## Behavior

`GET /api/v1/silver/contracts/{contract_id}/interface` resolves the current
contract instance through Stellar RPC, follows its active executable hash,
loads or fetches the immutable code artifact, validates `SHA-256(WASM)` against
the ledger hash, and decodes these WASM custom sections:

- `contractspecv0`: functions, structs, unions, enums, errors, and events
- `contractmetav0`: ordered key/value contract metadata
- `contractenvmetav0`: environment interface version

The JSON response is canonical. `?format=rust` returns a Rust-like rendering
for explorer display. `observed_functions` is explicitly separate because
invocation history cannot prove the complete declared interface. For WASM
contracts, `executable.wasm_size_bytes` exposes the verified active artifact's
size without requiring clients to download it first.

`GET /api/v1/silver/contracts/{contract_id}/wasm` streams the verified active
code as `application/wasm`. It includes the active hash as an ETag and supports
conditional requests. The contract-ID route re-resolves the current executable
before serving, so upgrades cannot leave it pinned to deployment-time code.

## Persistence

Configure a durable cache directory:

```yaml
contract_artifacts:
  cache_directory: "/var/lib/stellar-query-api/contract-artifacts"
  max_wasm_bytes: 4194304
```

Each immutable code hash produces `<hash>.wasm` and `<hash>.json`. Both are
written atomically, and the bytecode is hash-validated again on cache reads.
Corrupt or incomplete entries are fetched again from the authoritative source.
The current contract-to-hash mapping is deliberately not cached: it is resolved
from the live contract instance so contract upgrades are visible immediately.

## Stellar Asset Contract

The built-in Stellar Asset Contract executable has no uploaded WASM. The
interface route returns its canonical protocol interface with
`executable.type=stellar_asset` and `code_source=protocol_builtin`. The WASM
route returns `404` with an explicit explanation.

## Failure Semantics

- `400`: malformed contract ID or unsupported response format
- `404`: contract/current code missing, or WASM requested for a built-in SAC
- `503`: RPC, decoding, hash validation, or artifact persistence unavailable

The service never falls back to observed calls as a declared interface.
