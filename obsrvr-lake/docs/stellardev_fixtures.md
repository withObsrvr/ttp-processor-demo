# Real-World Fixtures — stellar dev build plan

Smoke/demo fixtures for validating tickets against **real Stellar data, no mocks** (see Ticket 0).
Record expected high-level facts only, not brittle full-response snapshots.

> ✅ **Ticket 0 acceptance MET (testnet, 2026-06-21).** Every buildable ticket (2–5) has a real fixture, and the testnet fixtures are confirmed present in the **live** obsrvr-lake testnet pipeline (not just on-chain): Ticket 2 storage+TTL verified; Ticket 4 wallet `GBDCULE5` returns balances; Ticket 3 `GBTORQK3` returns activity. The **only** outstanding fixture is the mainnet sorobandomains registry (Ticket 2 *demo*), which is blocked on mainnet backfill — a separate demo track, NOT a Ticket 0 blocker.

---

## sorobandomains (Ticket 2 — live contract state)

**Network: Mainnet (Public Network).** dApp: app.sorobandomains.org

| Role | Contract ID | Use for |
|---|---|---|
| **Registry** (primary) | `CC75Z72OCE667WVPQOROIWDAGBOXFNJ4VQONQEURL74EYIDLWA4F7FEN` | Listing all live registered domains by contract ID — the core Ticket 2 target |
| NFD (domain NFT) | `CADCRH6BW3MIZBBE7JOVKROR2GBEG64TJDT5Y3EX3OOIWRDZOOT5XUHD` | Secondary: NFT-backed domain entries |
| Legacy registry | `CATRNPHYKNXAPNLHEYH55REB6YSAJLGCPA4YM6L3WUKSZOPI77M2UMKI` | Older entries; useful for cold-path / historical coverage |
| Legacy Key-Value DB | `CDH2T2CBGFPFNVRWFK4XJIRP6VOWSVTSDCRBCJ2TEIO22GADQP6RG3Y6` | Legacy storage shape |
| Legacy Reverse Registrar | `CCAU556HKCUXF4LBPUV2KROU5FYGC6227G2LD3SVQ6GR6654IVTO2GBO` | Reverse lookups |

**Expected high-level facts (not full snapshots):**
- Registry has live domain entries that can be enumerated via `GET /silver/contracts/{id}/storage`.
- The set returned in `live_only` mode must contain **no deleted / no TTL-expired** entries.
- Legacy registry is a good target for the **cold/backfill path** test required by Ticket 2 acceptance (older data has flushed out of hot).

> ⚠️ **BLOCKED as a live fixture (status, 2026-06-21):** these contracts are **mainnet**, mainnet ingestion is **not complete** (testnet is the only pipeline running), and **no testnet sorobandomains deployment was found**. So none of the above can be queried through the live pipeline right now.

**What this means for Ticket 2 — decouple the code fix from the sorobandomains demo:**
- The Ticket 2 bug (no delete handling + wrong TTL scoping) is **real and code-verified independently of any fixture** — it does not need sorobandomains to fix or to test. Build and merge the fix now, validated on **testnet** against a real contract that exercises create/update/delete/TTL transitions (NOT mocks — see `CLAUDE.md` no-mock rule).
- **Testnet validation target (FOUND — see "Ticket 2 delete fixture" below).** A SAC token (e.g. CETES `CC72F57...` below) additionally gives TTL-on-balance coverage.
- The **sorobandomains enumeration demo stellar dev specifically asked for is mainnet-gated.** It cannot be shown to him until mainnet ingestion lands. Treat "finish mainnet ingestion" as a prerequisite for that demo, separate from shipping the Ticket 2 fix. Concretely, mainnet is mid-build: **Bronze is under active gap-repair backfill** and **Silver was reset to zero and is being rebuilt from scratch via `silver-history-loader`** — so the registry contract's storage won't be queryable on mainnet until both complete through the relevant ledger range. Readiness checks + access in the runbook: `obsrvr-agent-vault/runbooks/obsrvr-lake-mainnet-access.md` (Bronze gate `verify-bronze-silver-readiness-direct-go.sh`; Silver status `nomad job status silver-history-loader`).
- Gateway path is network-scoped: today `/lake/v1/testnet/api/v1/silver/contracts/{id}/storage`; switch to `/mainnet/` once ingestion is live.

### Ticket 2 delete fixture (testnet — found via nebu `contract-state` scan)

**Network: Testnet.** Provenance: `contract-state` processor over ledgers `3207000..3207050`. (Note: the nebu **MCP** tool is mainnet-pinned and returns 0 here — must scan via CLI with `--network testnet`.)

Contract `CCJQB4EEQLBL7RHIPYMYG26ZT2QRKEYNGVWWL2EPZCECFI6GZGNXMIEX` (the `trade` contract already listed under Ticket 5) does real PERSISTENT-storage deletes — 10 deletes in this 50-ledger window. Best single-key lifecycle for the test assertion:

| Key hash | Op | Ledger | Tx |
|---|---|---|---|
| `eb4e8c427fd0d98f4fe4b5084f473649a73bb28de4a35e5c457e15b552a82f66` | **create** | 3207019 | `67c208bad11dc0429b7a8e0d51c909e1ba68a32a24d5f9ba6cd23e25b9b3096b` |
| `eb4e8c427fd0d98f4fe4b5084f473649a73bb28de4a35e5c457e15b552a82f66` | **delete** | 3207025 | `3adbe05f1e90ea17f9ce6744abb2cd6a33fd0d96fad431914cb4ebd4432bbf2c` |

**Ticket 2 assertions this enables (all PERSISTENT, durability matters):**
- After ingesting through ledger **3207025**, key `eb4e8c42…` MUST be **absent** from `contract_data_current` and from `GET /silver/contracts/CCJQB4EE…/storage` in `live_only` mode. (Today's upsert-only transformer would wrongly keep it — this is the bug.)
- Long-lived key `a7911cdfdc28…` gets 22 updates across 3207000–3207048 with no delete → MUST remain live exactly once (no duplicate rows, latest value wins).
- Run the same assertions against the **cold/backfill** path, not just hot.

**Expected high-level facts (not full snapshots):** in `[3207000, 3207050]` this contract emits ~112 creates / 347 updates / 10 deletes.

Reproduce:

```bash
nebu fetch --mode rpc --network testnet --rpc-url https://soroban-testnet.stellar.org \
  3207000 3207050 \
  | contract-state --quiet --network testnet \
  | jq -c 'select(.operation=="delete")'
```

### Ticket 2 TTL / expiry fixture (testnet — found via nebu `ttl-tracker`)

**Network: Testnet. Current testnet head ≈ 3207706 (testnet was recently reset — short history available).**

TEMPORARY-durability contract-data entries have short TTLs and get **evicted** after expiry — they are the natural "must drop from `live_only`" fixtures. 7 live TEMPORARY entries in `[3207000, 3207050]`:

| Contract | Key hash | Note |
|---|---|---|
| `CBIELTK6YBZJU5UP2WWQEUCYKLPU6AUNZ2BQ4WWFEIE3USCIHMXQDAMA` | `e26d62185e6f0201e46cd85d7c30b9e2ffa6998052ccafbef97257538f9ae024` | This is the testnet **USDC SAC** already listed under Ticket 5 — convenient dual-purpose fixture |
| `CCDJSB3HDLNHY4PWQLGBDXOR7654HA2V7QO7RAHFGTT6VEUHJQN27WZJ` | `5df6bf9d…`, `9eb31e6d…`, `ac497d0f…`, `ca09cdd8…` | 4 temporaries on one contract |
| `CAKVUHDKKEG6SYUAVMQMDRMUGCNQJS74BP45NNYS7Y2TTYUMYFSLA7EU` | `1d112b8e…` | |
| `CCYOZJCOPG34LLQQ7N24YXBM7LL62R7ONMZ3G6WZAAYPB5OYKOMJRN63` | `9d05bde9…` | |

**Ticket 2 TTL assertion:** query the contract's storage now (entry present), then again after testnet head advances past the entry's expiration — the TEMPORARY entry MUST be excluded from `live_only` results. Look up the exact `expirationLedger` per key with `ttl-tracker` at test time (it moves as TTLs are extended). The soonest-expiring TTL entry observed in-window was `expirationLedger=3207730` (only ~24 ledgers past head).

> ✅ **TTL-join concern RESOLVED (verified 2026-06-21) — nebu quirk only, production join works.** The nebu finding (zero overlap between `contract-state.ledgerKeyHash` and `ttl-tracker.keyHash`, 0/114 creates) does **not** replicate in the real silver pipeline. Verified empirically: `GET /api/v1/silver/contracts/{id}/storage` on testnet returns **populated** `live_until_ledger_seq`, `ttl_remaining`, and `expired` on every entry (e.g. CCJQB4EE entries with `live_until_ledger_seq: 3328359`, `ttl_remaining: 120959`) — those fields come solely from the `LEFT JOIN ttl_current ON cd.key_hash = t.key_hash`, so a non-null result proves the hashes match. The ingester derives both from `SHA256(MarshalBinary(full xdr.LedgerKey union))` (`LedgerEntryToLedgerKeyHash`), which equals the protocol definition of `TTLEntry.KeyHash`. The nebu `contract-state` processor hashes differently (likely the inner `LedgerKeyContractData` without the union discriminant) — a nebu-standalone bug, irrelevant to silver.
>
> **Two consequences for Ticket 2 Task 2:**
> - The `(contract_id, key_hash)` vs `key_hash`-alone concern is **moot** — `key_hash` is `SHA256` of the *full* `LedgerKey` (which embeds contract_id + durability), so it is globally unique and joining on `key_hash` alone is already correct.
> - The join already computes `expired` per entry; the **real remaining work is filtering `expired` out by default in `live_only` mode** (today it's returned but not filtered). That part of the audit still stands.

---

## Tickets 3–5 fixtures — Mainnet candidates (for when mainnet ingestion lands)

*Mainnet is not yet ingesting — use the **Testnet** fixtures below for current validation. These mainnet candidates are pre-staged for the eventual mainnet cutover.*

**Network: Mainnet (Public Network).** Provenance: bounded nebu archive scan over ledgers `62080000..62080200` using `token-transfer`, `account-effects`, and `contract-invocation`.

### Ticket 3 (relationships)

Two addresses with repeated shared transfer history in the scanned range:

| Role | Address | Evidence |
|---|---|---|
| Sender | `GADUP4XUZ5PUVDRX6DXZRTSVIIZW3CXOQPVGARBKBUVMPZNKG7DE4W4M` | 917 transfers to receiver in range |
| Receiver | `GBJAMW27KDP6AUPFVFRM7TUE3RGGBGJCNXE5V4PJ3JLRYCXQ6VNNWTBO` | Received `AIus` transfers from sender |

Example tx: `9f7efd76b6815ac08dd520a3b5456c38e5109993d452b5979151855fc0c83998` at ledger `62080000`.

### Ticket 4 (unified balances)

Wallet candidate with XLM activity, classic trustline effects, and Soroban token transfer activity:

| Address | Evidence |
|---|---|
| `GADUP4XUZ5PUVDRX6DXZRTSVIIZW3CXOQPVGARBKBUVMPZNKG7DE4W4M` | `1257` trustline effects, `2016` XLM transfer events, `7869` Soroban token transfer events in range |

Example evidence tx: `9f7efd76b6815ac08dd520a3b5456c38e5109993d452b5979151855fc0c83998` at ledger `62080000` includes trustline update, XLM, and Soroban token transfer activity.

### Ticket 5 (history)

Account candidate with both classic and Soroban history in the scanned range:

| Address | Evidence |
|---|---|
| `GDSQAG3G2GAJKVOFLQ57QM62DPEPP5C44B4K6OWJWZ63RGUYRNU27BTP` | `32` classic account-effect events, `6` token events, and `1` contract invocation in range |

Example classic tx: `539ffee03c14c141a692b5cd16eb62457488da36796769c0f5517360341581af` at ledger `62080116` (`offer_created`).
Example Soroban tx: `47bd6ea387c0f63ed6d2313bb6c301be230218760bd73a1ac7d65ceb835edeae` at ledger `62080127`, invoking contract `CCPGFQUTSEHDIQODRE3GJDNE64A35HZ32L7LPDN7GXOCIYNBJSMS6V6B` function `swap_chained`.

Asset/contract candidate for balance-timeseries tests:

| Asset | Contract ID | Issuer | Evidence |
|---|---|---|---|
| USDC | `CCW67TSZV3SSS2HXMBQ5JFGCKJNXKZM7UQUWUZPUTHXSTZLEO7SJMI75` | `GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN` | `6460` transfer events in range |

Reproduce scan:

```bash
nebu fetch --mode archive \
  --datastore-type S3 \
  --bucket-path "aws-public-blockchain/v1.1/stellar/ledgers/pubnet" \
  --region us-east-2 \
  62080000 62080200 > /tmp/nebu-fixtures.xdr

cat /tmp/nebu-fixtures.xdr | token-transfer --quiet > /tmp/token.jsonl
cat /tmp/nebu-fixtures.xdr | account-effects --quiet > /tmp/effects.jsonl
cat /tmp/nebu-fixtures.xdr | contract-invocation --quiet > /tmp/invocation.jsonl
```

---

## Tickets 3–5 fixtures — Testnet (ACTIVE — use these now)

*Confirmed present in the live obsrvr-lake testnet pipeline (see Ticket 0 status at top). This is the validation set for current work.*

**Network: Testnet.** Provenance: bounded nebu RPC scan over ledgers `3207000..3207050` using `https://soroban-testnet.stellar.org` with `token-transfer`, `account-effects`, and `contract-invocation`.

### Ticket 3 (relationships)

Two addresses with repeated shared transfer history in the scanned range:

| Role | Address | Evidence |
|---|---|---|
| A | `GBTORQK3ZR3RPJF4WTTSH5KVDOAZ4BJI7PD2ECLSBDNHRG4ICNC4JJZV` | 9 XLM transfers to B |
| B | `GB5FCYPSK4ET44OVBXLJHWFW5LNG3ZLPUFSJTJBCGIM43JIU4RGYRLCH` | 9 XLM transfers back to A |

Example A → B tx: `0eb7ae2ec92cfd6350db651d576d4a0951c97bc684c2a53d6c4d3c34fab87789` at ledger `3207008`.
Example B → A tx: `044dee7f84a866f2cd07483a897ab0aa875fed8f92f715885359042c3056da42` at ledger `3207008`.

### Ticket 4 (unified balances)

Wallet candidate with XLM activity, classic trustline effects, Soroban token transfers, and contract invocations:

| Address | Evidence |
|---|---|
| `GBDCULE53LUPK4XHUCXBI35MAZFQHENMZ3JRKAJS2PPYBV646M6XKVHG` | `19` classic account-effect events, `7` XLM transfer events, `19` Soroban token transfer events, and `23` contract invocations in range |

Example evidence tx: `468c5df6d6156be0ead992a834d4b6c412d0bb4c69f8fd760713ec796ab31ba2` at ledger `3207000` includes trustline update, `CETES` Soroban token activity, and contract invocation.

### Ticket 5 (history)

Account candidate with full classic + Soroban history in the scanned range:

| Address | Evidence |
|---|---|
| `GBDCULE53LUPK4XHUCXBI35MAZFQHENMZ3JRKAJS2PPYBV646M6XKVHG` | `19` classic account-effect events, `7` XLM transfer events, `19` Soroban token transfer events, and `23` contract invocations in range |

Example classic/Soroban tx: `468c5df6d6156be0ead992a834d4b6c412d0bb4c69f8fd760713ec796ab31ba2` at ledger `3207000`, invoking contract `CCJQB4EEQLBL7RHIPYMYG26ZT2QRKEYNGVWWL2EPZCECFI6GZGNXMIEX` function `trade`.

Asset/contract candidates for balance-timeseries tests:

| Asset | Contract ID | Issuer | Evidence |
|---|---|---|---|
| CETES | `CC72F57YTPX76HAA64JQOEGHQAPSADQWSY5DWVBR66JINPFDLNCQYHIC` | `GC3CW7EDYRTWQ635VDIGY6S4ZUF5L6TQ7AA4MWS7LEQDBLUSZXV7UPS4` | `10` transfer events in range |
| USDC | `CBIELTK6YBZJU5UP2WWQEUCYKLPU6AUNZ2BQ4WWFEIE3USCIHMXQDAMA` | `GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5` | `6` transfer events in range |

Reproduce testnet scan:

```bash
nebu fetch --mode rpc \
  --network testnet \
  --rpc-url https://soroban-testnet.stellar.org \
  3207000 3207050 > /tmp/nebu-testnet-fixtures.xdr

cat /tmp/nebu-testnet-fixtures.xdr | token-transfer --quiet --network testnet > /tmp/testnet-token.jsonl
cat /tmp/nebu-testnet-fixtures.xdr | account-effects --quiet --network testnet > /tmp/testnet-effects.jsonl
cat /tmp/nebu-testnet-fixtures.xdr | contract-invocation --quiet --network testnet > /tmp/testnet-invocation.jsonl
```
