# Smart Account Indexer Alignment — Shape (2026-07-08)

## Problem

Passkey wallet login needs a reverse lookup: *"which smart account contracts can this credential (or signer address) operate?"* A passkey can sign for many contracts, secondary signers have no deterministic contract address, and the contract exposes no iterator over active rule IDs. kalepail's smart-account-kit indexer (Goldsky → Postgres views → Cloudflare Worker, local reference `/home/tillman/Documents/smart-account-kit` at `6f1c035`) is the best reference model for the current OpenZeppelin account surface: 11 smart-account event types parsed into current authorization state, served as credential/address → contracts lookups and per-contract rule state for the SDK.

Obsrvr-lake today has only heuristic smart-wallet classification. The query detector (`stellar-query-api/go/wallet_openzeppelin.go`), the transformer-side mirror (`silver-realtime-transformer/go/transformer.go`), the smart-wallet list heuristic (`stellar-query-api/go/handlers_smart_wallets.go`), and the detail/semantic helpers still lean on function names and storage substrings. They scrape signer-ish strings from storage text and parse **none** of the event vocabulary. Their OpenZeppelin function lists are part guardian-era (`add_guardian`, `recover`, `set_threshold`) and miss current context-rule/policy admin functions. There is no credential → contracts lookup anywhere in the API — the highest-value piece.

Everything needed is already ingested in the v3 Bronze event stream: `contract_events_stream_v1` carries decoded topics as positional columns (`topic0_decoded`..`topic3_decoded`), decoded JSON payloads (`data_decoded`), `successful`, `in_successful_contract_call`, and event coordinates (`ledger_sequence`, `transaction_hash`, `operation_index`, `event_index`). Older schema files only show `topics_decoded`, so implementation should explicitly target the live/v3 schema and fall back to `topics_decoded::jsonb->>0` only where positional topic columns are unavailable.

## Appetite

**1 week.** Detection fix is ~a day; the event-state transformer and Obsrvr-native query surface are the rest. Mainnet backfill is explicitly out (separate small bet once testnet proves the model).

## Solution (fat-marker sketch)

```
bronze contract_events_stream_v1 (event_type='contract',
successful=true where present, topic0_decoded ∈ 11 smart-account event types)
        │  new transform job in silver-realtime-transformer
        ▼
silver: smart_account_context_rules   (contract_id, rule_id, active, meta, ledger/order)
        smart_account_signers         (contract_id, rule_id, signer_type, address|credential_id, active)
        smart_account_policies        (contract_id, rule_id, policy_address, install_params, active)
        │
        ▼
query API:
  GET /api/v1/silver/smart-accounts/lookup/credential/{credentialId}
  GET /api/v1/silver/smart-accounts/lookup/address/{address}
  GET /api/v1/silver/smart-accounts/{contract_id}/rules
  GET /api/v1/silver/smart-accounts/stats
```

1. **Detection fix (day 1)** — Add the kit event vocabulary as a first-class signal: any successful contract event with `topic0_decoded` in the 11-event set classifies the emitting contract as `openzeppelin` with high confidence. Do not require `in_successful_contract_call = true`; on testnet, successful `context_rule_added` contract events have `successful = true` and `in_successful_contract_call = false`. Update all duplicate OZ/admin heuristic lists, not just `wallet_openzeppelin.go`: `silver-realtime-transformer/go/transformer.go`, `stellar-query-api/go/handlers_smart_wallets.go`, `stellar-query-api/go/handlers_smart_wallet_detail.go`, and `stellar-query-api/go/tx_semantic.go`. Add `add_context_rule` / `update_context_rule` / `remove_context_rule` / `add_policy` / `remove_policy`; demote guardian/recovery storage matching to low-confidence fallback.
2. **Event vocabulary** — Parse exactly these 11 event names unless the pinned kit schema says otherwise: `context_rule_added`, `context_rule_removed`, `context_rule_meta_updated`, `signer_added`, `signer_removed`, `signer_registered`, `signer_deregistered`, `policy_added`, `policy_removed`, `policy_registered`, `policy_deregistered`.
3. **Silver state model** — one new transform job following the existing `transformJobs` pattern: query Bronze events by event name, parse `data_decoded` JSON, and normalize bronze hex `contract_id` to the public `C...` strkey using the existing `hexToStrKey` helper pattern. Handle **both payload generations** (legacy inline `signers`/`policies` arrays; current registry shape where `signer_registered`/`policy_registered` publish payloads and rules reference `signer_ids`/`policy_ids`). Rows carry `active` flags maintained by add/remove events rather than query-time recomputation — materialized current state, per lake architecture.
4. **Ordering rule** — latest-event-wins must use true transaction order where possible. `contract_events_stream_v1` has `ledger_sequence`, `transaction_hash`, `operation_index`, and `event_index`, but no transaction index/TOID column. Join `transactions_row_v2` on `(ledger_sequence, transaction_hash)` to carry `transaction_id`, then order by `(transaction_id, operation_index, event_index)`. If `transaction_id` is unavailable in an older source, fall back to `(ledger_sequence, transaction_hash, operation_index, event_index)` and mark that as deterministic but not semantically perfect for same-ledger add/remove across different transactions.
5. **Payload parser** — Use the kit to understand the domain model, not as code to copy. The kit schema parses Goldsky's raw SCVal JSON (`data_json->'map'` with `{key,val}` entries). Obsrvr's `data_decoded` is already normalized by `ConvertScValToJSON` into `{"type":"map","entries":{...},"keys":[...]}`, vectors become arrays, addresses become `{type,address}`, and bytes become `{type:"bytes",hex,base64,length}`. The parser should be written against Obsrvr's shape and should read:
   - `topics[1]` / `topic1_decoded` as `context_rule_id`, `signer_id`, or `policy_id`.
   - map entries `signer`, `signers`, `signer_id`, `signer_ids`, `policy`, `policies`, `policy_id`, `policy_ids`, and `install_param`.
   - signer tuple/vector positions: `[0] = signer_type`, `[1].address = signer_address`, `[2].hex = raw signer bytes`.
   - credential extraction per the kit rule: for `External` signers, if raw signer bytes are longer than 65 bytes / 130 hex chars, `credential_id = hex[130:]`; otherwise `credential_id` is null and the original bytes stay in `raw_bytes` for inspection. Lookup input is lowercase hex; the SDK normalizes base64url credential IDs to hex before calling the indexer.
6. **Endpoints** — Make Obsrvr-native `/api/v1/silver/...` routes canonical and shape them like the rest of the Silver API. The kit response fields are useful acceptance vocabulary (`contracts`, rule counts, signer counts, active context rules), but exact path/field compatibility is a follow-up, not the first bet. Wire the same state into the existing smart-wallet detail response, replacing the storage-substring signer scrape.
7. **Reference material** for payload shapes and endpoint semantics: `/home/tillman/Documents/smart-account-kit/indexer/handler/schema.sql`, `indexer/handler/src/index.ts`, and `src/indexer.ts` at commit `6f1c035`. Treat these as examples and fixtures, not source to paste. Check in small Obsrvr-shaped fixture payloads for both generations so the transform is independent of a moving remote repo.

## Scope line

```
COULD HAVE ───────────── usage overlay from auth-capture (last-used per signer);
                         richer stats beyond kit parity; mainnet backfill plan write-up
NICE TO HAVE ─────────── thin kit-compatible aliases/field mapping;
                         testnet historical replay via silver-cold-replay job
MUST HAVE ══════════════ transformer job parsing all 11 events (both shapes,
                         credential extraction, transaction-order-aware latest wins);
                         hot schema + migration + cold-flush schema registration;
                         Obsrvr-native smart-account endpoints and smart-wallet integration;
                         detection fix across duplicate heuristic lists; tests against fixture events
```

## Rabbit holes (don't)

- Don't build a generic SCVal→model decoding framework — parse exactly the two known payload shapes, fail closed (log + skip) on anything else.
- Don't paste the kit `schema.sql` parser directly — its JSON paths are for Goldsky's raw SCVal shape, not Obsrvr's normalized `data_decoded`.
- Don't copy the kit API wholesale — preserve the domain semantics, but keep Obsrvr's `/api/v1/silver/...` conventions as the canonical surface.
- Don't chase every historical OZ contract revision; testnet replay tells us which shapes actually exist on-chain.
- Don't restructure the smart-wallet detail handler — swap its signer source, nothing more.
- Don't touch the smart-account-kit SDK or try to get upstream to bless the endpoint; semantic parity is enough for this bet.
- Don't claim same-ledger ordering is fixed unless the transform has a transaction-order source (`transactions_row_v2.transaction_id` or equivalent).

## No-Gos

- No new services — this is a transformer job + query-api endpoints.
- No Goldsky-style live views over raw events — we materialize (query-time recomputation is the kit's scaling flaw; see un-indexable `credential_id` in their views).
- No bronze schema changes — events are already captured.
- No mainnet backfill this cycle.

## Implementation map

- `silver-realtime-transformer/go/bronze_reader.go` and `bronze_cold_reader.go`: add a smart-account event query joining `transactions_row_v2` for `transaction_id`, filtering `event_type = 'contract'`, `successful = true` where present, and the 11 event names.
- `silver-realtime-transformer/go/schema/init_silver_hot_complete.sql` plus a new migration: add `smart_account_context_rules`, `smart_account_signers`, and `smart_account_policies` with indexes on `(contract_id)`, `(credential_id)`, `(signer_address)`, `(contract_id, rule_id)`, and active-only partial indexes.
- `silver-cold-flusher/schema/*`: register the new silver tables so cold flush/backfill does not silently drop the state.
- `silver-realtime-transformer/go/transformer.go`: add the transform job after Bronze event availability and before wallet classification, then let wallet classification consume the materialized state.
- `stellar-query-api/go`: add reader methods and handlers for lookup-by-credential, lookup-by-address, contract rules, and smart-account stats under `/api/v1/silver/smart-accounts/*`; update `/api/v1/silver/smart-wallets/{contract_id}` to source signer configuration/policies from smart-account state first.
- Swagger/docs fixtures: include one known testnet contract, one credential, one signer address, and a removed signer/rule case.

## Implementation notes

- For an already-running testnet deployment, do **not** use the transformer's full `--cold-replay` path just to populate smart-account state: it resets and advances the shared realtime transformer checkpoint while replaying every transform. Use the narrow `--smart-account-replay` mode instead. It rebuilds only `smart_account_context_rules`, `smart_account_signers`, `smart_account_policies`, and semantic wallet classification for a bounded ledger range, while leaving `realtime_transformer_checkpoint` unchanged.
- Observed testnet contracts use the concrete admin functions `update_context_rule_name` and `update_context_rule_valid_until`; treat them as OpenZeppelin smart-account signals alongside the shorthand `update_context_rule`.
- The first implementation targets the current v3 Bronze topic columns (`topic0_decoded`, `topic1_decoded`) because those are present in the testnet deployment. A `topics_decoded` fallback remains a portability follow-up for older Bronze sources.

## Testnet implementation status (2026-07-08)

Implemented and deployed to obsrvr-lake testnet with:

- `withobsrvr/stellar-query-api:smart-account-20260708`
- `withobsrvr/silver-realtime-transformer:smart-account-20260708`

Applied schema on testnet:

- `silver-realtime-transformer/migrations/009_add_smart_account_state.sql`
- TOID columns from migration `008` that the deployed transformer needs for enriched-operation writes (`transaction_id`, `operation_id` on both enriched operation tables). The heavier `008` index creation was intentionally not run during this rollout.

Replay and live maintenance evidence:

- Bounded smart-account replay covered fixture ranges `1913500-1914500`, `2756060-2756070`, and `2996160-2997470`.
- Live transformer batches later advanced smart-account state past replay data. The public stats endpoint returned `last_modified_ledger=3501296`, `contract_count=79`, `active_rule_count=96`, `active_signer_count=142`, `credential_count=5`, and `active_policy_count=46`.

Pinned acceptance fixtures:

- Credential lookup: `9ca5204617ab254b6b21cbae8a30c42377d0cd4f` returns contract `CCQBQIAG2E2L5NOIML2SGAJYMXPID3MAQNII5USMENID3SDJ4ATOU2HG`, rule IDs `[0,3,4]`, and one credential signer.
- Address lookup: `GDIJX4AHMT2JSAJHYL6EUDNBBEFGI75XQWWCTUESDMXJ3N3FOS7QAPZ7` returns contract `CCWI24KIBLWBXYQD6ORLLST3NGBEQUPEGXS7PZM5CR3EU6QSVSGPR7GS`.
- Contract rules: `CCQBQIAG2E2L5NOIML2SGAJYMXPID3MAQNII5USMENID3SDJ4ATOU2HG` returns active rules `[0,3,4]`; removed rule IDs from the same contract are absent from the default active view.
- Smart-wallet detail: `GET /api/v1/silver/smart-wallet/CCQBQIAG2E2L5NOIML2SGAJYMXPID3MAQNII5USMENID3SDJ4ATOU2HG` returns `is_smart_wallet=true`, `wallet_type=openzeppelin`, `confidence=0.95`, and signer IDs from state, including the credential above.

Reference worker note:

- `https://smart-account-indexer.sdf-ecosystem.workers.dev/api/lookup/9ca5204617ab254b6b21cbae8a30c42377d0cd4f` returned HTTP 500 `{"error":"Database query failed"}` on 2026-07-08, so worker parity could not be used as the rollout gate. Acceptance for this cycle is based on Obsrvr-native state reconstruction from real testnet Bronze data plus endpoint verification.

## Complete testnet backfill + serving projection (2026-07-08)

Completed a full testnet smart-account backfill through ledger `3502597`.

- First batch job replayed ledgers `1-2200000` from Bronze Cold before OOM restart; all completed batches had already committed.
- Resume batch job replayed ledgers `2200001-3502597` from Bronze Cold with `--smart-account-replay-batch-size=25000`, processing `16984` smart-account events and `8027` wallet-classification touches in `4m9s`.
- The targeted replay did not move `realtime_transformer_checkpoint`; the live transformer continued running and maintaining new smart-account state.

Added and deployed denormalized serving tables in `serving-projection-processor`:

- `serving.sv_smart_account_contracts_current`
- `serving.sv_smart_account_rules_current`
- `serving.sv_smart_account_signers_by_credential`
- `serving.sv_smart_account_signers_by_address`

Deployment:

- Image: `withobsrvr/serving-projection-processor:54619b1-20260708165050`
- Nomad job: `serving-projection-processor` version `9`
- Projector: `smart_accounts`

Final testnet serving counts after the complete backfill:

- Contracts: `8875`
- Active rules: `11263`
- Credential lookup rows: `238`
- Address lookup rows: `3114`
- Serving max smart-account ledger: `3502726`, matching Silver state at verification time.

Fixture checks in serving:

- Credential `9ca5204617ab254b6b21cbae8a30c42377d0cd4f` maps to `CCQBQIAG2E2L5NOIML2SGAJYMXPID3MAQNII5USMENID3SDJ4ATOU2HG`, rule `{3}`, signer `{10}`.
- Address `GDIJX4AHMT2JSAJHYL6EUDNBBEFGI75XQWWCTUESDMXJ3N3FOS7QAPZ7` maps to `CCWI24KIBLWBXYQD6ORLLST3NGBEQUPEGXS7PZM5CR3EU6QSVSGPR7GS`, rule `{0}`, signer `{0}`.

## Done

Against known OpenZeppelin smart-account **testnet** accounts:

1. `GET /api/v1/silver/smart-accounts/lookup/credential/{cred}` returns the expected contract set and rule/signer counts for a real passkey credential.
2. `GET /api/v1/silver/smart-accounts/{contract}/rules` returns active rules, signers, and policies from materialized state, with removed rules/signers absent from the default active view.
3. `GET /api/v1/silver/smart-wallets/{contract}` detects `openzeppelin` with event-based confidence and real signers (credential IDs, not storage substrings).
4. `go test ./...` green in both services, with fixture-event tests covering both payload generations and an add→remove→re-add sequence.

## Resolved questions

1. Acceptance fixtures are pinned above from real testnet replay ranges. The external worker was unavailable for the chosen credential during rollout, so it is no longer a hard gate for this Obsrvr-native endpoint.
2. v3 Bronze positional topic columns are present in obsrvr-lake testnet and are the supported rollout target. Older Bronze fallback can be added if this transformer needs to run against earlier schemas.
