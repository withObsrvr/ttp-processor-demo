### Updated rollout note

 Continued the serving-layer production rollout on the existing Nomad node and enabled the
 recent-activity/event tier.

 #### Newly deployed

 Enabled in serving-projection-processor:
 - contract_calls_recent
 - operations_recent
 - events_recent

 Enabled serving-first API reads in stellar-query-api for:
 - /api/v1/silver/contracts/{id}/recent-calls
 - /api/v1/silver/accounts/{id}/activity
 - /api/v1/silver/events/generic
 - /api/v1/silver/events/contract/{contract_id}

 All follow the same staged rollout model:
 1. try serving table first
 2. fall back to legacy/unified path if serving data is absent or insufficient

 #### Images deployed

 - withobsrvr/stellar-query-api:2026-04-14-05
 - withobsrvr/serving-projection-processor:2026-04-14-04

 #### Nomad status

 Jobs healthy after rollout:
 - stellar-query-api
 - serving-projection-processor

 #### Serving data observed

 Current serving counts observed from prod:
 - serving.sv_events_recent = 6418
 - serving.sv_operations_recent populated
 - serving.sv_contract_calls_recent populated

 Processor logs confirmed successful runs for:
 - operations_recent
 - events_recent
 - contract_calls_recent

 #### Gateway verification completed

 Verified live through gateway.withobsrvr.com:

 Contracts
 - /api/v1/silver/contracts/top
 - /api/v1/silver/contracts/{id}/metadata
 - /api/v1/silver/contracts/{id}/recent-calls

 Accounts
 - /api/v1/silver/accounts/current
 - /api/v1/silver/accounts/top
 - /api/v1/silver/accounts/{id}/balances
 - /api/v1/silver/accounts/{id}/activity

 Assets
 - /api/v1/silver/assets
 - /api/v1/silver/assets/{asset}/holders
 - /api/v1/silver/assets/{asset}/stats

 Network
 - /api/v1/silver/stats/network

 Events
 - /api/v1/silver/events/generic
 - /api/v1/silver/events/contract/{contract_id}

 #### Verification results

 - account current/top/balances: working
 - asset list/stats/holders: working
 - network stats: working
 - contract rankings: working
 - contract metadata: improved via fallback when serving metadata is incomplete
 - contract recent calls: working, cursor pagination verified
 - account activity: working, cursor pagination verified
 - generic events: working, serving-backed
 - contract-scoped generic events: working

 #### Important implementation note

 During rollout, events_recent initially failed because the projector was reading the wrong DB
 context for bronze event rows. That was corrected by:
 - querying bronze hot directly from the source pool
 - inserting into serving through the target pool

 Topic extraction was also improved so serving-backed event responses now expose decoded topic
 values where available.

 #### Current production serving-backed endpoints

 Current-state / ranking / stats:
 - /api/v1/silver/stats/network
 - /api/v1/silver/accounts/current
 - /api/v1/silver/accounts/top
 - /api/v1/silver/accounts/{id}/balances
 - /api/v1/silver/assets
 - /api/v1/silver/assets/{asset}/holders
 - /api/v1/silver/assets/{asset}/stats
 - /api/v1/silver/contracts/top
 - /api/v1/silver/contracts/{id}/metadata

 Recent activity / feeds:
 - /api/v1/silver/contracts/{id}/recent-calls
 - /api/v1/silver/accounts/{id}/activity
 - /api/v1/silver/events/generic
 - /api/v1/silver/events/contract/{contract_id}

 #### Current posture

 The serving layer is now live in prod for:
 - current state
 - leaderboard/ranking surfaces
 - contract recent calls
 - account recent activity
 - recent generic contract events

 This was done without adding a new Nomad server.

 #### Remaining notable gaps

 Still not yet serving-backed:
 - richer explorer event endpoint routing beyond the generic event surfaces
 - any additional event-specific enrichment surfaces that still depend on bronze/unified logic
 - long-term retention/cleanup/compaction for recent-feed tables
 - optimization of rebuild-style projectors into incremental projectors

 ────────────────────────────────────────────────────────────────────────────────

 ### Quick endpoint status list

 Serving-backed in prod now:
 - /api/v1/silver/stats/network
 - /api/v1/silver/accounts/current
 - /api/v1/silver/accounts/top
 - /api/v1/silver/accounts/{id}/balances
 - /api/v1/silver/accounts/{id}/activity
 - /api/v1/silver/assets
 - /api/v1/silver/assets/{asset}/holders
 - /api/v1/silver/assets/{asset}/stats
 - /api/v1/silver/contracts/top
 - /api/v1/silver/contracts/{id}/metadata
 - /api/v1/silver/contracts/{id}/recent-calls
 - /api/v1/silver/events/generic
 - /api/v1/silver/events/contract/{contract_id}
