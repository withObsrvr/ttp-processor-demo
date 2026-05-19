alter table serving.sv_explorer_events_recent
    add column if not exists transaction_successful boolean;

alter table serving.sv_explorer_events_recent
    add column if not exists in_successful_contract_call boolean;

comment on column serving.sv_explorer_events_recent.transaction_successful is 'Containing transaction succeeded on-chain; source of public explorer event status.';
comment on column serving.sv_explorer_events_recent.in_successful_contract_call is 'Raw Soroban call-context flag; not equivalent to transaction success.';
comment on column serving.sv_explorer_events_recent.successful is 'Deprecated compatibility alias for transaction_successful on explorer event APIs.';

-- Existing rows were produced before the projection carried distinct semantics.
-- They must be rebuilt from public.contract_events_stream_v1 for full parity.
