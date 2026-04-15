-- Minimal serving schema for Obsrvr Lake / Prism
--
-- Goal:
--   - Keep full history in DuckLake
--   - Keep only current state, recent feeds, and compact aggregates in Postgres
--   - Avoid rebuilding Horizon-scale historical relational storage
--
-- Notes:
--   - Assumes PostgreSQL 15+
--   - Recent feed tables should be partitioned by time in production
--   - Numeric display columns are optional; they exist to avoid repeated formatting in API handlers

create schema if not exists serving;

create extension if not exists pg_trgm;

-- ============================================================
-- 1. CURRENT STATE TABLES
-- ============================================================

create table if not exists serving.sv_accounts_current (
    account_id               text primary key,
    balance_stroops          bigint not null,
    sequence_number          bigint,
    num_subentries           integer,
    created_at               timestamptz,
    last_modified_ledger     bigint not null,
    updated_at               timestamptz not null,
    home_domain              text,
    master_weight            integer,
    low_threshold            integer,
    med_threshold            integer,
    high_threshold           integer,
    signers_json             jsonb,
    is_smart_account         boolean not null default false,
    smart_account_type       text,
    first_seen_ledger        bigint
);

create index if not exists sv_accounts_current_last_modified_idx
    on serving.sv_accounts_current (last_modified_ledger desc);
create index if not exists sv_accounts_current_balance_idx
    on serving.sv_accounts_current (balance_stroops desc);
create index if not exists sv_accounts_current_home_domain_idx
    on serving.sv_accounts_current (home_domain);
create index if not exists sv_accounts_current_smart_idx
    on serving.sv_accounts_current (is_smart_account, smart_account_type);


create table if not exists serving.sv_account_balances_current (
    account_id               text not null,
    asset_key                text not null,
    asset_code               text not null,
    asset_issuer             text,
    asset_type               text not null,
    balance_stroops          bigint not null,
    balance_display          numeric(38,7),
    limit_stroops            bigint,
    is_authorized            boolean,
    last_modified_ledger     bigint not null,
    updated_at               timestamptz not null,
    primary key (account_id, asset_key)
);

create index if not exists sv_account_balances_current_account_idx
    on serving.sv_account_balances_current (account_id, balance_stroops desc);
create index if not exists sv_account_balances_current_asset_rank_idx
    on serving.sv_account_balances_current (asset_key, balance_stroops desc, account_id);
create index if not exists sv_account_balances_current_asset_updated_idx
    on serving.sv_account_balances_current (asset_key, updated_at desc);


create table if not exists serving.sv_assets_current (
    asset_key                text primary key,
    asset_code               text not null,
    asset_issuer             text,
    asset_type               text not null,
    is_native                boolean not null,
    is_sep41                 boolean not null default false,
    contract_id              text,
    name                     text,
    symbol                   text,
    decimals                 integer,
    icon_url                 text,
    verified                 boolean not null default false,
    issuer_domain            text,
    holder_count             bigint not null default 0,
    trustline_count          bigint not null default 0,
    circulating_supply       numeric(38,7),
    updated_at               timestamptz not null
);

create index if not exists sv_assets_current_holder_count_idx
    on serving.sv_assets_current (holder_count desc);
create index if not exists sv_assets_current_supply_idx
    on serving.sv_assets_current (circulating_supply desc);
create index if not exists sv_assets_current_code_idx
    on serving.sv_assets_current (asset_code);
create index if not exists sv_assets_current_verified_code_idx
    on serving.sv_assets_current (verified, asset_code);


create table if not exists serving.sv_contracts_current (
    contract_id                    text primary key,
    name                           text,
    symbol                         text,
    contract_type                  text,
    wallet_type                    text,
    creator_account                text,
    deploy_ledger                  bigint,
    deploy_timestamp               timestamptz,
    wasm_hash                      text,
    wasm_size_bytes                bigint,
    persistent_entries             integer,
    temporary_entries              integer,
    instance_entries               integer,
    total_state_size_bytes         bigint,
    estimated_monthly_rent_stroops bigint,
    exported_functions_json        jsonb,
    first_seen_at                  timestamptz,
    last_seen_at                   timestamptz,
    updated_at                     timestamptz not null
);

create index if not exists sv_contracts_current_type_idx
    on serving.sv_contracts_current (contract_type);
create index if not exists sv_contracts_current_wallet_type_idx
    on serving.sv_contracts_current (wallet_type);
create index if not exists sv_contracts_current_creator_idx
    on serving.sv_contracts_current (creator_account);
create index if not exists sv_contracts_current_last_seen_idx
    on serving.sv_contracts_current (last_seen_at desc);


create table if not exists serving.sv_offers_current (
    offer_id                  bigint primary key,
    seller_account_id         text not null,
    selling_asset_key         text not null,
    buying_asset_key          text not null,
    amount                    numeric(38,7),
    price                     numeric(38,18),
    last_modified_ledger      bigint not null,
    updated_at                timestamptz not null
);

create index if not exists sv_offers_current_seller_idx
    on serving.sv_offers_current (seller_account_id, last_modified_ledger desc);
create index if not exists sv_offers_current_pair_idx
    on serving.sv_offers_current (selling_asset_key, buying_asset_key);


create table if not exists serving.sv_liquidity_pools_current (
    pool_id                   text primary key,
    asset_a_key               text not null,
    asset_b_key               text not null,
    reserve_a                 numeric(38,7),
    reserve_b                 numeric(38,7),
    total_shares              numeric(38,7),
    fee_bp                    integer,
    trustline_count           integer,
    last_modified_ledger      bigint,
    updated_at                timestamptz not null
);

create index if not exists sv_liquidity_pools_current_pair_idx
    on serving.sv_liquidity_pools_current (asset_a_key, asset_b_key);
create index if not exists sv_liquidity_pools_current_updated_idx
    on serving.sv_liquidity_pools_current (updated_at desc);

-- ============================================================
-- 2. RECENT FEED TABLES
--   Retention recommended: 30-90 days in Postgres.
-- ============================================================

create table if not exists serving.sv_transactions_recent (
    tx_hash                   text primary key,
    ledger_sequence           bigint not null,
    created_at                timestamptz not null,
    source_account            text,
    fee_charged_stroops       bigint,
    max_fee_stroops           bigint,
    successful                boolean not null,
    operation_count           integer,
    tx_type                   text,
    summary_text              text,
    summary_json              jsonb,
    primary_contract_id       text,
    primary_asset_key         text,
    primary_amount_stroops    bigint,
    memo_type                 text,
    memo_value                text,
    account_sequence          bigint,
    is_soroban                boolean not null default false,
    cpu_insns                 bigint,
    mem_bytes                 bigint,
    read_bytes                bigint,
    write_bytes               bigint,
    ingested_at               timestamptz not null default now()
);

alter table serving.sv_transactions_recent
    add column if not exists summary_json jsonb;

create index if not exists sv_transactions_recent_ledger_idx
    on serving.sv_transactions_recent (ledger_sequence desc);
create index if not exists sv_transactions_recent_created_idx
    on serving.sv_transactions_recent (created_at desc);
create index if not exists sv_transactions_recent_source_idx
    on serving.sv_transactions_recent (source_account, created_at desc);
create index if not exists sv_transactions_recent_contract_idx
    on serving.sv_transactions_recent (primary_contract_id, created_at desc);
create index if not exists sv_transactions_recent_type_idx
    on serving.sv_transactions_recent (tx_type, created_at desc);


create table if not exists serving.sv_operations_recent (
    operation_id              text primary key,
    tx_hash                   text not null,
    ledger_sequence           bigint not null,
    created_at                timestamptz not null,
    op_index                  integer not null,
    type_code                 integer,
    type_name                 text,
    source_account            text,
    destination_account       text,
    asset_key                 text,
    amount_stroops            bigint,
    contract_id               text,
    function_name             text,
    successful                boolean,
    is_payment_op             boolean not null default false,
    is_soroban_op             boolean not null default false,
    summary_text              text
);

create index if not exists sv_operations_recent_tx_idx
    on serving.sv_operations_recent (tx_hash, op_index);
create index if not exists sv_operations_recent_ledger_idx
    on serving.sv_operations_recent (ledger_sequence, op_index);
create index if not exists sv_operations_recent_source_idx
    on serving.sv_operations_recent (source_account, created_at desc);
create index if not exists sv_operations_recent_destination_idx
    on serving.sv_operations_recent (destination_account, created_at desc);
create index if not exists sv_operations_recent_contract_idx
    on serving.sv_operations_recent (contract_id, created_at desc);
create index if not exists sv_operations_recent_type_idx
    on serving.sv_operations_recent (type_name, created_at desc);


create table if not exists serving.sv_events_recent (
    event_id                  text primary key,
    tx_hash                   text not null,
    ledger_sequence           bigint not null,
    created_at                timestamptz not null,
    event_index               integer,
    contract_id               text,
    topic0                    text,
    topic1                    text,
    topic2                    text,
    topic3                    text,
    event_type                text,
    from_address              text,
    to_address                text,
    asset_key                 text,
    amount_stroops            bigint,
    raw_event_json            jsonb,
    decoded_summary           text
);

create index if not exists sv_events_recent_created_idx
    on serving.sv_events_recent (created_at desc);
create index if not exists sv_events_recent_ledger_idx
    on serving.sv_events_recent (ledger_sequence desc, event_index);
create index if not exists sv_events_recent_tx_idx
    on serving.sv_events_recent (tx_hash);
create index if not exists sv_events_recent_contract_idx
    on serving.sv_events_recent (contract_id, created_at desc);
create index if not exists sv_events_recent_topic0_idx
    on serving.sv_events_recent (topic0, created_at desc);
create index if not exists sv_events_recent_from_idx
    on serving.sv_events_recent (from_address, created_at desc);
create index if not exists sv_events_recent_to_idx
    on serving.sv_events_recent (to_address, created_at desc);


create table if not exists serving.sv_explorer_events_recent (
    event_id                  text primary key,
    tx_hash                   text not null,
    ledger_sequence           bigint not null,
    created_at                timestamptz not null,
    event_index               integer,
    operation_index           integer,
    contract_id               text,
    contract_address          text,
    topic0                    text,
    topic1                    text,
    topic2                    text,
    topic3                    text,
    topics_decoded            text,
    data_decoded              text,
    successful                boolean,
    explorer_type             text not null,
    protocol                  text,
    contract_name             text,
    contract_symbol           text,
    contract_category         text,
    ingested_at               timestamptz not null default now()
);

create index if not exists sv_explorer_events_recent_created_idx
    on serving.sv_explorer_events_recent (created_at desc);
create index if not exists sv_explorer_events_recent_ledger_idx
    on serving.sv_explorer_events_recent (ledger_sequence desc, event_index);
create index if not exists sv_explorer_events_recent_type_idx
    on serving.sv_explorer_events_recent (explorer_type, created_at desc);
create index if not exists sv_explorer_events_recent_contract_idx
    on serving.sv_explorer_events_recent (contract_id, created_at desc);
create index if not exists sv_explorer_events_recent_contract_address_idx
    on serving.sv_explorer_events_recent (contract_address, created_at desc);
create index if not exists sv_explorer_events_recent_tx_idx
    on serving.sv_explorer_events_recent (tx_hash);
create index if not exists sv_explorer_events_recent_topic0_idx
    on serving.sv_explorer_events_recent (topic0, created_at desc);
create index if not exists sv_explorer_events_recent_contract_name_trgm_idx
    on serving.sv_explorer_events_recent using gin (contract_name gin_trgm_ops);
create index if not exists sv_explorer_events_recent_topics_trgm_idx
    on serving.sv_explorer_events_recent using gin (topics_decoded gin_trgm_ops);


create table if not exists serving.sv_trades_recent (
    trade_id                   text primary key,
    ledger_sequence            bigint not null,
    created_at                 timestamptz not null,
    tx_hash                    text,
    seller_account             text,
    buyer_account              text,
    base_asset_key             text not null,
    counter_asset_key          text not null,
    base_amount                numeric(38,7),
    counter_amount             numeric(38,7),
    price                      numeric(38,18),
    trade_type                 text,
    pool_id                    text,
    offer_id                   bigint
);

create index if not exists sv_trades_recent_created_idx
    on serving.sv_trades_recent (created_at desc);
create index if not exists sv_trades_recent_pair_idx
    on serving.sv_trades_recent (base_asset_key, counter_asset_key, created_at desc);
create index if not exists sv_trades_recent_seller_idx
    on serving.sv_trades_recent (seller_account, created_at desc);
create index if not exists sv_trades_recent_buyer_idx
    on serving.sv_trades_recent (buyer_account, created_at desc);


create table if not exists serving.sv_contract_calls_recent (
    call_id                    text primary key,
    tx_hash                    text not null,
    ledger_sequence            bigint not null,
    created_at                 timestamptz not null,
    contract_id                text not null,
    caller_account             text,
    function_name              text,
    successful                 boolean,
    cpu_insns                  bigint,
    mem_bytes                  bigint,
    read_bytes                 bigint,
    write_bytes                bigint,
    summary_text               text
);

create index if not exists sv_contract_calls_recent_contract_idx
    on serving.sv_contract_calls_recent (contract_id, created_at desc);
create index if not exists sv_contract_calls_recent_caller_idx
    on serving.sv_contract_calls_recent (caller_account, created_at desc);
create index if not exists sv_contract_calls_recent_function_idx
    on serving.sv_contract_calls_recent (function_name, created_at desc);
create index if not exists sv_contract_calls_recent_created_idx
    on serving.sv_contract_calls_recent (created_at desc);

-- ============================================================
-- 3. COMPACT AGGREGATE TABLES
-- ============================================================

create table if not exists serving.sv_network_stats_current (
    network                       text primary key,
    generated_at                  timestamptz not null,
    latest_ledger                 bigint not null,
    latest_ledger_closed_at       timestamptz,
    avg_close_time_seconds        numeric(10,2),
    protocol_version              integer,
    tx_24h_total                  bigint,
    tx_24h_failed                 bigint,
    ops_24h_total                 bigint,
    ops_24h_contract_invoke       bigint,
    ops_24h_payments              bigint,
    ops_prev_24h_total            bigint,
    ops_prev_24h_contract_invoke  bigint,
    fee_median_stroops            bigint,
    fee_p99_stroops               bigint,
    fee_daily_total_stroops       bigint,
    surge_active                  boolean,
    active_accounts_24h           bigint,
    created_accounts_24h          bigint,
    active_contracts_24h          bigint,
    avg_cpu_insns_24h             bigint,
    rent_burned_24h_stroops       bigint,
    validator_count               integer
);


create table if not exists serving.sv_asset_stats_current (
    asset_key                    text primary key,
    holder_count                 bigint,
    trustline_count              bigint,
    circulating_supply           numeric(38,7),
    volume_24h                   numeric(38,7),
    transfers_24h                bigint,
    unique_accounts_24h          bigint,
    top10_concentration          numeric(12,8),
    updated_at                   timestamptz not null
);

create index if not exists sv_asset_stats_current_volume_idx
    on serving.sv_asset_stats_current (volume_24h desc);
create index if not exists sv_asset_stats_current_holders_idx
    on serving.sv_asset_stats_current (holder_count desc);
create index if not exists sv_asset_stats_current_updated_idx
    on serving.sv_asset_stats_current (updated_at desc);


create table if not exists serving.sv_contract_stats_current (
    contract_id                  text primary key,
    total_calls_24h              bigint,
    total_calls_7d               bigint,
    total_calls_30d              bigint,
    unique_callers_24h           bigint,
    unique_callers_7d            bigint,
    success_count_24h            bigint,
    failure_count_24h            bigint,
    success_rate_24h             numeric(12,8),
    top_function                 text,
    last_activity_at             timestamptz,
    avg_cpu_insns_24h            bigint,
    first_seen_at                timestamptz,
    updated_at                   timestamptz not null
);

create index if not exists sv_contract_stats_current_calls24h_idx
    on serving.sv_contract_stats_current (total_calls_24h desc);
create index if not exists sv_contract_stats_current_calls7d_idx
    on serving.sv_contract_stats_current (total_calls_7d desc);
create index if not exists sv_contract_stats_current_last_activity_idx
    on serving.sv_contract_stats_current (last_activity_at desc);


create table if not exists serving.sv_contract_function_stats_current (
    contract_id                  text not null,
    function_name                text not null,
    calls_24h                    bigint,
    calls_7d                     bigint,
    calls_30d                    bigint,
    success_count_24h            bigint,
    failure_count_24h            bigint,
    avg_cpu_insns_24h            bigint,
    last_called_at               timestamptz,
    updated_at                   timestamptz not null,
    primary key (contract_id, function_name)
);

create index if not exists sv_contract_function_stats_current_rank_idx
    on serving.sv_contract_function_stats_current (contract_id, calls_24h desc);


create table if not exists serving.sv_ledger_stats_recent (
    ledger_sequence              bigint primary key,
    closed_at                    timestamptz not null,
    ledger_hash                  text,
    prev_hash                    text,
    protocol_version             integer,
    base_fee_stroops             bigint,
    max_tx_set_size              integer,
    successful_tx_count          integer,
    failed_tx_count              integer,
    operation_count              integer,
    soroban_op_count             integer,
    events_emitted               integer,
    total_fee_charged_stroops    bigint,
    total_cpu_insns              bigint,
    total_read_bytes             bigint,
    total_write_bytes            bigint,
    total_rent_stroops           bigint,
    close_time_seconds           numeric(10,2)
);

create index if not exists sv_ledger_stats_recent_closed_idx
    on serving.sv_ledger_stats_recent (closed_at desc);


create table if not exists serving.sv_asset_holders_top (
    asset_key                    text not null,
    rank                         integer not null,
    account_id                   text not null,
    balance_stroops              bigint not null,
    updated_at                   timestamptz not null,
    primary key (asset_key, rank)
);

create index if not exists sv_asset_holders_top_account_idx
    on serving.sv_asset_holders_top (account_id);

-- ============================================================
-- 4. LOOKUP / SEARCH / REGISTRY TABLES
-- ============================================================

create table if not exists serving.sv_search_entities (
    network                     text not null,
    entity_type                 text not null,
    entity_id                   text not null,
    display_name                text,
    subtitle                    text,
    search_text                 text not null,
    rank_score                  numeric(12,4) not null default 0,
    updated_at                  timestamptz not null,
    primary key (network, entity_type, entity_id)
);

create index if not exists sv_search_entities_search_trgm_idx
    on serving.sv_search_entities using gin (search_text gin_trgm_ops);
create index if not exists sv_search_entities_rank_idx
    on serving.sv_search_entities (network, rank_score desc);


create table if not exists serving.sv_asset_metadata (
    asset_key                   text primary key,
    name                        text,
    symbol                      text,
    description                 text,
    icon_url                    text,
    issuer_domain               text,
    toml_url                    text,
    verified                    boolean,
    updated_at                  timestamptz not null
);


create table if not exists serving.sv_contract_labels (
    contract_id                 text primary key,
    name                        text,
    category                    text,
    subcategory                 text,
    verified                    boolean,
    source                      text,
    updated_at                  timestamptz not null
);

-- ============================================================
-- 5. OPERATIONAL METADATA TABLES
-- ============================================================

create table if not exists serving.sv_projection_checkpoints (
    projection_name             text not null,
    network                     text not null,
    last_ledger_sequence        bigint not null,
    last_closed_at              timestamptz,
    updated_at                  timestamptz not null,
    primary key (projection_name, network)
);


create table if not exists serving.sv_rebuild_jobs (
    job_id                      uuid primary key,
    job_type                    text not null,
    status                      text not null,
    start_ledger                bigint,
    end_ledger                  bigint,
    started_at                  timestamptz not null,
    finished_at                 timestamptz,
    error_text                  text
);

-- ============================================================
-- OPTIONAL VIEW EXAMPLES
-- ============================================================

create or replace view serving.v_top_accounts as
select
    account_id,
    balance_stroops,
    sequence_number,
    last_modified_ledger,
    updated_at
from serving.sv_accounts_current
order by balance_stroops desc;

create or replace view serving.v_assets_ranked_by_volume as
select
    a.asset_key,
    a.asset_code,
    a.asset_issuer,
    a.asset_type,
    a.name,
    a.symbol,
    s.volume_24h,
    s.holder_count,
    s.circulating_supply,
    s.updated_at
from serving.sv_assets_current a
join serving.sv_asset_stats_current s using (asset_key)
order by s.volume_24h desc nulls last;
