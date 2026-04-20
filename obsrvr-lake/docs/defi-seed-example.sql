-- Example bootstrap SQL for the DeFi registry.
--
-- This file is intentionally documentation / bootstrap material, not a mandatory
-- migration. Review and replace placeholder metadata before using in an
-- environment that serves real traffic.

INSERT INTO serving.sv_defi_protocols (
    protocol_id,
    network,
    display_name,
    slug,
    category,
    status,
    adapter_name,
    pricing_model,
    health_model,
    supports_history,
    supports_rewards,
    supports_health_factor,
    website_url,
    docs_url,
    source,
    verified,
    config_json
) VALUES
    (
        'blend',
        'testnet',
        'Blend',
        'blend',
        'lending',
        'active',
        'blend_v1',
        'oracle_spot',
        'health_factor',
        true,
        true,
        true,
        'https://blend.capital/',
        'https://docs.blend.capital/',
        'manual',
        false,
        '{"note":"bootstrap example; contract/admin metadata reviewed for current testnet rollout","admin_account":"GCL4PUHEJX3ZCPTYB6RKPYK5NQDCGXVRATGX37MMH2MWA23KQEIHTKU5","seed_confidence":"user_verified"}'::jsonb
    ),
    (
        'aquarius',
        'testnet',
        'Aquarius',
        'aquarius',
        'amm',
        'active',
        'aquarius_v1',
        'reserve_share',
        null,
        true,
        false,
        false,
        'https://aqua.network/',
        'https://docs.aqua.network/',
        'manual',
        false,
        '{"note":"bootstrap example; router observed in contract_invocations_raw","seed_confidence":"observed_in_contract_invocations"}'::jsonb
    ),
    (
        'soroswap',
        'testnet',
        'Soroswap',
        'soroswap',
        'amm',
        'active',
        'soroswap_v1',
        'reserve_share',
        null,
        true,
        false,
        false,
        'https://soroswap.finance/',
        'https://docs.soroswap.finance/',
        'manual',
        false,
        '{"note":"bootstrap example; Soroswap hashes reviewed for current testnet rollout","factory_wasm_hash":"86285a9234d3f0d687eaf88efe8d5d72172b38c9a86624c9934c0cbf2aff2993","router_wasm_hash":"4b95bbf9caec2c6e00c786f53c5f392c2fcdb8435ac0a862ab5e0645eb65824c","pair_wasm_hash":"8447525edd62f72ffaf52136358034657ea0511a8fec1cd0ebde649f86cca464","token_wasm_hash":"263d165c1a1bf5a0dddbd0f21744a55cd8cacedf96ade78f53c570df11490e46","seed_confidence":"user_verified"}'::jsonb
    ),
    (
        'fxdao',
        'testnet',
        'FxDAO',
        'fxdao',
        'stable',
        'active',
        'fxdao_v1',
        'oracle_spot',
        'health_factor',
        true,
        true,
        true,
        null,
        null,
        'manual',
        false,
        '{"note":"bootstrap example; protocol row retained but contract ids still pending verification","status_note":"contract ids pending verification"}'::jsonb
    )
ON CONFLICT (protocol_id) DO UPDATE SET
    display_name = EXCLUDED.display_name,
    slug = EXCLUDED.slug,
    category = EXCLUDED.category,
    status = EXCLUDED.status,
    adapter_name = EXCLUDED.adapter_name,
    pricing_model = EXCLUDED.pricing_model,
    health_model = EXCLUDED.health_model,
    supports_history = EXCLUDED.supports_history,
    supports_rewards = EXCLUDED.supports_rewards,
    supports_health_factor = EXCLUDED.supports_health_factor,
    website_url = EXCLUDED.website_url,
    docs_url = EXCLUDED.docs_url,
    source = EXCLUDED.source,
    verified = EXCLUDED.verified,
    config_json = EXCLUDED.config_json,
    updated_at = now();

-- Contract rows below reflect currently-identified testnet contracts.
-- Blend values were supplied externally and are retained as unverified until
-- independently confirmed from indexed chain metadata. Aquarius and Soroswap
-- router entries were observed in live contract_invocations_raw.
INSERT INTO serving.sv_defi_protocol_contracts (
    protocol_id,
    contract_id,
    role,
    is_active,
    verified,
    source,
    metadata_json
) VALUES
    ('blend', 'CAPBMXIQTICKWFPWFDJWMAKBXBPJZUKLNONQH3MLPLLBKQ643CYN5PRW', 'market', true, false, 'manual', '{"seed_confidence":"user_verified_not_observed_in_current_index"}'::jsonb),
    ('blend', 'CCBTMXJW4BCEX2YCCOHQ5RX2C5CS6U4FZAYXFAL7LCB7GSAIHYVW4QLE', 'oracle', true, false, 'manual', '{"seed_confidence":"user_verified_not_observed_in_current_index"}'::jsonb),
    ('aquarius', 'CBCFTQSPDBAIZ6R6PJQKSQWKNKWH2QIV3I4J72SHWBIK3ADRRAM5A6GD', 'router', true, true, 'manual', '{"observed_functions":["swap_chained","swap_chained_strict_receive","init_standard_pool","init_stableswap_pool"]}'::jsonb),
    ('soroswap', 'CCJUD55AG6W5HAI5LRVNKAE5WDP5XGZBUDS5WNTIVDU7O264UZZE7BRD', 'router', true, true, 'manual', '{"observed_functions":["swap_exact_tokens_for_tokens","add_liquidity","remove_liquidity"]}'::jsonb),
    ('soroswap', 'CDP3HMUH6SMS3S7NPGNDJLULCOXXEPSHY4JKUKMBNQMATHDHWXRRJTBY', 'factory', true, false, 'manual', '{"seed_confidence":"user_verified"}'::jsonb)
ON CONFLICT (protocol_id, contract_id, role) DO UPDATE SET
    is_active = EXCLUDED.is_active,
    verified = EXCLUDED.verified,
    source = EXCLUDED.source,
    metadata_json = EXCLUDED.metadata_json,
    updated_at = now();
