-- Schema for network topology data in stellar_hot
-- Tables store Stellarbeat network scan snapshots, node measurements, and org measurements.

-- Network-level snapshot per scan
CREATE TABLE IF NOT EXISTS network_topology_snapshots (
    scan_id INTEGER PRIMARY KEY,
    scan_time TIMESTAMPTZ NOT NULL,
    latest_ledger BIGINT NOT NULL,
    latest_ledger_close_time TIMESTAMPTZ,
    -- Network measurement fields (inlined, one row per scan)
    nr_of_active_watchers SMALLINT,
    nr_of_connectable_nodes SMALLINT,
    nr_of_active_validators SMALLINT,
    nr_of_active_full_validators SMALLINT,
    nr_of_active_organizations SMALLINT,
    transitive_quorum_set_size SMALLINT,
    has_transitive_quorum_set BOOLEAN,
    has_quorum_intersection BOOLEAN,
    top_tier_size SMALLINT,
    top_tier_orgs_size SMALLINT,
    has_symmetric_top_tier BOOLEAN,
    min_blocking_set_size SMALLINT,
    min_blocking_set_filtered_size SMALLINT,
    min_blocking_set_orgs_size SMALLINT,
    min_blocking_set_orgs_filtered_size SMALLINT,
    min_blocking_set_country_size SMALLINT,
    min_blocking_set_country_filtered_size SMALLINT,
    min_blocking_set_isp_size SMALLINT,
    min_blocking_set_isp_filtered_size SMALLINT,
    min_splitting_set_size SMALLINT,
    min_splitting_set_orgs_size SMALLINT,
    min_splitting_set_country_size SMALLINT,
    min_splitting_set_isp_size SMALLINT
);

CREATE INDEX IF NOT EXISTS idx_nts_latest_ledger ON network_topology_snapshots(latest_ledger);
CREATE INDEX IF NOT EXISTS idx_nts_scan_time ON network_topology_snapshots(scan_time);

-- Per-node measurement per scan
CREATE TABLE IF NOT EXISTS node_topology_measurements (
    scan_id INTEGER REFERENCES network_topology_snapshots(scan_id),
    public_key VARCHAR(56) NOT NULL,
    is_active BOOLEAN NOT NULL,
    is_validating BOOLEAN NOT NULL,
    is_full_validator BOOLEAN NOT NULL,
    is_overloaded BOOLEAN NOT NULL,
    is_active_in_scp BOOLEAN NOT NULL,
    connectivity_error BOOLEAN NOT NULL,
    stellar_core_version_behind BOOLEAN NOT NULL,
    history_archive_has_error BOOLEAN NOT NULL,
    lag_ms SMALLINT,
    index SMALLINT NOT NULL,
    trust_centrality_score DOUBLE PRECISION,
    page_rank_score DOUBLE PRECISION,
    trust_rank INTEGER,
    PRIMARY KEY (scan_id, public_key)
);

-- Per-organization measurement per scan
CREATE TABLE IF NOT EXISTS org_topology_measurements (
    scan_id INTEGER REFERENCES network_topology_snapshots(scan_id),
    organization_id TEXT NOT NULL,
    is_sub_quorum_available BOOLEAN NOT NULL,
    index SMALLINT NOT NULL,
    toml_state TEXT,
    PRIMARY KEY (scan_id, organization_id)
);
