package main

import "testing"

func TestBuildProjectorsRunsValidatorIdentityBeforeHeavyRebuilds(t *testing.T) {
	cfg := &Config{
		Service: ServiceConfig{Network: "mainnet"},
		Radar: RadarConfig{
			BaseURL:               "https://radar.withobsrvr.com/api",
			RequestTimeoutSeconds: 10,
		},
		Projectors: ProjectorsConfig{
			AssetStats:          ProjectorConfig{Enabled: true},
			ContractStats:       ProjectorConfig{Enabled: true},
			ValidatorIdentities: ProjectorConfig{Enabled: true},
		},
	}

	projectors := buildProjectors(cfg, nil, nil, nil, nil)
	if len(projectors) != 3 {
		t.Fatalf("projector count = %d, want 3", len(projectors))
	}
	if got := projectors[0].Name(); got != "validator_identities" {
		t.Fatalf("first projector = %q, want validator_identities", got)
	}
}
