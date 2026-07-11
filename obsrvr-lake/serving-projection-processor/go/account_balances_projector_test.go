package main

import "testing"

func TestBalanceProjectionStepsPreserveTrustlinesOnAccountOnlyChange(t *testing.T) {
	projectXLM, projectTrustlines := balanceProjectionSteps(changedAccount{
		AccountID:      "GA",
		Ledger:         100,
		AccountChanged: true,
	})

	if !projectXLM {
		t.Fatalf("expected account-only change to refresh native XLM balance")
	}
	if projectTrustlines {
		t.Fatalf("account-only change must not rebuild trustlines from hot-only state")
	}
}

func TestBalanceProjectionStepsRefreshTrustlinesOnTrustlineChange(t *testing.T) {
	projectXLM, projectTrustlines := balanceProjectionSteps(changedAccount{
		AccountID:        "GA",
		Ledger:           100,
		TrustlineChanged: true,
	})

	if projectXLM {
		t.Fatalf("trustline-only change should not require native XLM refresh")
	}
	if !projectTrustlines {
		t.Fatalf("expected trustline change to refresh observed trustline rows")
	}
}

func TestBalanceProjectionStepsRefreshBothWhenBothChanged(t *testing.T) {
	projectXLM, projectTrustlines := balanceProjectionSteps(changedAccount{
		AccountID:        "GA",
		Ledger:           100,
		AccountChanged:   true,
		TrustlineChanged: true,
	})

	if !projectXLM || !projectTrustlines {
		t.Fatalf("both changes should refresh native and trustline rows, got XLM=%v trustlines=%v", projectXLM, projectTrustlines)
	}
}

func TestBalanceRemovalAssetKey(t *testing.T) {
	code := "USDC"
	issuer := "GISSUER"
	assetType := "credit_alphanum4"
	poolDetails := `{"limit":"0.0000000","asset_type":"liquidity_pool","liquidity_pool_id":"abc123"}`
	classicDetails := `{"limit":"0.0000000","asset_code":"USDC","asset_issuer":"GISSUER","asset_type":"credit_alphanum4"}`

	key, err := balanceRemovalAssetKey(&assetType, &code, &issuer, &classicDetails)
	if err != nil || key != "USDC:GISSUER" {
		t.Fatalf("classic trustline key = %q err=%v, want USDC:GISSUER", key, err)
	}

	lpType := "liquidity_pool"
	key, err = balanceRemovalAssetKey(&lpType, nil, nil, &poolDetails)
	if err != nil || key != "POOL:abc123" {
		t.Fatalf("pool share key = %q err=%v, want POOL:abc123", key, err)
	}

	// A removal effect with no identifiable asset must fail loudly rather than
	// deleting nothing (stale balance) or the wrong row.
	if _, err := balanceRemovalAssetKey(nil, nil, nil, nil); err == nil {
		t.Fatal("unidentifiable removal must error")
	}
}
