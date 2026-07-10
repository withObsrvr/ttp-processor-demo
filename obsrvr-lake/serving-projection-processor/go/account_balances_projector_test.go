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
