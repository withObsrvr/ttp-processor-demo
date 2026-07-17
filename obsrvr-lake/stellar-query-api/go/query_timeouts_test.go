package main

import (
	"testing"
	"time"
)

func TestLedgerFullQueryTimeout(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		t.Setenv("QUERY_API_LEDGER_FULL_TIMEOUT", "")
		if got := ledgerFullQueryTimeout(); got != 3500*time.Millisecond {
			t.Fatalf("ledgerFullQueryTimeout() = %s, want 3.5s", got)
		}
	})

	t.Run("duration override", func(t *testing.T) {
		t.Setenv("QUERY_API_LEDGER_FULL_TIMEOUT", "3500ms")
		if got := ledgerFullQueryTimeout(); got != 3500*time.Millisecond {
			t.Fatalf("ledgerFullQueryTimeout() = %s, want 3.5s", got)
		}
	})

	t.Run("invalid override", func(t *testing.T) {
		t.Setenv("QUERY_API_LEDGER_FULL_TIMEOUT", "not-a-duration")
		if got := ledgerFullQueryTimeout(); got != 3500*time.Millisecond {
			t.Fatalf("ledgerFullQueryTimeout() = %s, want 3.5s", got)
		}
	})
}

func TestHomeSummaryQueryTimeout(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		t.Setenv("QUERY_API_HOME_SUMMARY_TIMEOUT", "")
		if got := homeSummaryQueryTimeout(); got != 2500*time.Millisecond {
			t.Fatalf("homeSummaryQueryTimeout() = %v, want 2.5s", got)
		}
	})

	t.Run("override", func(t *testing.T) {
		t.Setenv("QUERY_API_HOME_SUMMARY_TIMEOUT", "1800ms")
		if got := homeSummaryQueryTimeout(); got != 1800*time.Millisecond {
			t.Fatalf("homeSummaryQueryTimeout() = %v, want 1.8s", got)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		t.Setenv("QUERY_API_HOME_SUMMARY_TIMEOUT", "not-a-duration")
		if got := homeSummaryQueryTimeout(); got != 2500*time.Millisecond {
			t.Fatalf("homeSummaryQueryTimeout() = %v, want default 2.5s", got)
		}
	})
}

func TestSmartWalletQueryTimeout(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		t.Setenv("QUERY_API_SMART_WALLET_TIMEOUT", "")
		if got := smartWalletQueryTimeout(); got != 1500*time.Millisecond {
			t.Fatalf("smartWalletQueryTimeout() = %v, want 1.5s", got)
		}
	})

	t.Run("override", func(t *testing.T) {
		t.Setenv("QUERY_API_SMART_WALLET_TIMEOUT", "900ms")
		if got := smartWalletQueryTimeout(); got != 900*time.Millisecond {
			t.Fatalf("smartWalletQueryTimeout() = %v, want 900ms", got)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		t.Setenv("QUERY_API_SMART_WALLET_TIMEOUT", "not-a-duration")
		if got := smartWalletQueryTimeout(); got != 1500*time.Millisecond {
			t.Fatalf("smartWalletQueryTimeout() = %v, want default 1.5s", got)
		}
	})
}
