package main

import "testing"

func TestNormalizeTTLEntryForCurrentLedger(t *testing.T) {
	t.Run("expired when live_until is in the past even if stored flag was false", func(t *testing.T) {
		entry := &TTLEntry{
			LiveUntilLedger: 100,
			Expired:         false,
		}
		normalizeTTLEntryForCurrentLedger(entry, 150)
		if entry.LedgersRemaining != -50 {
			t.Fatalf("expected ledgers_remaining=-50, got %d", entry.LedgersRemaining)
		}
		if !entry.Expired {
			t.Fatalf("expected expired=true")
		}
	})

	t.Run("not expired when live_until is still ahead", func(t *testing.T) {
		entry := &TTLEntry{
			LiveUntilLedger: 250,
			Expired:         true,
		}
		normalizeTTLEntryForCurrentLedger(entry, 150)
		if entry.LedgersRemaining != 100 {
			t.Fatalf("expected ledgers_remaining=100, got %d", entry.LedgersRemaining)
		}
		if entry.Expired {
			t.Fatalf("expected expired=false")
		}
	})
}
