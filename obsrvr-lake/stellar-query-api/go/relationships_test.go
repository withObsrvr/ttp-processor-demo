package main

import (
	"strings"
	"testing"
	"time"
)

func TestRelationshipCursorRoundTrip(t *testing.T) {
	want := RelationshipCursor{
		LedgerSequence:  3207008,
		ClosedAt:        time.Date(2026, 6, 21, 12, 34, 56, 789, time.UTC),
		TransactionHash: "0eb7ae2ec92cfd6350db651d576d4a0951c97bc684c2a53d6c4d3c34fab87789",
		Order:           "desc",
	}
	got, err := DecodeRelationshipCursor(want.Encode())
	if err != nil {
		t.Fatalf("DecodeRelationshipCursor returned error: %v", err)
	}
	if got.LedgerSequence != want.LedgerSequence || !got.ClosedAt.Equal(want.ClosedAt) || got.TransactionHash != want.TransactionHash || got.Order != want.Order {
		t.Fatalf("cursor round trip mismatch: got %+v want %+v", got, want)
	}
}

func TestBuildRelationshipQueryKeepsColdToExistingTables(t *testing.T) {
	query := buildRelationshipQuery("hot_db.public", "cold_db.silver", "1=1", "DESC", 3)
	if !strings.Contains(query, "hot_db.public.contract_invocation_calls") {
		t.Fatalf("expected hot contract_invocation_calls in query")
	}
	if strings.Contains(query, "cold_db.silver.contract_invocation_calls") {
		t.Fatalf("cold schema does not currently define contract_invocation_calls; query must not reference it")
	}
	for _, table := range []string{"token_transfers_raw", "semantic_flows_value", "contract_invocations_raw", "semantic_activities"} {
		if !strings.Contains(query, "cold_db.silver."+table) {
			t.Fatalf("expected cold table %s in query", table)
		}
	}
}
