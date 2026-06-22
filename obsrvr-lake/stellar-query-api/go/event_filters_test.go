package main

import (
	"net/http/httptest"
	"testing"
)

func TestParseEventFiltersValidation(t *testing.T) {
	cases := []string{
		"/api/v1/silver/events?event_type=swap",
		"/api/v1/silver/events?source_type=other",
		"/api/v1/silver/events?order=sideways",
		"/api/v1/silver/events?start_ledger=-1",
		"/api/v1/silver/events?start_ledger=0",
		"/api/v1/silver/events?start_ledger=1&end_ledger=0",
		"/api/v1/silver/events?start_ledger=10&end_ledger=9",
	}
	for _, target := range cases {
		req := httptest.NewRequest("GET", target, nil)
		if _, err := parseEventFilters(req); err == nil {
			t.Fatalf("parseEventFilters(%q) expected error", target)
		}
	}

	req := httptest.NewRequest("GET", "/api/v1/silver/events?event_type=mint&source_type=soroban&order=asc&start_ledger=1&end_ledger=2", nil)
	filters, err := parseEventFilters(req)
	if err != nil {
		t.Fatalf("parseEventFilters valid request: %v", err)
	}
	if filters.EventType != "mint" || filters.SourceType != "soroban" || filters.Order != "asc" || filters.StartLedger != 1 || filters.EndLedger != 2 {
		t.Fatalf("unexpected filters: %#v", filters)
	}
}

func TestParseGenericEventFiltersValidation(t *testing.T) {
	cases := []string{
		"/api/v1/silver/events/generic?event_type=transfer",
		"/api/v1/silver/events/generic?order=sideways",
		"/api/v1/silver/events/generic?start_ledger=nope",
		"/api/v1/silver/events/generic?end_ledger=0",
		"/api/v1/silver/events/generic?start_ledger=10&end_ledger=9",
	}
	for _, target := range cases {
		req := httptest.NewRequest("GET", target, nil)
		if _, err := parseGenericEventFilters(req); err == nil {
			t.Fatalf("parseGenericEventFilters(%q) expected error", target)
		}
	}

	req := httptest.NewRequest("GET", "/api/v1/silver/events/generic?event_type=contract&topic0=transfer&order=desc&start_ledger=1&end_ledger=2", nil)
	filters, err := parseGenericEventFilters(req)
	if err != nil {
		t.Fatalf("parseGenericEventFilters valid request: %v", err)
	}
	if filters.EventType == nil || *filters.EventType != "contract" || filters.Topic0 == nil || *filters.Topic0 != "transfer" || filters.Order != "desc" {
		t.Fatalf("unexpected filters: %#v", filters)
	}
}

func TestEventCoverageDocumentsSemanticVsRawStreams(t *testing.T) {
	unified := unifiedEventCoverage()
	if unified.Version == "" || len(unified.Includes) == 0 || len(unified.Limitations) == 0 {
		t.Fatalf("incomplete unified coverage: %#v", unified)
	}
	generic := genericEventCoverage()
	if generic.Version == "" || len(generic.Includes) == 0 || len(generic.Limitations) == 0 {
		t.Fatalf("incomplete generic coverage: %#v", generic)
	}
}
