package main

import (
	"net/http/httptest"
	"testing"
)

func TestParseHorizonPageQueryDefaults(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/v1/horizon-compat/transactions", nil)

	got, err := parseHorizonPageQuery(req)
	if err != nil {
		t.Fatalf("parseHorizonPageQuery: %v", err)
	}
	if got.Cursor != "" || got.Order != "asc" || got.Limit != defaultHorizonLimit {
		t.Fatalf("query = %+v", got)
	}
}

func TestParseHorizonPageQueryValidatesOrderAndLimit(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/v1/horizon-compat/transactions?order=sideways&limit=10", nil)
	if _, err := parseHorizonPageQuery(req); err == nil {
		t.Fatalf("expected invalid order error")
	}

	req = httptest.NewRequest("GET", "/api/v1/horizon-compat/transactions?limit=0", nil)
	if _, err := parseHorizonPageQuery(req); err == nil {
		t.Fatalf("expected invalid limit error")
	}
}

func TestParseHorizonPageQueryClampsLimit(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/v1/horizon-compat/transactions?order=DESC&cursor=abc&limit=9999", nil)

	got, err := parseHorizonPageQuery(req)
	if err != nil {
		t.Fatalf("parseHorizonPageQuery: %v", err)
	}
	if got.Cursor != "abc" || got.Order != "desc" || got.Limit != maxHorizonLimit {
		t.Fatalf("query = %+v", got)
	}
}
