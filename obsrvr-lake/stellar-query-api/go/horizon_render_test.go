package main

import (
	"net/http/httptest"
	"testing"
)

func TestHorizonCompatLinkBuilderPreservesProxyPrefix(t *testing.T) {
	t.Setenv("HORIZON_COMPAT_BASE_URL", "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/horizon-compat")
	req := httptest.NewRequest("GET", "/api/v1/horizon-compat/transactions/abc", nil)

	links := newHorizonCompatLinkBuilder(req)

	got := links.Link("/transactions", "abc")
	if got.Href != "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/horizon-compat/transactions/abc" {
		t.Fatalf("href = %q", got.Href)
	}

	paged := links.PagedLink("/transactions", "abc", "effects")
	wantPaged := "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/horizon-compat/transactions/abc/effects{?cursor,limit,order}"
	if paged.Href != wantPaged || !paged.Templated {
		t.Fatalf("paged link = %+v, want href %q templated", paged, wantPaged)
	}
}

func TestHorizonCompatLinkBuilderUsesForwardedScheme(t *testing.T) {
	t.Setenv("HORIZON_COMPAT_BASE_URL", "")
	req := httptest.NewRequest("GET", "/api/v1/horizon-compat/transactions/abc", nil)
	req.Host = "obsrvr-lake-testnet.withobsrvr.com"
	req.Header.Set("X-Forwarded-Proto", "https")

	links := newHorizonCompatLinkBuilder(req)
	got := links.Link("/transactions", "abc")

	if got.Href != "https://obsrvr-lake-testnet.withobsrvr.com/api/v1/horizon-compat/transactions/abc" {
		t.Fatalf("href = %q", got.Href)
	}
}
