package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
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

func TestWriteHorizonJSONNormalizesEmbeddedRecordsKey(t *testing.T) {
	var page struct {
		Embedded struct {
			Records []map[string]string
		} `json:"_embedded"`
	}
	page.Embedded.Records = []map[string]string{{"id": "1"}}

	rec := httptest.NewRecorder()
	if err := writeHorizonJSON(rec, http.StatusOK, page); err != nil {
		t.Fatalf("writeHorizonJSON: %v", err)
	}

	var top map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &top); err != nil {
		t.Fatalf("json.Unmarshal top: %v", err)
	}
	var embedded map[string]json.RawMessage
	if err := json.Unmarshal(top["_embedded"], &embedded); err != nil {
		t.Fatalf("json.Unmarshal embedded: %v", err)
	}
	if _, ok := embedded["Records"]; ok {
		t.Fatalf("unexpected SDK-style Records key in response: %s", rec.Body.String())
	}
	recordsRaw, ok := embedded["records"]
	if !ok {
		t.Fatalf("missing Horizon records key in response: %s", rec.Body.String())
	}
	var records []map[string]string
	if err := json.Unmarshal(recordsRaw, &records); err != nil {
		t.Fatalf("json.Unmarshal records: %v", err)
	}
	if len(records) != 1 || records[0]["id"] != "1" {
		t.Fatalf("records = %#v", records)
	}
}

func TestHorizonCompatCollectionLinksPagingRoundTrip(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/horizon-compat/operations?limit=2&order=desc&cursor=999", nil)
	page := horizonPageQuery{Cursor: "999", Order: "desc", Limit: 2}

	links := horizonCompatCollectionLinks(req, page, "111", "222")

	// next pages forward from the LAST record's token in the same order;
	// prev pages backward from the FIRST record's token with inverted order.
	assertLinkQuery := func(name, href, wantCursor, wantOrder string) {
		t.Helper()
		parsed, err := url.Parse(href)
		if err != nil {
			t.Fatalf("%s href %q: %v", name, href, err)
		}
		q := parsed.Query()
		if q.Get("cursor") != wantCursor || q.Get("order") != wantOrder || q.Get("limit") != "2" {
			t.Fatalf("%s = cursor:%q order:%q limit:%q, want cursor:%q order:%q limit:2",
				name, q.Get("cursor"), q.Get("order"), q.Get("limit"), wantCursor, wantOrder)
		}
	}
	assertLinkQuery("self", links.Self.Href, "999", "desc")
	assertLinkQuery("next", links.Next.Href, "222", "desc")
	assertLinkQuery("prev", links.Prev.Href, "111", "asc")
}

func TestInvertHorizonOrder(t *testing.T) {
	if invertHorizonOrder("desc") != "asc" || invertHorizonOrder("asc") != "desc" || invertHorizonOrder("") != "desc" {
		t.Fatal("order inversion broken")
	}
}
