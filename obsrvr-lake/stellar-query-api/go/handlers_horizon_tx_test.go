package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/gorilla/mux"
)

func TestHorizonTransactionHandlerReturnsHALTransaction(t *testing.T) {
	t.Setenv("HORIZON_COMPAT_BASE_URL", "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/horizon-compat")

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(regexp.QuoteMeta(horizonHotTransactionQuery)).
		WithArgs("txhash").
		WillReturnRows(sqlmock.NewRows(horizonTxColumns).AddRow(
			int64(123),
			"txhash",
			int64(528280977408),
			"GACCOUNT",
			nil,
			int64(99),
			int64(100),
			int64(1000),
			true,
			int64(2),
			"none",
			nil,
			time.Date(2026, 7, 9, 15, 4, 0, 0, time.UTC),
			"AAAA-envelope",
			"AAAA-result",
			"AAAA-meta",
			"AAAA-fee-meta",
			`["sig1"]`,
			nil,
			nil,
		))

	handlers := &HorizonCompatHandlers{txReader: &HorizonTransactionReader{hot: db}}
	router := mux.NewRouter()
	router.HandleFunc("/api/v1/horizon-compat/transactions/{hash}", handlers.HandleTransaction).Methods("GET")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/horizon-compat/transactions/txhash", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "application/hal+json" {
		t.Fatalf("content-type = %q", got)
	}

	var body struct {
		Links struct {
			Self struct {
				Href string `json:"href"`
			} `json:"self"`
			Operations struct {
				Href      string `json:"href"`
				Templated bool   `json:"templated"`
			} `json:"operations"`
		} `json:"_links"`
		ID string `json:"id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if body.ID != "txhash" {
		t.Fatalf("id = %q", body.ID)
	}
	wantSelf := "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/horizon-compat/transactions/txhash"
	if body.Links.Self.Href != wantSelf {
		t.Fatalf("self href = %q, want %q", body.Links.Self.Href, wantSelf)
	}
	wantOps := wantSelf + "/operations{?cursor,limit,order}"
	if body.Links.Operations.Href != wantOps || !body.Links.Operations.Templated {
		t.Fatalf("operations link = %+v, want href %q templated", body.Links.Operations, wantOps)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
