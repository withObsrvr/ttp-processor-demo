package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"
)

type fakeHorizonOperationReader struct {
	filters OperationFilters
	ops     []EnrichedOperation
	op      *EnrichedOperation
	err     error
}

func (f *fakeHorizonOperationReader) GetEnrichedOperationsWithCursor(ctx context.Context, filters OperationFilters) ([]EnrichedOperation, string, bool, error) {
	f.filters = filters
	return f.ops, "", false, f.err
}

func (f *fakeHorizonOperationReader) GetOperationByID(ctx context.Context, id int64) (*EnrichedOperation, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.op != nil {
		return f.op, nil
	}
	if len(f.ops) > 0 {
		return &f.ops[0], nil
	}
	return nil, errHorizonOperationNotFound
}

type fakeHorizonEffectReader struct {
	filters EffectFilters
	effects []SilverEffect
	err     error
}

func (f *fakeHorizonEffectReader) GetEffects(ctx context.Context, filters EffectFilters) ([]SilverEffect, string, bool, error) {
	f.filters = filters
	return f.effects, "", false, f.err
}

func TestHorizonTransactionOperationsFiltersAndReturnsPage(t *testing.T) {
	t.Setenv("HORIZON_COMPAT_BASE_URL", "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/horizon-compat")
	dest := "GDEST"
	amount := "12.3000000"
	reader := &fakeHorizonOperationReader{ops: []EnrichedOperation{{
		TransactionHash: "txhash",
		OperationID:     7,
		LedgerSequence:  123,
		LedgerClosedAt:  "2026-07-09T15:04:00Z",
		SourceAccount:   "GSOURCE",
		Type:            1,
		TypeName:        "PAYMENT",
		Destination:     &dest,
		Amount:          &amount,
		TxSuccessful:    true,
		IsPaymentOp:     true,
	}}}
	handlers := &HorizonCompatHandlers{operationReader: reader}
	router := mux.NewRouter()
	router.HandleFunc("/api/v1/horizon-compat/transactions/{hash}/operations", handlers.HandleTransactionOperations).Methods("GET")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/horizon-compat/transactions/txhash/operations?limit=1&order=desc", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if reader.filters.TxHash != "txhash" || reader.filters.Limit != 1 || reader.filters.Order != "desc" {
		t.Fatalf("filters = %+v", reader.filters)
	}

	var body struct {
		Links struct {
			Self struct {
				Href string `json:"href"`
			} `json:"self"`
		} `json:"_links"`
		Embedded struct {
			Records []map[string]any `json:"records"`
		} `json:"_embedded"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if len(body.Embedded.Records) != 1 {
		t.Fatalf("records len = %d", len(body.Embedded.Records))
	}
	record := body.Embedded.Records[0]
	if record["type"] != "payment" || record["transaction_hash"] != "txhash" || record["from"] != "GSOURCE" || record["to"] != "GDEST" {
		t.Fatalf("record = %#v", record)
	}
	wantSelf := "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/horizon-compat/transactions/txhash/operations?limit=1&order=desc"
	if body.Links.Self.Href != wantSelf {
		t.Fatalf("self href = %q, want %q", body.Links.Self.Href, wantSelf)
	}
}

func TestHorizonAccountPaymentsFiltersPaymentsOnly(t *testing.T) {
	reader := &fakeHorizonOperationReader{}
	handlers := &HorizonCompatHandlers{operationReader: reader}
	router := mux.NewRouter()
	router.HandleFunc("/api/v1/horizon-compat/accounts/{id}/payments", handlers.HandleAccountPayments).Methods("GET")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/horizon-compat/accounts/GACCOUNT/payments?limit=5", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if reader.filters.AccountID != "GACCOUNT" || !reader.filters.PaymentsOnly || reader.filters.Limit != 5 {
		t.Fatalf("filters = %+v", reader.filters)
	}
}

func TestHorizonTransactionEffectsFiltersAndReturnsPage(t *testing.T) {
	account := "GACCOUNT"
	amount := "5.0000000"
	issuer := "GISSUER"
	opID := int64(528280981505)
	reader := &fakeHorizonEffectReader{effects: []SilverEffect{{
		LedgerSequence:   123,
		TransactionHash:  "txhash",
		OperationIndex:   1,
		EffectIndex:      2,
		OperationID:      &opID,
		EffectType:       2,
		EffectTypeString: "account_credited",
		AccountID:        &account,
		Asset:            &AssetInfo{Code: "USD", Issuer: &issuer, Type: "credit_alphanum4"},
		Amount:           &amount,
		Timestamp:        time.Date(2026, 7, 9, 15, 4, 0, 0, time.UTC),
	}}}
	handlers := &HorizonCompatHandlers{effectReader: reader}
	router := mux.NewRouter()
	router.HandleFunc("/api/v1/horizon-compat/transactions/{hash}/effects", handlers.HandleTransactionEffects).Methods("GET")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/horizon-compat/transactions/txhash/effects?limit=2&order=asc", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if reader.filters.TransactionHash != "txhash" || reader.filters.Limit != 2 || reader.filters.Order != "asc" {
		t.Fatalf("filters = %+v", reader.filters)
	}

	var body struct {
		Embedded struct {
			Records []map[string]any `json:"records"`
		} `json:"_embedded"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if len(body.Embedded.Records) != 1 {
		t.Fatalf("records len = %d", len(body.Embedded.Records))
	}
	record := body.Embedded.Records[0]
	if record["type"] != "account_credited" || record["account"] != "GACCOUNT" || record["asset_code"] != "USD" || record["amount"] != "5.0000000" {
		t.Fatalf("record = %#v", record)
	}
}
