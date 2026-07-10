package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/gorilla/mux"
	protocol "github.com/stellar/go-stellar-sdk/protocols/horizon"
	"github.com/stellar/go-stellar-sdk/toid"
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

type fakeHorizonTransactionReader struct {
	available bool
	hash      string
	tx        *protocol.Transaction
	err       error
}

func (f *fakeHorizonTransactionReader) Available() bool {
	return f.available
}

func (f *fakeHorizonTransactionReader) GetTransactionByHash(ctx context.Context, hash string) (*protocol.Transaction, error) {
	f.hash = hash
	return f.tx, f.err
}

func (f *fakeHorizonTransactionReader) GetTransactionByHashAtLedger(ctx context.Context, hash string, ledgerSeq int64) (*protocol.Transaction, error) {
	f.hash = hash
	return f.tx, f.err
}

func (f *fakeHorizonTransactionReader) GetTransactionByIDAtLedger(ctx context.Context, transactionID, ledgerSeq int64) (*protocol.Transaction, error) {
	return f.tx, f.err
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
	if record["id"] != "7" || record["paging_token"] != "7" {
		t.Fatalf("operation id/paging token = %#v", record)
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

func TestHorizonTransactionOperationsUsesHorizonInvokeHostFunctionShape(t *testing.T) {
	argsJSON := `[{"type":"symbol","value":"AAAA"}]`
	contractID := "CCONTRACT"
	functionName := "transfer"
	reader := &fakeHorizonOperationReader{ops: []EnrichedOperation{{
		TransactionHash:   "txhash",
		OperationID:       528280981505,
		LedgerSequence:    123,
		LedgerClosedAt:    "2026-07-09T15:04:00Z",
		SourceAccount:     "GSOURCE",
		Type:              24,
		TypeName:          "INVOKE_HOST_FUNCTION",
		TxSuccessful:      true,
		IsSorobanOp:       true,
		SorobanContractID: &contractID,
		SorobanFunction:   &functionName,
		SorobanArgsJSON:   &argsJSON,
	}}}
	handlers := &HorizonCompatHandlers{operationReader: reader}
	router := mux.NewRouter()
	router.HandleFunc("/api/v1/horizon-compat/transactions/{hash}/operations", handlers.HandleTransactionOperations).Methods("GET")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/horizon-compat/transactions/txhash/operations?limit=1", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
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
	params, _ := record["parameters"].([]any)
	if record["type"] != "invoke_host_function" ||
		record["address"] != "" ||
		record["function"] != "HostFunctionTypeHostFunctionTypeInvokeContract" ||
		len(params) != 1 {
		t.Fatalf("record = %#v", record)
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
	if reader.filters.TransactionHash != "txhash" || reader.filters.Limit != 2 || reader.filters.Order != "asc" || !reader.filters.HorizonOrder {
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
	if record["type"] != "account_credited" ||
		record["account"] != "GACCOUNT" ||
		record["asset_code"] != "USD" ||
		record["amount"] != "5.0000000" ||
		record["id"] != "0000000528280981505-0000000003" ||
		record["paging_token"] != "528280981505-3" {
		t.Fatalf("record = %#v", record)
	}
}

func TestDecodeHorizonEffectCursorPair(t *testing.T) {
	cursor, err := decodeHorizonEffectCursor("528280981505-3")
	if err != nil {
		t.Fatalf("decodeHorizonEffectCursor: %v", err)
	}
	if cursor == nil || cursor.OperationID == nil || *cursor.OperationID != 528280981505 || cursor.EffectIndex != 2 {
		t.Fatalf("cursor = %+v", cursor)
	}
}

func TestHorizonTransactionEffectsAddsLedgerFilterWhenTransactionFound(t *testing.T) {
	effectReader := &fakeHorizonEffectReader{}
	txReader := &fakeHorizonTransactionReader{
		available: true,
		tx:        &protocol.Transaction{Hash: "txhash", Ledger: 3177525},
	}
	handlers := &HorizonCompatHandlers{txReader: txReader, effectReader: effectReader}
	router := mux.NewRouter()
	router.HandleFunc("/api/v1/horizon-compat/transactions/{hash}/effects", handlers.HandleTransactionEffects).Methods("GET")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/horizon-compat/transactions/txhash/effects?limit=2", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if txReader.hash != "txhash" {
		t.Fatalf("txReader hash = %q", txReader.hash)
	}
	if effectReader.filters.TransactionHash != "txhash" || effectReader.filters.LedgerSequence != 3177525 {
		t.Fatalf("filters = %+v", effectReader.filters)
	}
}

func TestDecodeHorizonOperationCursorAcceptsNumericPagingToken(t *testing.T) {
	id := toid.New(123, 4, 1).ToInt64()

	cursor, err := decodeHorizonOperationCursor(strconv.FormatInt(id, 10))
	if err != nil {
		t.Fatalf("decodeHorizonOperationCursor: %v", err)
	}
	if cursor == nil || cursor.LedgerSequence != 123 || cursor.OperationIndex != id {
		t.Fatalf("cursor = %+v, want ledger 123 operation id %d", cursor, id)
	}
}
