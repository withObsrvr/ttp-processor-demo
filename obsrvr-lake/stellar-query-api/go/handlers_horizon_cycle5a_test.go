package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	protocol "github.com/stellar/go-stellar-sdk/protocols/horizon"
	hbase "github.com/stellar/go-stellar-sdk/protocols/horizon/base"
)

type fakeHorizonAccountReader struct {
	accountID string
	account   *protocol.Account
	err       error
}

func (f *fakeHorizonAccountReader) GetHorizonAccount(ctx context.Context, accountID string) (*protocol.Account, error) {
	f.accountID = accountID
	return f.account, f.err
}

type fakeHorizonLedgerReader struct {
	sequence int64
	page     horizonPageQuery
	ledger   *protocol.Ledger
	ledgers  []protocol.Ledger
	err      error
}

func (f *fakeHorizonLedgerReader) GetLedger(ctx context.Context, sequence int64) (*protocol.Ledger, error) {
	f.sequence = sequence
	return f.ledger, f.err
}

func (f *fakeHorizonLedgerReader) GetLedgers(ctx context.Context, page horizonPageQuery) ([]protocol.Ledger, error) {
	f.page = page
	return f.ledgers, f.err
}

type fakeHorizonFeeStatsReader struct {
	stats *protocol.FeeStats
	err   error
}

func (f *fakeHorizonFeeStatsReader) GetFeeStats(ctx context.Context) (*protocol.FeeStats, error) {
	return f.stats, f.err
}

func TestHorizonAccountRootReturnsSDKShapeAndLinks(t *testing.T) {
	t.Setenv("HORIZON_COMPAT_BASE_URL", "https://gateway.withobsrvr.com/lake/v1/testnet/api/v1/horizon-compat")
	account := &protocol.Account{
		ID:        "GACCOUNT",
		AccountID: "GACCOUNT",
		Sequence:  123,
		Balances: []protocol.Balance{{
			Balance: "10.0000000",
			Asset:   hbase.Asset{Type: "native"},
		}},
		Signers: []protocol.Signer{{Key: "GACCOUNT", Type: "ed25519_public_key", Weight: 1}},
		Data:    map[string]string{},
		PT:      "GACCOUNT",
	}
	reader := &fakeHorizonAccountReader{account: account}
	handlers := &HorizonCompatHandlers{accountReader: reader}
	router := mux.NewRouter()
	router.HandleFunc("/api/v1/horizon-compat/accounts/{id}", handlers.HandleAccount).Methods("GET")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/horizon-compat/accounts/GACCOUNT", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if reader.accountID != "GACCOUNT" {
		t.Fatalf("accountID = %q", reader.accountID)
	}

	var body struct {
		AccountID string `json:"account_id"`
		Sequence  string `json:"sequence"`
		Links     struct {
			Transactions struct {
				Href string `json:"href"`
			} `json:"transactions"`
			Data struct {
				Href      string `json:"href"`
				Templated bool   `json:"templated"`
			} `json:"data"`
		} `json:"_links"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if body.AccountID != "GACCOUNT" || body.Sequence != "123" {
		t.Fatalf("body = %#v", body)
	}
	if !strings.Contains(body.Links.Transactions.Href, "/accounts/GACCOUNT/transactions") {
		t.Fatalf("transactions link = %q", body.Links.Transactions.Href)
	}
	if !body.Links.Data.Templated || !strings.Contains(body.Links.Data.Href, "/accounts/GACCOUNT/data/{key}") {
		t.Fatalf("data link = %#v", body.Links.Data)
	}
}

func TestHorizonLedgerDetailReturnsLinks(t *testing.T) {
	reader := &fakeHorizonLedgerReader{ledger: &protocol.Ledger{
		ID:       "ledgerhash",
		Hash:     "ledgerhash",
		Sequence: 123,
		PT:       "528280977408",
		ClosedAt: time.Date(2026, 7, 9, 15, 4, 0, 0, time.UTC),
	}}
	handlers := &HorizonCompatHandlers{ledgerReader: reader}
	router := mux.NewRouter()
	router.HandleFunc("/api/v1/horizon-compat/ledgers/{sequence:[0-9]+}", handlers.HandleLedger).Methods("GET")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/horizon-compat/ledgers/123", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if reader.sequence != 123 {
		t.Fatalf("sequence = %d", reader.sequence)
	}
	var body struct {
		Sequence int32 `json:"sequence"`
		Links    struct {
			Self struct {
				Href string `json:"href"`
			} `json:"self"`
			Transactions struct {
				Href string `json:"href"`
			} `json:"transactions"`
		} `json:"_links"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if body.Sequence != 123 || !strings.Contains(body.Links.Self.Href, "/ledgers/123") || !strings.Contains(body.Links.Transactions.Href, "/ledgers/123/transactions") {
		t.Fatalf("body = %#v", body)
	}
}

func TestHorizonLedgersCollectionParsesPaging(t *testing.T) {
	reader := &fakeHorizonLedgerReader{ledgers: []protocol.Ledger{{
		ID:       "ledgerhash",
		Hash:     "ledgerhash",
		Sequence: 123,
		PT:       "528280977408",
	}}}
	handlers := &HorizonCompatHandlers{ledgerReader: reader}
	router := mux.NewRouter()
	router.HandleFunc("/api/v1/horizon-compat/ledgers", handlers.HandleLedgers).Methods("GET")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/horizon-compat/ledgers?limit=2&order=desc&cursor=528280977408", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if reader.page.Limit != 2 || reader.page.Order != "desc" || reader.page.Cursor != "528280977408" {
		t.Fatalf("page = %+v", reader.page)
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
}

func TestHorizonFeeStatsReturnsDistribution(t *testing.T) {
	reader := &fakeHorizonFeeStatsReader{stats: &protocol.FeeStats{
		LastLedger:        123,
		LastLedgerBaseFee: 100,
		FeeCharged:        protocol.FeeDistribution{Min: 100, Max: 200, Mode: 100},
		MaxFee:            protocol.FeeDistribution{Min: 100, Max: 300, Mode: 100},
	}}
	handlers := &HorizonCompatHandlers{feeStatsReader: reader}
	router := mux.NewRouter()
	router.HandleFunc("/api/v1/horizon-compat/fee_stats", handlers.HandleFeeStats).Methods("GET")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/horizon-compat/fee_stats", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var body struct {
		LastLedger string `json:"last_ledger"`
		FeeCharged struct {
			Max string `json:"max"`
		} `json:"fee_charged"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if body.LastLedger != "123" || body.FeeCharged.Max != "200" {
		t.Fatalf("body = %#v", body)
	}
}

func TestHorizonOperationDetailAndEffectsFilter(t *testing.T) {
	opID := int64(528280981505)
	opReader := &fakeHorizonOperationReader{op: &EnrichedOperation{
		TransactionHash: "txhash",
		OperationID:     opID,
		LedgerSequence:  123,
		LedgerClosedAt:  "2026-07-09T15:04:00Z",
		SourceAccount:   "GSOURCE",
		Type:            0,
		TypeName:        "CREATE_ACCOUNT",
		TxSuccessful:    true,
	}}
	effectReader := &fakeHorizonEffectReader{}
	handlers := &HorizonCompatHandlers{operationReader: opReader, effectReader: effectReader}
	router := mux.NewRouter()
	router.HandleFunc("/api/v1/horizon-compat/operations/{id:[0-9]+}", handlers.HandleOperation).Methods("GET")
	router.HandleFunc("/api/v1/horizon-compat/operations/{id:[0-9]+}/effects", handlers.HandleOperationEffects).Methods("GET")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/horizon-compat/operations/528280981505", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("operation status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var opBody struct {
		ID    string `json:"id"`
		Links struct {
			Effects struct {
				Href string `json:"href"`
			} `json:"effects"`
		} `json:"_links"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &opBody); err != nil {
		t.Fatalf("json.Unmarshal operation: %v", err)
	}
	if opBody.ID != "528280981505" || !strings.Contains(opBody.Links.Effects.Href, "/operations/528280981505/effects") {
		t.Fatalf("opBody = %#v", opBody)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/horizon-compat/operations/528280981505/effects?limit=3", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("effects status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if effectReader.filters.OperationID == nil || *effectReader.filters.OperationID != opID || effectReader.filters.Limit != 3 {
		t.Fatalf("filters = %+v", effectReader.filters)
	}
}
