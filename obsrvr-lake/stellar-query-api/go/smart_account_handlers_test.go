package main

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/gorilla/mux"
)

func TestNormalizeSmartAccountCredential(t *testing.T) {
	if got := normalizeSmartAccountCredential("0xDEADBEEF"); got != "deadbeef" {
		t.Fatalf("hex credential = %q, want deadbeef", got)
	}
	encoded := base64.RawURLEncoding.EncodeToString([]byte{0xde, 0xad, 0xbe, 0xef})
	if got := normalizeSmartAccountCredential(encoded); got != "deadbeef" {
		t.Fatalf("base64url credential = %q, want deadbeef", got)
	}
}

func TestHandleSmartAccountLookupByCredentialUsesServing(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	updated := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta("FROM serving.sv_watermarks")).
		WithArgs("serving.sv_smart_account_signers").
		WillReturnRows(sqlmock.NewRows([]string{"table_name", "status", "complete_from", "complete_thru", "updated_at"}).
			AddRow("serving.sv_smart_account_signers", "complete", int64(3), int64(100), updated))
	mock.ExpectQuery(regexp.QuoteMeta("FROM serving.sv_smart_account_signers s")).
		WithArgs("credential", "deadbeef", 100).
		WillReturnRows(sqlmock.NewRows([]string{
			"contract_id",
			"wallet_type",
			"context_rule_count",
			"active_signer_count",
			"credential_signer_count",
			"address_signer_count",
			"active_policy_count",
			"context_rule_ids",
			"first_seen_ledger",
			"last_modified_ledger",
		}).AddRow(
			"CACCOUNT",
			"openzeppelin",
			1,
			2,
			1,
			1,
			1,
			"{7}",
			int64(100),
			int64(120),
		))

	handler := &SmartWalletHandlers{hotReader: &SilverHotReader{db: db}}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/silver/smart-accounts/lookup/credential/deadbeef", nil)
	req = mux.SetURLVars(req, map[string]string{"credential_id": "deadbeef"})
	rec := httptest.NewRecorder()

	handler.HandleSmartAccountLookupByCredential(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var resp SmartAccountLookupResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if resp.Source != "serving.sv_smart_account_signers" {
		t.Fatalf("source = %q, want serving.sv_smart_account_signers", resp.Source)
	}
	if resp.Coverage == nil || resp.Coverage.CompleteThru != 100 {
		t.Fatalf("coverage = %#v, want complete thru 100", resp.Coverage)
	}
	if resp.Count != 1 || resp.Contracts[0].ContractID != "CACCOUNT" {
		t.Fatalf("response = %#v", resp)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHandleSmartAccountLookupByCredential(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(regexp.QuoteMeta("FROM serving.sv_watermarks")).
		WithArgs("serving.sv_smart_account_signers").
		WillReturnRows(sqlmock.NewRows([]string{"table_name", "status", "complete_from", "complete_thru", "updated_at"}))

	mock.ExpectQuery(regexp.QuoteMeta("WITH active_rules")).
		WithArgs("deadbeef", 100).
		WillReturnRows(sqlmock.NewRows([]string{
			"contract_id",
			"wallet_type",
			"context_rule_count",
			"active_signer_count",
			"credential_signer_count",
			"address_signer_count",
			"active_policy_count",
			"context_rule_ids",
			"first_seen_ledger",
			"last_modified_ledger",
		}).AddRow(
			"CACCOUNT",
			"openzeppelin",
			1,
			2,
			1,
			1,
			1,
			"{7}",
			int64(100),
			int64(120),
		))

	handler := &SmartWalletHandlers{hotReader: &SilverHotReader{db: db}}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/silver/smart-accounts/lookup/credential/3q2-7w", nil)
	req = mux.SetURLVars(req, map[string]string{"credential_id": base64.RawURLEncoding.EncodeToString([]byte{0xde, 0xad, 0xbe, 0xef})})
	rec := httptest.NewRecorder()

	handler.HandleSmartAccountLookupByCredential(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var resp SmartAccountLookupResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if resp.Normalized != "deadbeef" {
		t.Fatalf("normalized = %q, want deadbeef", resp.Normalized)
	}
	if resp.Count != 1 || len(resp.Contracts) != 1 {
		t.Fatalf("count/contracts = %d/%d, want 1/1", resp.Count, len(resp.Contracts))
	}
	got := resp.Contracts[0]
	if got.ContractID != "CACCOUNT" || got.ContextRuleCount != 1 || len(got.ContextRuleIDs) != 1 || got.ContextRuleIDs[0] != 7 {
		t.Fatalf("contract summary = %#v", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
