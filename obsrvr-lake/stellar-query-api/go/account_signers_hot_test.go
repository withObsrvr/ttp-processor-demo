package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func TestHandleAccountSignersUsesHotPointRead(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(regexp.QuoteMeta("FROM accounts_current")).
		WithArgs("GAACCOUNT").
		WillReturnRows(sqlmock.NewRows([]string{
			"account_id", "signers", "master_weight", "low_threshold", "med_threshold", "high_threshold",
		}).AddRow("GAACCOUNT", `[{"key":"GSECONDARY","weight":2,"type":"ed25519_public_key"}]`, 1, 1, 2, 3))

	hot := &SilverHotReader{db: db}
	handlers := NewSilverHandlers(NewUnifiedSilverReader(hot, nil), nil, ReaderModeUnified)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/silver/accounts/signers?account_id=GAACCOUNT", nil)
	rec := httptest.NewRecorder()

	handlers.HandleAccountSigners(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var got AccountSignersResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(got.Signers) != 2 || got.Signers[0].Key != "GAACCOUNT" || got.Signers[1].Key != "GSECONDARY" {
		t.Fatalf("signers = %#v", got.Signers)
	}
	if got.Thresholds.LowThreshold != 1 || got.Thresholds.MedThreshold != 2 || got.Thresholds.HighThreshold != 3 {
		t.Fatalf("thresholds = %#v", got.Thresholds)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestGetAccountSignersRejectsCorruptSignerState(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(regexp.QuoteMeta("FROM accounts_current")).
		WithArgs("GAACCOUNT").
		WillReturnRows(sqlmock.NewRows([]string{
			"account_id", "signers", "master_weight", "low_threshold", "med_threshold", "high_threshold",
		}).AddRow("GAACCOUNT", "not-json", 1, 0, 0, 0))

	_, err = (&SilverHotReader{db: db}).GetAccountSigners(context.Background(), "GAACCOUNT")
	if err == nil || !strings.Contains(err.Error(), "signers unparseable") {
		t.Fatalf("error = %v, want corrupt signer-state error", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
