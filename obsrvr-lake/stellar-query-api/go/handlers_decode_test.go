package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandleDecodeScValRejectsUnknownFields(t *testing.T) {
	h := &DecodeHandlers{}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/silver/decode/scval", strings.NewReader(`{"xdr":"AAAAAQ==","unexpected":true}`))
	w := httptest.NewRecorder()

	h.HandleDecodeScVal(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", w.Code)
	}
}

func TestHandleBatchDecodedTransactionsRejectsUnknownFields(t *testing.T) {
	h := &DecodeHandlers{coldReader: &SilverColdReader{}}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/silver/tx/batch/decoded", strings.NewReader(`{"hashes":["abc"],"unexpected":true}`))
	w := httptest.NewRecorder()

	h.HandleBatchDecodedTransactions(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", w.Code)
	}
}
