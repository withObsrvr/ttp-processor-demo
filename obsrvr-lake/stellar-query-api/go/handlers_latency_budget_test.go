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
)

func TestHandleSmartWalletInfoReturnsPartialWhenBudgetExpires(t *testing.T) {
	t.Setenv("QUERY_API_SMART_WALLET_TIMEOUT", "10ms")
	const contractID = "CDASVYLQISK4WX7NC6C3GTUSYVZFOWQHSPWVGRP6R4COVQ2DK4K2SFBV"
	seenContractID := make(chan string, 1)

	handler := &SmartWalletHandlers{
		infoLookup: func(ctx context.Context, gotContractID string) (*SmartWalletInfo, error) {
			seenContractID <- gotContractID
			<-ctx.Done()
			time.Sleep(300 * time.Millisecond)
			return &SmartWalletInfo{ContractID: gotContractID}, nil
		},
	}
	request := mux.SetURLVars(
		httptest.NewRequest(http.MethodGet, "/api/v1/silver/smart-wallet/"+contractID, nil),
		map[string]string{"contract_id": contractID},
	)
	recorder := httptest.NewRecorder()

	started := time.Now()
	handler.HandleSmartWalletInfo(recorder, request)
	if elapsed := time.Since(started); elapsed > 150*time.Millisecond {
		t.Fatalf("handler returned after %s, want bounded partial response", elapsed)
	}
	if got := <-seenContractID; got != contractID {
		t.Fatalf("contract ID = %q, want %q", got, contractID)
	}
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}

	var response SmartWalletInfo
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !response.Partial {
		t.Fatalf("partial = false, want true: %+v", response)
	}
	if len(response.Warnings) != 1 || !strings.Contains(response.Warnings[0], "budget exhausted") {
		t.Fatalf("warnings = %#v, want budget warning", response.Warnings)
	}
}

func TestCollectLedgerFullResultsDrainsBufferedResultsAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ch := make(chan ledgerFullResult, 2)
	ch <- ledgerFullResult{key: "ledger", data: "ledger"}
	ch <- ledgerFullResult{key: "operations", data: []string{"operation"}}

	collected, warnings := collectLedgerFullResults(ctx, ch)
	for _, key := range []string{"ledger", "operations"} {
		if _, ok := collected[key]; !ok {
			t.Fatalf("collector dropped buffered %s result after cancellation: %#v", key, collected)
		}
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "budget exhausted") {
		t.Fatalf("warnings = %#v, want budget warning", warnings)
	}
}

func TestCollectLedgerFullResultsReturnsAtContextDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	ch := make(chan ledgerFullResult, 1)
	ch <- ledgerFullResult{key: "operations", data: []string{"operation"}}

	started := time.Now()
	collected, warnings := collectLedgerFullResults(ctx, ch)
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("collector returned after %s, want context-bounded response", elapsed)
	}
	if _, ok := collected["operations"]; !ok {
		t.Fatalf("collector dropped completed result: %#v", collected)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "budget exhausted") {
		t.Fatalf("warnings = %#v, want budget warning", warnings)
	}
}
