package main

import (
	"context"
	"fmt"
	"strconv"
	"testing"
)

type fakeHorizonServingAccountTransactionReader struct {
	rows    []AccountTransaction
	next    string
	hasMore bool
	covered bool
	err     error
	called  bool
	filters AccountTransactionsFilters
}

func (f *fakeHorizonServingAccountTransactionReader) GetServingAccountTransactions(ctx context.Context, filters AccountTransactionsFilters) ([]AccountTransaction, string, bool, bool, error) {
	f.called = true
	f.filters = filters
	return f.rows, f.next, f.hasMore, f.covered, f.err
}

type fakeHorizonFederatedAccountTransactionReader struct {
	rows     []AccountTransaction
	next     string
	hasMore  bool
	coverage AccountLedgerIndexCoverage
	err      error
	called   bool
	filters  AccountTransactionsFilters
}

func (f *fakeHorizonFederatedAccountTransactionReader) GetAccountTransactions(ctx context.Context, filters AccountTransactionsFilters) ([]AccountTransaction, string, bool, AccountLedgerIndexCoverage, error) {
	f.called = true
	f.filters = filters
	return f.rows, f.next, f.hasMore, f.coverage, f.err
}

func TestHorizonAccountTransactionReaderPrefersServingFeedWhenCovered(t *testing.T) {
	t.Setenv("ACCOUNT_TX_FEED_ENABLED", "true")

	serving := &fakeHorizonServingAccountTransactionReader{
		rows: []AccountTransaction{{
			LedgerSequence:  123,
			TransactionHash: "txhash",
			PagingToken:     "toid-cursor",
		}},
		next:    "next-toid-cursor",
		hasMore: true,
		covered: true,
	}
	federated := &fakeHorizonFederatedAccountTransactionReader{}
	reader := &HorizonAccountTransactionReader{serving: serving, federated: federated}

	rows, next, hasMore, coverage, err := reader.GetAccountTransactions(context.Background(), AccountTransactionsFilters{
		AccountID: "GACCOUNT",
		Limit:     1,
		Order:     "desc",
	})
	if err != nil {
		t.Fatalf("GetAccountTransactions: %v", err)
	}
	if !serving.called || federated.called {
		t.Fatalf("serving called=%v federated called=%v", serving.called, federated.called)
	}
	if len(rows) != 1 || rows[0].PagingToken != "toid-cursor" || next != "next-toid-cursor" || !hasMore {
		t.Fatalf("unexpected result rows=%#v next=%q hasMore=%v", rows, next, hasMore)
	}
	if !coverage.Used || coverage.Status != "serving_feed" {
		t.Fatalf("coverage = %+v", coverage)
	}
}

func TestHorizonAccountTransactionReaderFallsBackWhenFeedUncovered(t *testing.T) {
	t.Setenv("ACCOUNT_TX_FEED_ENABLED", "true")

	serving := &fakeHorizonServingAccountTransactionReader{covered: false}
	federated := &fakeHorizonFederatedAccountTransactionReader{
		rows: []AccountTransaction{{LedgerSequence: 122, TransactionHash: "txhash"}},
		coverage: AccountLedgerIndexCoverage{
			Enabled: true,
			Status:  "complete",
		},
	}
	reader := &HorizonAccountTransactionReader{serving: serving, federated: federated}

	rows, _, _, coverage, err := reader.GetAccountTransactions(context.Background(), AccountTransactionsFilters{AccountID: "GACCOUNT", Limit: 1})
	if err != nil {
		t.Fatalf("GetAccountTransactions: %v", err)
	}
	if !serving.called || !federated.called {
		t.Fatalf("serving called=%v federated called=%v", serving.called, federated.called)
	}
	if len(rows) != 1 || rows[0].TransactionHash != "txhash" || coverage.Status != "complete" {
		t.Fatalf("rows=%#v coverage=%+v", rows, coverage)
	}
}

func TestHorizonAccountTransactionReaderFallsBackWhenFeedErrors(t *testing.T) {
	t.Setenv("ACCOUNT_TX_FEED_ENABLED", "true")

	serving := &fakeHorizonServingAccountTransactionReader{err: fmt.Errorf("feed unavailable")}
	federated := &fakeHorizonFederatedAccountTransactionReader{rows: []AccountTransaction{{TransactionHash: "fallback"}}}
	reader := &HorizonAccountTransactionReader{serving: serving, federated: federated}

	rows, _, _, _, err := reader.GetAccountTransactions(context.Background(), AccountTransactionsFilters{AccountID: "GACCOUNT", Limit: 1})
	if err != nil {
		t.Fatalf("GetAccountTransactions: %v", err)
	}
	if !serving.called || !federated.called || len(rows) != 1 || rows[0].TransactionHash != "fallback" {
		t.Fatalf("serving called=%v federated called=%v rows=%#v", serving.called, federated.called, rows)
	}
}

func TestHorizonAccountTransactionPagingTokenPreservesServingTOIDCursor(t *testing.T) {
	got := horizonAccountTransactionPagingToken(AccountTransaction{PagingToken: "toid-cursor"}, "desc")
	if got != "toid-cursor" {
		t.Fatalf("paging token = %q, want serving TOID cursor", got)
	}
}

func TestDecodeHorizonHistoryCursorAcceptsRecordPagingToken(t *testing.T) {
	// A Horizon client pages with a record's paging_token, which is the numeric
	// TOID (ledger << 32 | tx-order << 12 | op-index).
	toid := (int64(456) << 32) | (int64(3) << 12)
	got, err := decodeHorizonHistoryCursor(strconv.FormatInt(toid, 10))
	if err != nil {
		t.Fatalf("decodeHorizonHistoryCursor: %v", err)
	}
	if got == nil || got.LedgerSequence != 456 || got.TieBreaker != strconv.FormatInt(toid, 10) {
		t.Fatalf("cursor = %#v, want ledger 456 with TOID tie-breaker", got)
	}

	// The collection-link base64 cursor form still decodes.
	encoded := HistoryCursor{LedgerSequence: 456, TransactionHash: "txhash", Order: "desc"}.Encode()
	round, err := decodeHorizonHistoryCursor(encoded)
	if err != nil || round == nil || round.LedgerSequence != 456 || round.TransactionHash != "txhash" {
		t.Fatalf("base64 cursor = %#v err=%v", round, err)
	}

	for _, empty := range []string{"", "now"} {
		if c, err := decodeHorizonHistoryCursor(empty); err != nil || c != nil {
			t.Fatalf("cursor(%q) = %#v err=%v, want nil", empty, c, err)
		}
	}

	if _, err := decodeHorizonHistoryCursor("!!not-a-cursor!!"); err == nil {
		t.Fatal("garbage cursor should error")
	}
}
