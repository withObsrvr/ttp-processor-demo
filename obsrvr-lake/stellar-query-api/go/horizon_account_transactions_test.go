package main

import (
	"context"
	"fmt"
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
