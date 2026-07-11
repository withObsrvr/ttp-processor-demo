package main

import (
	"context"
	"fmt"
	"log"
)

type horizonServingAccountTransactionReader interface {
	GetServingAccountTransactions(context.Context, AccountTransactionsFilters) ([]AccountTransaction, string, bool, bool, error)
}

// HorizonAccountTransactionReader keeps the Horizon compatibility route on the
// same account transaction serving feed as the silver route, with the federated
// reader as a compatibility fallback for uncovered ranges. The serving path is
// gated on ACCOUNT_TX_FEED_ENABLED (default off): without it every request
// takes the federated path.
type HorizonAccountTransactionReader struct {
	serving   horizonServingAccountTransactionReader
	federated horizonAccountTransactionReader
}

func NewHorizonAccountTransactionReader(serving *SilverHotReader, federated *UnifiedDuckDBReader) *HorizonAccountTransactionReader {
	out := &HorizonAccountTransactionReader{}
	if serving != nil {
		out.serving = serving
	}
	if federated != nil {
		out.federated = federated
	}
	return out
}

func (r *HorizonAccountTransactionReader) GetAccountTransactions(ctx context.Context, filters AccountTransactionsFilters) ([]AccountTransaction, string, bool, AccountLedgerIndexCoverage, error) {
	if r == nil {
		return nil, "", false, AccountLedgerIndexCoverage{}, fmt.Errorf("horizon account transactions reader is not configured")
	}
	if accountTransactionFeedEnabled() && r.serving != nil {
		rows, next, hasMore, covered, err := r.serving.GetServingAccountTransactions(ctx, filters)
		if err == nil && covered {
			return rows, next, hasMore, AccountLedgerIndexCoverage{
				Enabled:        true,
				PruningEnabled: true,
				Status:         "serving_feed",
				Complete:       true,
				Used:           true,
			}, nil
		}
		if err != nil {
			log.Printf("horizon account transaction feed unavailable account=%s err=%v; falling back to federated path", filters.AccountID, err)
		}
	}
	if r.federated != nil {
		return r.federated.GetAccountTransactions(ctx, filters)
	}
	return nil, "", false, AccountLedgerIndexCoverage{}, fmt.Errorf("horizon account transactions require serving feed or unified reader")
}
