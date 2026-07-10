package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	protocol "github.com/stellar/go-stellar-sdk/protocols/horizon"
)

type HorizonCompatHandlers struct {
	txReader                 *HorizonTransactionReader
	accountReader            horizonAccountReader
	accountTransactionReader horizonAccountTransactionReader
	ledgerReader             horizonLedgerReader
	feeStatsReader           horizonFeeStatsReader
	operationReader          horizonOperationReader
	effectReader             horizonEffectReader
}

type horizonOperationReader interface {
	GetEnrichedOperationsWithCursor(context.Context, OperationFilters) ([]EnrichedOperation, string, bool, error)
	GetOperationByID(context.Context, int64) (*EnrichedOperation, error)
}

type horizonEffectReader interface {
	GetEffects(context.Context, EffectFilters) ([]SilverEffect, string, bool, error)
}

type horizonAccountReader interface {
	GetHorizonAccount(context.Context, string) (*protocol.Account, error)
}

type horizonAccountTransactionReader interface {
	GetAccountTransactions(context.Context, AccountTransactionsFilters) ([]AccountTransaction, string, bool, AccountLedgerIndexCoverage, error)
}

type horizonLedgerReader interface {
	GetLedger(context.Context, int64) (*protocol.Ledger, error)
	GetLedgers(context.Context, horizonPageQuery) ([]protocol.Ledger, error)
}

type horizonFeeStatsReader interface {
	GetFeeStats(context.Context) (*protocol.FeeStats, error)
}

func NewHorizonCompatHandlers(app *application) *HorizonCompatHandlers {
	return &HorizonCompatHandlers{
		txReader:                 NewHorizonTransactionReader(app.hotReader, app.coldReader, app.indexReader),
		accountReader:            NewHorizonAccountReader(app.silverHotReader, app.unifiedDuckDBReader),
		accountTransactionReader: NewHorizonAccountTransactionReader(app.silverHotReader, app.unifiedDuckDBReader),
		ledgerReader:             NewHorizonLedgerReader(app.queryService, app.unifiedDuckDBReader),
		feeStatsReader:           NewHorizonFeeStatsReader(app.hotReader, app.coldReader),
		operationReader:          NewHorizonOperationReader(app.unifiedDuckDBReader),
		effectReader:             app.unifiedDuckDBReader,
	}
}

func (h *HorizonCompatHandlers) HandleTransaction(w http.ResponseWriter, r *http.Request) {
	if h.txReader == nil || !h.txReader.Available() {
		renderHorizonProblem(w, r, horizonProblem(
			http.StatusServiceUnavailable,
			"data_unavailable",
			"Data Unavailable",
			"Horizon compatibility transaction lookup requires a bronze hot or cold reader.",
		))
		return
	}

	hash := mux.Vars(r)["hash"]
	if hash == "" {
		renderHorizonProblem(w, r, horizonProblem(http.StatusBadRequest, "bad_request", "Bad Request", "transaction hash is required"))
		return
	}

	tx, err := h.txReader.GetTransactionByHash(r.Context(), hash)
	if err != nil {
		switch {
		case errors.Is(err, errHorizonTransactionNotFound):
			renderHorizonProblem(w, r, horizonProblem(http.StatusNotFound, "not_found", "Resource Missing", "Transaction not found."))
		case errors.Is(err, errHorizonTransactionXDRUnavailable), errors.Is(err, errHorizonTransactionReaderUnavailable):
			renderHorizonProblem(w, r, horizonProblem(http.StatusServiceUnavailable, "data_unavailable", "Data Unavailable", err.Error()))
		default:
			renderHorizonProblem(w, r, horizonProblem(http.StatusInternalServerError, "server_error", "Internal Server Error", err.Error()))
		}
		return
	}

	populateHorizonTransactionLinks(r, tx)
	if err := writeHorizonJSON(w, http.StatusOK, tx); err != nil {
		renderHorizonProblem(w, r, horizonProblem(http.StatusInternalServerError, "server_error", "Internal Server Error", err.Error()))
	}
}

func populateHorizonTransactionLinks(r *http.Request, tx *protocol.Transaction) {
	links := newHorizonCompatLinkBuilder(r)
	tx.Links.Self = links.Link("/transactions", tx.ID)
	tx.Links.Transaction = tx.Links.Self
	tx.Links.Account = links.Link("/accounts", tx.Account)
	tx.Links.Ledger = links.Link("/ledgers", fmt.Sprintf("%d", tx.Ledger))
	tx.Links.Operations = links.PagedLink("/transactions", tx.ID, "operations")
	tx.Links.Effects = links.PagedLink("/transactions", tx.ID, "effects")
	tx.Links.Succeeds = links.Linkf("/transactions?order=desc&cursor=%s", tx.PT)
	tx.Links.Precedes = links.Linkf("/transactions?order=asc&cursor=%s", tx.PT)
}
