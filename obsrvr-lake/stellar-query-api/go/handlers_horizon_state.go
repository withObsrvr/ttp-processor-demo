package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	protocol "github.com/stellar/go-stellar-sdk/protocols/horizon"
)

func (h *HorizonCompatHandlers) HandleAccount(w http.ResponseWriter, r *http.Request) {
	if h.accountReader == nil {
		renderHorizonProblem(w, r, horizonProblem(
			http.StatusServiceUnavailable,
			"data_unavailable",
			"Data Unavailable",
			"Horizon compatibility account lookup requires the unified silver reader.",
		))
		return
	}

	accountID := mux.Vars(r)["id"]
	if accountID == "" {
		renderHorizonProblem(w, r, horizonProblem(http.StatusBadRequest, "bad_request", "Bad Request", "account id is required"))
		return
	}

	ctx, cancel := withInteractiveQueryTimeout(r.Context())
	defer cancel()
	account, err := h.accountReader.GetHorizonAccount(ctx, accountID)
	if err != nil {
		switch {
		case errors.Is(err, errHorizonAccountNotFound):
			renderHorizonProblem(w, r, horizonProblem(http.StatusNotFound, "not_found", "Resource Missing", "Account not found."))
		case errors.Is(err, errHorizonAccountDataUnavailable):
			renderHorizonProblem(w, r, horizonProblem(http.StatusServiceUnavailable, "data_unavailable", "Data Unavailable", err.Error()))
		case isQueryTimeout(err):
			renderHorizonProblem(w, r, horizonProblem(http.StatusGatewayTimeout, "timeout", "Timeout", err.Error()))
		default:
			renderHorizonProblem(w, r, horizonProblem(http.StatusInternalServerError, "server_error", "Internal Server Error", err.Error()))
		}
		return
	}

	populateHorizonAccountLinks(r, account)
	if err := writeHorizonJSON(w, http.StatusOK, account); err != nil {
		renderHorizonProblem(w, r, horizonProblem(http.StatusInternalServerError, "server_error", "Internal Server Error", err.Error()))
	}
}

func (h *HorizonCompatHandlers) HandleAccountTransactions(w http.ResponseWriter, r *http.Request) {
	if h.accountTransactionReader == nil || h.txReader == nil || !h.txReader.Available() {
		renderHorizonProblem(w, r, horizonProblem(
			http.StatusServiceUnavailable,
			"data_unavailable",
			"Data Unavailable",
			"Horizon compatibility account transactions require account-history silver data and bronze transaction XDR.",
		))
		return
	}

	page, err := parseHorizonPageQuery(r)
	if err != nil {
		renderHorizonProblem(w, r, horizonProblem(http.StatusBadRequest, "bad_request", "Bad Request", err.Error()))
		return
	}
	cursor, err := decodeHorizonHistoryCursor(page.Cursor)
	if err != nil {
		renderHorizonProblem(w, r, horizonProblem(http.StatusBadRequest, "bad_request", "Bad Request", err.Error()))
		return
	}

	accountID := mux.Vars(r)["id"]
	filters := AccountTransactionsFilters{
		AccountID: accountID,
		Limit:     int(page.Limit),
		Order:     page.Order,
		Cursor:    cursor,
	}

	ctx, cancel := withInteractiveQueryTimeout(r.Context())
	defer cancel()
	rows, _, _, _, err := h.accountTransactionReader.GetAccountTransactions(ctx, filters)
	if err != nil {
		if isQueryTimeout(err) {
			renderHorizonProblem(w, r, horizonProblem(http.StatusGatewayTimeout, "timeout", "Timeout", err.Error()))
			return
		}
		renderHorizonProblem(w, r, horizonProblem(http.StatusInternalServerError, "server_error", "Internal Server Error", err.Error()))
		return
	}

	records := make([]protocol.Transaction, 0, len(rows))
	for _, row := range rows {
		var tx *protocol.Transaction
		var err error
		hydrateCtx, hydrateCancel := context.WithTimeout(ctx, horizonAccountTransactionHydrationTimeout())
		if row.TransactionID > 0 {
			tx, err = h.txReader.GetTransactionByIDAtLedger(hydrateCtx, row.TransactionID, row.LedgerSequence)
		} else {
			tx, err = h.txReader.GetTransactionByHashAtLedger(hydrateCtx, row.TransactionHash, row.LedgerSequence)
		}
		hydrateCancel()
		if err != nil {
			switch {
			case isQueryTimeout(err), errors.Is(err, errHorizonTransactionXDRUnavailable), errors.Is(err, errHorizonTransactionReaderUnavailable), errors.Is(err, errHorizonTransactionNotFound):
				renderHorizonProblem(w, r, horizonProblem(http.StatusServiceUnavailable, "data_unavailable", "Data Unavailable", err.Error()))
			default:
				renderHorizonProblem(w, r, horizonProblem(http.StatusInternalServerError, "server_error", "Internal Server Error", err.Error()))
			}
			return
		}
		populateHorizonTransactionLinks(r, tx)
		records = append(records, *tx)
	}

	var firstCursor, lastCursor string
	if len(rows) > 0 {
		firstCursor = horizonAccountTransactionPagingToken(rows[0], page.Order)
		lastCursor = horizonAccountTransactionPagingToken(rows[len(rows)-1], page.Order)
	}

	var out protocol.TransactionsPage
	out.Links = horizonCompatCollectionLinks(r, page, firstCursor, lastCursor)
	out.Embedded.Records = records
	if err := writeHorizonJSON(w, http.StatusOK, out); err != nil {
		renderHorizonProblem(w, r, horizonProblem(http.StatusInternalServerError, "server_error", "Internal Server Error", err.Error()))
	}
}

func horizonAccountTransactionHydrationTimeout() time.Duration {
	return 1500 * time.Millisecond
}

func horizonLedgerDetailTimeout() time.Duration {
	return 15 * time.Second
}

func (h *HorizonCompatHandlers) HandleLedger(w http.ResponseWriter, r *http.Request) {
	if h.ledgerReader == nil {
		renderHorizonProblem(w, r, horizonProblem(
			http.StatusServiceUnavailable,
			"data_unavailable",
			"Data Unavailable",
			"Horizon compatibility ledger lookup requires bronze ledger data.",
		))
		return
	}
	sequence, err := strconv.ParseInt(mux.Vars(r)["sequence"], 10, 64)
	if err != nil || sequence <= 0 {
		renderHorizonProblem(w, r, horizonProblem(http.StatusBadRequest, "bad_request", "Bad Request", "ledger sequence must be a positive integer"))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), horizonLedgerDetailTimeout())
	defer cancel()
	ledger, err := h.ledgerReader.GetLedger(ctx, sequence)
	if err != nil {
		switch {
		case errors.Is(err, errHorizonLedgerNotFound):
			renderHorizonProblem(w, r, horizonProblem(http.StatusNotFound, "not_found", "Resource Missing", "Ledger not found."))
		case isQueryTimeout(err):
			renderHorizonProblem(w, r, horizonProblem(http.StatusGatewayTimeout, "timeout", "Timeout", err.Error()))
		default:
			renderHorizonProblem(w, r, horizonProblem(http.StatusInternalServerError, "server_error", "Internal Server Error", err.Error()))
		}
		return
	}
	populateHorizonLedgerLinks(r, ledger)
	if err := writeHorizonJSON(w, http.StatusOK, ledger); err != nil {
		renderHorizonProblem(w, r, horizonProblem(http.StatusInternalServerError, "server_error", "Internal Server Error", err.Error()))
	}
}

func (h *HorizonCompatHandlers) HandleLedgers(w http.ResponseWriter, r *http.Request) {
	if h.ledgerReader == nil {
		renderHorizonProblem(w, r, horizonProblem(
			http.StatusServiceUnavailable,
			"data_unavailable",
			"Data Unavailable",
			"Horizon compatibility ledger collections require bronze ledger data.",
		))
		return
	}
	page, err := parseHorizonPageQuery(r)
	if err != nil {
		renderHorizonProblem(w, r, horizonProblem(http.StatusBadRequest, "bad_request", "Bad Request", err.Error()))
		return
	}

	ctx, cancel := withInteractiveQueryTimeout(r.Context())
	defer cancel()
	records, err := h.ledgerReader.GetLedgers(ctx, page)
	if err != nil {
		if isQueryTimeout(err) {
			renderHorizonProblem(w, r, horizonProblem(http.StatusGatewayTimeout, "timeout", "Timeout", err.Error()))
			return
		}
		renderHorizonProblem(w, r, horizonProblem(http.StatusInternalServerError, "server_error", "Internal Server Error", err.Error()))
		return
	}

	for i := range records {
		populateHorizonLedgerLinks(r, &records[i])
	}
	var firstCursor, lastCursor string
	if len(records) > 0 {
		firstCursor = records[0].PT
		lastCursor = records[len(records)-1].PT
	}

	var out protocol.LedgersPage
	out.Links = horizonCompatCollectionLinks(r, page, firstCursor, lastCursor)
	out.Embedded.Records = records
	if err := writeHorizonJSON(w, http.StatusOK, out); err != nil {
		renderHorizonProblem(w, r, horizonProblem(http.StatusInternalServerError, "server_error", "Internal Server Error", err.Error()))
	}
}

func (h *HorizonCompatHandlers) HandleFeeStats(w http.ResponseWriter, r *http.Request) {
	if h.feeStatsReader == nil {
		renderHorizonProblem(w, r, horizonProblem(
			http.StatusServiceUnavailable,
			"data_unavailable",
			"Data Unavailable",
			"Horizon compatibility fee stats require bronze ledger and transaction data.",
		))
		return
	}

	ctx, cancel := withInteractiveQueryTimeout(r.Context())
	defer cancel()
	stats, err := h.feeStatsReader.GetFeeStats(ctx)
	if err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			renderHorizonProblem(w, r, horizonProblem(http.StatusServiceUnavailable, "data_unavailable", "Data Unavailable", "No ledger data is available for fee stats."))
		case isQueryTimeout(err):
			renderHorizonProblem(w, r, horizonProblem(http.StatusGatewayTimeout, "timeout", "Timeout", err.Error()))
		default:
			renderHorizonProblem(w, r, horizonProblem(http.StatusInternalServerError, "server_error", "Internal Server Error", err.Error()))
		}
		return
	}
	if err := writeHorizonJSON(w, http.StatusOK, stats); err != nil {
		renderHorizonProblem(w, r, horizonProblem(http.StatusInternalServerError, "server_error", "Internal Server Error", err.Error()))
	}
}

func populateHorizonAccountLinks(r *http.Request, account *protocol.Account) {
	links := newHorizonCompatLinkBuilder(r)
	account.Links.Self = links.Link("/accounts", account.AccountID)
	account.Links.Transactions = links.PagedLink("/accounts", account.AccountID, "transactions")
	account.Links.Operations = links.PagedLink("/accounts", account.AccountID, "operations")
	account.Links.Payments = links.PagedLink("/accounts", account.AccountID, "payments")
	account.Links.Effects = links.PagedLink("/accounts", account.AccountID, "effects")
	account.Links.Offers = links.PagedLink("/accounts", account.AccountID, "offers")
	account.Links.Trades = links.PagedLink("/accounts", account.AccountID, "trades")
	account.Links.Data = links.Linkf("/accounts/%s/data/{key}", account.AccountID)
}

func populateHorizonLedgerLinks(r *http.Request, ledger *protocol.Ledger) {
	links := newHorizonCompatLinkBuilder(r)
	seq := fmt.Sprintf("%d", ledger.Sequence)
	ledger.Links.Self = links.Link("/ledgers", seq)
	ledger.Links.Transactions = links.PagedLink("/ledgers", seq, "transactions")
	ledger.Links.Operations = links.PagedLink("/ledgers", seq, "operations")
	ledger.Links.Payments = links.PagedLink("/ledgers", seq, "payments")
	ledger.Links.Effects = links.PagedLink("/ledgers", seq, "effects")
}

func decodeHorizonHistoryCursor(raw string) (*HistoryCursor, error) {
	if raw == "" || raw == "now" {
		return nil, nil
	}
	// Horizon clients page with a record's paging_token, which for transactions
	// is the numeric TOID. The serving feed pages exactly on the TOID tie-breaker;
	// the federated fallback pages at ledger granularity (the TOID's high 32 bits),
	// which can skip same-ledger sibling transactions only when serving coverage
	// is unavailable.
	if toid, err := strconv.ParseInt(raw, 10, 64); err == nil && toid > 0 {
		return &HistoryCursor{
			LedgerSequence: toid >> 32,
			TieBreaker:     raw,
		}, nil
	}
	return DecodeHistoryCursor(raw)
}

func horizonAccountTransactionPagingToken(tx AccountTransaction, order string) string {
	if tx.PagingToken != "" {
		return tx.PagingToken
	}
	ts, _ := time.Parse(time.RFC3339Nano, tx.ClosedAt)
	if ts.IsZero() {
		ts = parseHorizonTimestamp(tx.ClosedAt)
	}
	return HistoryCursor{
		LedgerSequence:  tx.LedgerSequence,
		ClosedAt:        ts,
		TransactionHash: tx.TransactionHash,
		Order:           order,
	}.Encode()
}
