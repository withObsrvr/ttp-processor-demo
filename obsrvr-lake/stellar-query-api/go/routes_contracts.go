package main

import (
	"github.com/gorilla/mux"
)

func (app *application) registerSilverContractRoutes(router *mux.Router) {
	unifiedSilverReader := app.unifiedSilverReader
	unifiedDuckDBReader := app.unifiedDuckDBReader
	silverHandlers := app.silverHandlers

	var contractCallHandlers *ContractCallHandlers
	if unifiedDuckDBReader != nil {
		contractCallHandlers = NewContractCallHandlersWithUnified(unifiedSilverReader, unifiedDuckDBReader)
	} else {
		contractCallHandlers = NewContractCallHandlers(unifiedSilverReader)
	}

	router.HandleFunc("/api/v1/silver/tx/{hash}/contracts-involved", contractCallHandlers.HandleContractsInvolved).Methods("GET")
	router.HandleFunc("/api/v1/silver/tx/{hash}/call-graph", contractCallHandlers.HandleCallGraph).Methods("GET")
	router.HandleFunc("/api/v1/silver/tx/{hash}/hierarchy", contractCallHandlers.HandleTransactionHierarchy).Methods("GET")
	router.HandleFunc("/api/v1/silver/tx/{hash}/contracts-summary", contractCallHandlers.HandleContractsSummary).Methods("GET")
	router.HandleFunc("/api/v1/silver/contracts/top", contractCallHandlers.HandleTopContracts).Methods("GET")
	router.HandleFunc("/api/v1/silver/stats/contracts", contractCallHandlers.HandleTopContracts).Methods("GET")
	router.HandleFunc("/api/v1/silver/contracts/{id}/recent-calls", contractCallHandlers.HandleRecentCalls).Methods("GET")
	router.HandleFunc("/api/v1/silver/contracts/{id}/callers", contractCallHandlers.HandleContractCallers).Methods("GET")
	router.HandleFunc("/api/v1/silver/contracts/{id}/callees", contractCallHandlers.HandleContractCallees).Methods("GET")
	router.HandleFunc("/api/v1/silver/contracts/{id}/call-summary", contractCallHandlers.HandleContractCallSummary).Methods("GET")
	router.HandleFunc("/api/v1/silver/contracts/{id}/analytics", contractCallHandlers.HandleContractAnalyticsSummary).Methods("GET")
	router.HandleFunc("/api/v1/silver/contracts/{id}/metadata", contractCallHandlers.HandleContractMetadata).Methods("GET")

	if unifiedDuckDBReader != nil {
		router.HandleFunc("/api/v1/silver/contracts/{id}/storage", silverHandlers.HandleContractStorage).Methods("GET")
	}
}

func (app *application) registerSilverAnalyticsRoutes(router *mux.Router) {
	unifiedDuckDBReader := app.unifiedDuckDBReader
	unifiedSilverReader := app.unifiedSilverReader
	silverHandlers := app.silverHandlers

	if unifiedDuckDBReader != nil {
		feeStatsHandler := NewFeeStatsHandler(unifiedDuckDBReader)
		router.HandleFunc("/api/v1/silver/stats/fees", feeStatsHandler.HandleFeeStats).Methods("GET")
		router.HandleFunc("/api/v1/silver/ledgers/{seq}/fees", feeStatsHandler.HandleLedgerFees).Methods("GET")
		router.HandleFunc("/api/v1/silver/ledgers/{seq}/soroban", feeStatsHandler.HandleLedgerSoroban).Methods("GET")

		sorobanStatsHandler := NewSorobanStatsHandler(unifiedDuckDBReader, unifiedSilverReader.hot)
		router.HandleFunc("/api/v1/silver/stats/soroban", sorobanStatsHandler.HandleSorobanStats).Methods("GET")
		router.HandleFunc("/api/v1/silver/ledgers/recent", silverHandlers.HandleRecentLedgers).Methods("GET")
		router.HandleFunc("/api/v1/silver/transactions/recent", silverHandlers.HandleRecentTransactions).Methods("GET")
		router.HandleFunc("/api/v1/silver/transactions/summaries", silverHandlers.HandleTransactionSummaries).Methods("GET")
	}
}
