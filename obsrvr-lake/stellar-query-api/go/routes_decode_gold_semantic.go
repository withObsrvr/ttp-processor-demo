package main

import "github.com/gorilla/mux"

func (app *application) registerTokenAndDecodeRoutes(router *mux.Router) {
	unifiedSilverReader := app.unifiedSilverReader
	silverHotReader := app.silverHotReader
	coldReader := app.coldReader
	hotReader := app.hotReader

	var txHotPathReader *TxHotPathReader
	if hotReader != nil && silverHotReader != nil {
		txHotPathReader = NewTxHotPathReader(hotReader.DB(), silverHotReader.DB())
	}

	eventHandlers := NewEventHandlers(unifiedSilverReader.cold, txHotPathReader)
	router.HandleFunc("/api/v1/silver/events", eventHandlers.HandleUnifiedEvents).Methods("GET")
	router.HandleFunc("/api/v1/silver/events/by-contract", eventHandlers.HandleContractEvents).Methods("GET")
	router.HandleFunc("/api/v1/silver/address/{addr}/events", eventHandlers.HandleAddressEvents).Methods("GET")
	router.HandleFunc("/api/v1/silver/tx/{hash}/events", eventHandlers.HandleTransactionEvents).Methods("GET")

	sep41Handlers := NewSEP41Handlers(unifiedSilverReader.cold)
	router.HandleFunc("/api/v1/silver/tokens/{contract_id}/balances", sep41Handlers.HandleTokenBalances).Methods("GET")
	router.HandleFunc("/api/v1/silver/tokens/{contract_id}/balance/{address}", sep41Handlers.HandleSingleBalance).Methods("GET")
	router.HandleFunc("/api/v1/silver/tokens/{contract_id}/transfers", sep41Handlers.HandleTokenTransfers).Methods("GET")
	router.HandleFunc("/api/v1/silver/tokens/{contract_id}/stats", sep41Handlers.HandleTokenStats).Methods("GET")
	router.HandleFunc("/api/v1/silver/tokens/{contract_id}", sep41Handlers.HandleTokenMetadata).Methods("GET")
	router.HandleFunc("/api/v1/silver/address/{addr}/token-balances", sep41Handlers.HandleAddressTokenPortfolio).Methods("GET")

	decodeHandlers := NewDecodeHandlers(silverHotReader, unifiedSilverReader.cold, coldReader, unifiedSilverReader, txHotPathReader)
	router.HandleFunc("/api/v1/silver/tx/batch/decoded", decodeHandlers.HandleBatchDecodedTransactions).Methods("GET", "POST")
	router.HandleFunc("/api/v1/silver/tx/{hash}/decoded", decodeHandlers.HandleDecodedTransaction).Methods("GET")
	router.HandleFunc("/api/v1/silver/tx/{hash}/semantic", decodeHandlers.HandleSemanticTransaction).Methods("GET")
	router.HandleFunc("/api/v1/silver/tx/{hash}/full", decodeHandlers.HandleFullTransaction).Methods("GET")
	router.HandleFunc("/api/v1/silver/contracts/{id}/interface", decodeHandlers.HandleContractInterface).Methods("GET")
	router.HandleFunc("/api/v1/silver/decode/scval", decodeHandlers.HandleDecodeScVal).Methods("POST")

	// Prism tx-detail page fast path: single PK lookup into serving.sv_tx_receipts
	// instead of 5–7 parallel cold-Parquet queries. Returns 404 if the row hasn't
	// been materialized yet; frontend should fall back to the per-section endpoints
	// above when that happens.
	txReceiptHandlers := NewTxReceiptHandlers(silverHotReader)
	router.HandleFunc("/api/v1/silver/tx/{hash}/receipt", txReceiptHandlers.HandleTxReceipt).Methods("GET")
}

func (app *application) registerGoldRoutes(router *mux.Router) {
	unifiedSilverReader := app.unifiedSilverReader

	goldHandlers := NewGoldHandlers(unifiedSilverReader)
	router.HandleFunc("/api/v1/gold/snapshots/account", goldHandlers.HandleAccountSnapshot).Methods("GET")
	router.HandleFunc("/api/v1/gold/snapshots/balance", goldHandlers.HandleAssetHolders).Methods("GET")
	router.HandleFunc("/api/v1/gold/snapshots/portfolio", goldHandlers.HandlePortfolioSnapshot).Methods("GET")
	router.HandleFunc("/api/v1/gold/snapshots/accounts/batch", goldHandlers.HandleBatchAccounts).Methods("POST")

	complianceHandlers := NewComplianceHandlers(unifiedSilverReader)
	router.HandleFunc("/api/v1/gold/compliance/transactions", complianceHandlers.HandleTransactionArchive).Methods("GET")
	router.HandleFunc("/api/v1/gold/compliance/balances", complianceHandlers.HandleBalanceArchive).Methods("GET")
	router.HandleFunc("/api/v1/gold/compliance/supply", complianceHandlers.HandleSupplyTimeline).Methods("GET")
	router.HandleFunc("/api/v1/gold/compliance/archive", complianceHandlers.HandleFullArchive).Methods("POST")
	router.HandleFunc("/api/v1/gold/compliance/archive/{id}", complianceHandlers.HandleArchiveStatus).Methods("GET")
	router.HandleFunc("/api/v1/gold/compliance/archive/{id}/download/{artifact}", complianceHandlers.HandleArchiveDownload).Methods("GET")
	router.HandleFunc("/api/v1/gold/compliance/lineage", complianceHandlers.HandleLineage).Methods("GET")
}

func (app *application) registerSemanticRoutes(router *mux.Router) {
	unifiedSilverReader := app.unifiedSilverReader
	unifiedDuckDBReader := app.unifiedDuckDBReader

	semanticHandlers := NewSemanticHandlers(unifiedSilverReader)
	if unifiedDuckDBReader != nil {
		semanticHandlers.SetDuckDBReader(unifiedDuckDBReader)
	}
	router.HandleFunc("/api/v1/semantic/activities", semanticHandlers.HandleSemanticActivities).Methods("GET")
	router.HandleFunc("/api/v1/semantic/contracts", semanticHandlers.HandleSemanticContracts).Methods("GET")
	router.HandleFunc("/api/v1/semantic/flows", semanticHandlers.HandleSemanticFlows).Methods("GET")
	router.HandleFunc("/api/v1/semantic/contracts/functions", semanticHandlers.HandleSemanticContractFunctions).Methods("GET")
	router.HandleFunc("/api/v1/semantic/assets", semanticHandlers.HandleSemanticAssets).Methods("GET")
	router.HandleFunc("/api/v1/semantic/dex/pairs", semanticHandlers.HandleSemanticDexPairs).Methods("GET")
	router.HandleFunc("/api/v1/semantic/accounts/summary", semanticHandlers.HandleSemanticAccountSummary).Methods("GET")
	router.HandleFunc("/api/v1/semantic/tokens/{contract_id}", semanticHandlers.HandleSemanticTokenSummary).Methods("GET")
	router.HandleFunc("/api/v1/semantic/defi/protocols", semanticHandlers.HandleDefiProtocols).Methods("GET")
	router.HandleFunc("/api/v1/semantic/defi/markets", semanticHandlers.HandleDefiMarkets).Methods("GET")
	router.HandleFunc("/api/v1/semantic/defi/status", semanticHandlers.HandleDefiStatus).Methods("GET")
	router.HandleFunc("/api/v1/semantic/defi/exposure", semanticHandlers.HandleDefiExposure).Methods("GET")
	router.HandleFunc("/api/v1/semantic/defi/positions", semanticHandlers.HandleDefiPositions).Methods("GET")
	router.HandleFunc("/api/v1/semantic/defi/positions/{position_id}", semanticHandlers.HandleDefiPosition).Methods("GET")
}
