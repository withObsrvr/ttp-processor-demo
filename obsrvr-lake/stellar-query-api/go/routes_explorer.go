package main

import (
	"log"

	"github.com/gorilla/mux"
)

func (app *application) registerExplorerRoutes(router *mux.Router) {
	coldReader := app.coldReader
	silverHotReader := app.silverHotReader
	hotReader := app.hotReader
	unifiedSilverReader := app.unifiedSilverReader
	unifiedDuckDBReader := app.unifiedDuckDBReader
	indexReader := app.indexReader

	var txHotPathReader *TxHotPathReader
	if hotReader != nil && silverHotReader != nil {
		txHotPathReader = NewTxHotPathReader(hotReader.DB(), silverHotReader.DB())
	}

	genericEventHandlers := NewGenericEventHandlers(coldReader, silverHotReader)
	router.HandleFunc("/api/v1/silver/events/generic", genericEventHandlers.HandleGenericEvents).Methods("GET")
	router.HandleFunc("/api/v1/silver/events/contract/{contract_id}", genericEventHandlers.HandleContractGenericEvents).Methods("GET")

	eventClassifier, err := NewEventClassifier(silverHotReader.DB())
	if err != nil {
		log.Printf("WARNING: failed to create event classifier: %v (explorer events will use fallback)", err)
	}
	if eventClassifier != nil {
		explorerEventHandlers := NewExplorerEventHandlers(coldReader, silverHotReader, eventClassifier)
		router.HandleFunc("/api/v1/explorer/events", explorerEventHandlers.HandleExplorerEvents).Methods("GET")
		router.HandleFunc("/api/v1/explorer/events/rules", explorerEventHandlers.HandleExplorerEventRules).Methods("GET")
		router.HandleFunc("/api/v1/explorer/events/rules/reload", requireAdmin(explorerEventHandlers.HandleExplorerEventRulesReload)).Methods("POST")
	}

	if unifiedDuckDBReader != nil {
		registryHandlers := NewContractRegistryHandlers(silverHotReader.DB())
		router.HandleFunc("/api/v1/explorer/contracts/search", registryHandlers.HandleSearchContracts).Methods("GET")
		router.HandleFunc("/api/v1/explorer/contracts/seed", requireAdmin(registryHandlers.HandleSeedRegistry)).Methods("POST")
		router.HandleFunc("/api/v1/explorer/contracts/{id}", registryHandlers.HandleGetContract).Methods("GET")
		router.HandleFunc("/api/v1/explorer/contracts/{id}", requireAdmin(registryHandlers.HandleDeleteContract)).Methods("DELETE")
		router.HandleFunc("/api/v1/explorer/contracts", registryHandlers.HandleListContracts).Methods("GET")
		router.HandleFunc("/api/v1/explorer/contracts", requireAdmin(registryHandlers.HandleUpsertContract)).Methods("POST")

		searchHandlers := NewSearchHandlers(silverHotReader, unifiedSilverReader.cold)
		router.HandleFunc("/api/v1/silver/search", searchHandlers.HandleSearch).Methods("GET")

		txDiffHandlers := NewTxDiffHandlers(coldReader, txHotPathReader, indexReader)
		router.HandleFunc("/api/v1/silver/tx/{hash}/diffs", txDiffHandlers.HandleTransactionDiffs).Methods("GET")

		priceHandlers := NewPriceHandlers(silverHotReader)
		router.HandleFunc("/api/v1/silver/prices/pairs", priceHandlers.HandleTradePairs).Methods("GET")
		router.HandleFunc("/api/v1/silver/prices/{base}/{counter}/ohlc", priceHandlers.HandleOHLCCandles).Methods("GET")
		router.HandleFunc("/api/v1/silver/prices/{base}/{counter}/latest", priceHandlers.HandleLatestPrice).Methods("GET")

		smartWalletHandlers := NewSmartWalletHandlers(silverHotReader, unifiedSilverReader.cold, coldReader)
		smartWalletHandlers.SetUnifiedDuckDBReader(unifiedDuckDBReader)
		router.HandleFunc("/api/v1/silver/smart-wallet/{contract_id}", smartWalletHandlers.HandleSmartWalletInfo).Methods("GET")
		router.HandleFunc("/api/v1/silver/smart-wallets", smartWalletHandlers.HandleSmartWalletsList).Methods("GET")
		router.HandleFunc("/api/v1/silver/smart-wallets/{contract_id}", smartWalletHandlers.HandleSmartWalletDetail).Methods("GET")
		router.HandleFunc("/api/v1/silver/smart-wallets/{contract_id}/balances", smartWalletHandlers.HandleSmartWalletBalances).Methods("GET")

		contractTxHandlers := NewContractTransactionsHandlers(silverHotReader)
		router.HandleFunc("/api/v1/silver/contracts/{contract_id}/transactions", contractTxHandlers.HandleContractTransactions).Methods("GET")
	}
}
