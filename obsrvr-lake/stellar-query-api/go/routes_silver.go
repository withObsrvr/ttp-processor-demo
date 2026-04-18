package main

import (
	"log"

	"github.com/gorilla/mux"
)

func (app *application) registerSilverRoutes(router *mux.Router) {
	silverHandlers := app.silverHandlers
	queryService := app.queryService
	unifiedSilverReader := app.unifiedSilverReader
	unifiedDuckDBReader := app.unifiedDuckDBReader
	silverHotReader := app.silverHotReader
	hotReader := app.hotReader
	coldReader := app.coldReader

	log.Println("Registering Silver API endpoints:")

	router.HandleFunc("/api/v1/silver/accounts", silverHandlers.HandleListAccounts)
	router.HandleFunc("/api/v1/silver/accounts/current", silverHandlers.HandleAccountCurrent)
	router.HandleFunc("/api/v1/silver/accounts/history", silverHandlers.HandleAccountHistory)
	router.HandleFunc("/api/v1/silver/accounts/top", silverHandlers.HandleTopAccounts)
	router.HandleFunc("/api/v1/silver/accounts/signers", silverHandlers.HandleAccountSigners)
	router.HandleFunc("/api/v1/silver/accounts/{id}/balances", silverHandlers.HandleAccountBalances).Methods("GET")
	router.HandleFunc("/api/v1/silver/accounts/{id}/offers", silverHandlers.HandleAccountOffers).Methods("GET")
	router.HandleFunc("/api/v1/silver/accounts/{id}/contracts", silverHandlers.HandleAccountContracts).Methods("GET")
	log.Println("  ✓ /api/v1/silver/accounts (list all)")
	log.Println("  ✓ /api/v1/silver/accounts/*")
	log.Println("  ✓ /api/v1/silver/accounts/signers")
	log.Println("  ✓ /api/v1/silver/accounts/{id}/balances")
	log.Println("  ✓ /api/v1/silver/accounts/{id}/offers")
	log.Println("  ✓ /api/v1/silver/accounts/{id}/contracts")

	router.HandleFunc("/api/v1/silver/assets", silverHandlers.HandleAssetList).Methods("GET")
	router.HandleFunc("/api/v1/silver/assets/{asset}/holders", silverHandlers.HandleTokenHolders).Methods("GET")
	router.HandleFunc("/api/v1/silver/assets/{asset}/stats", silverHandlers.HandleTokenStats).Methods("GET")
	log.Println("  ✓ /api/v1/silver/assets (list all assets)")
	log.Println("  ✓ /api/v1/silver/assets/{asset}/holders")
	log.Println("  ✓ /api/v1/silver/assets/{asset}/stats")

	router.HandleFunc("/api/v1/silver/operations/enriched", silverHandlers.HandleEnrichedOperations)
	router.HandleFunc("/api/v1/silver/operations/soroban/by-function", silverHandlers.HandleSorobanOpsByFunction).Methods("GET")
	router.HandleFunc("/api/v1/silver/calls", silverHandlers.HandleSorobanOpsByFunction).Methods("GET")
	router.HandleFunc("/api/v1/silver/operations/soroban", silverHandlers.HandleSorobanOperations)
	router.HandleFunc("/api/v1/silver/payments", silverHandlers.HandlePayments)
	router.HandleFunc("/api/v1/silver/transfers", silverHandlers.HandleTokenTransfers)
	router.HandleFunc("/api/v1/silver/transfers/stats", silverHandlers.HandleTokenTransferStats)
	router.HandleFunc("/api/v1/silver/explorer/account", silverHandlers.HandleAccountOverview)
	router.HandleFunc("/api/v1/silver/explorer/transaction", silverHandlers.HandleTransactionDetails)
	router.HandleFunc("/api/v1/silver/explorer/asset", silverHandlers.HandleAssetOverview)

	router.HandleFunc("/api/v1/silver/offers", silverHandlers.HandleOffers).Methods("GET")
	router.HandleFunc("/api/v1/silver/offers/pair", silverHandlers.HandleOffersByPair).Methods("GET")
	router.HandleFunc("/api/v1/silver/offers/{id}", silverHandlers.HandleOfferByID).Methods("GET")
	router.HandleFunc("/api/v1/silver/liquidity-pools", silverHandlers.HandleLiquidityPools).Methods("GET")
	router.HandleFunc("/api/v1/silver/liquidity-pools/asset", silverHandlers.HandleLiquidityPoolsByAsset).Methods("GET")
	router.HandleFunc("/api/v1/silver/liquidity-pools/{id}", silverHandlers.HandleLiquidityPoolByID).Methods("GET")
	router.HandleFunc("/api/v1/silver/claimable-balances", silverHandlers.HandleClaimableBalances).Methods("GET")
	router.HandleFunc("/api/v1/silver/claimable-balances/asset", silverHandlers.HandleClaimableBalancesByAsset).Methods("GET")
	router.HandleFunc("/api/v1/silver/claimable-balances/{id}", silverHandlers.HandleClaimableBalanceByID).Methods("GET")
	router.HandleFunc("/api/v1/silver/trades", silverHandlers.HandleTrades).Methods("GET")
	router.HandleFunc("/api/v1/silver/trades/by-pair", silverHandlers.HandleTradesByPair).Methods("GET")
	router.HandleFunc("/api/v1/silver/trades/stats", silverHandlers.HandleTradeStats).Methods("GET")
	router.HandleFunc("/api/v1/silver/effects", silverHandlers.HandleEffects).Methods("GET")
	router.HandleFunc("/api/v1/silver/effects/types", silverHandlers.HandleEffectTypes).Methods("GET")
	router.HandleFunc("/api/v1/silver/effects/transaction/{tx_hash}", silverHandlers.HandleEffectsByTransaction).Methods("GET")
	router.HandleFunc("/api/v1/silver/tx/{hash}/effects", silverHandlers.HandleEffectsByTransactionAlias).Methods("GET")
	router.HandleFunc("/api/v1/silver/soroban/contract-code", silverHandlers.HandleContractCode).Methods("GET")
	router.HandleFunc("/api/v1/silver/soroban/ttl", silverHandlers.HandleTTL).Methods("GET")
	router.HandleFunc("/api/v1/silver/soroban/ttl/resolve", silverHandlers.HandleTTLResolve).Methods("GET")
	router.HandleFunc("/api/v1/silver/soroban/ttl/expiring", silverHandlers.HandleTTLExpiring).Methods("GET")
	router.HandleFunc("/api/v1/silver/soroban/ttl/expired", silverHandlers.HandleTTLExpired).Methods("GET")
	router.HandleFunc("/api/v1/silver/soroban/evicted-keys", silverHandlers.HandleEvictedKeys).Methods("GET")
	router.HandleFunc("/api/v1/silver/soroban/restored-keys", silverHandlers.HandleRestoredKeys).Methods("GET")
	router.HandleFunc("/api/v1/silver/soroban/config", silverHandlers.HandleSorobanConfig).Methods("GET")
	router.HandleFunc("/api/v1/silver/soroban/config/limits", silverHandlers.HandleSorobanConfigLimits).Methods("GET")
	router.HandleFunc("/api/v1/silver/soroban/contract-data", silverHandlers.HandleContractData).Methods("GET")
	router.HandleFunc("/api/v1/silver/soroban/contract-data/entry", silverHandlers.HandleContractDataEntry).Methods("GET")

	router.HandleFunc("/api/v1/silver/ledgers/{seq:[0-9]+}", queryService.HandleLedgerBySequence).Methods("GET")
	router.HandleFunc("/api/v1/silver/ledger/{seq:[0-9]+}", queryService.HandleLedgerBySequence).Methods("GET")

	ledgerFullHandler := NewLedgerFullHandler(queryService, silverHotReader, unifiedSilverReader)
	router.HandleFunc("/api/v1/silver/ledger/{seq:[0-9]+}/full", ledgerFullHandler.HandleLedgerFull).Methods("GET")
	router.HandleFunc("/api/v1/silver/ledgers/{seq:[0-9]+}/full", ledgerFullHandler.HandleLedgerFull).Methods("GET")

	networkStatsHandler := NewNetworkStatsHandler(unifiedSilverReader, coldReader)
	if hotReader != nil {
		networkStatsHandler.SetBronzeHotPG(hotReader.DB())
	}
	if unifiedDuckDBReader != nil {
		networkStatsHandler.SetUnifiedReader(unifiedDuckDBReader)
		router.HandleFunc("/api/v1/bronze/stats/network", networkStatsHandler.HandleBronzeNetworkStats).Methods("GET")
	}
	router.HandleFunc("/api/v1/silver/stats/network", networkStatsHandler.HandleNetworkStats).Methods("GET")
	router.HandleFunc("/api/v1/silver/data-boundaries", silverHandlers.HandleDataBoundaries).Methods("GET")

	accountActivityHandler := NewAccountActivityHandler(unifiedSilverReader)
	router.HandleFunc("/api/v1/silver/accounts/{id}/activity", accountActivityHandler.HandleAccountActivity).Methods("GET")

	app.registerSilverContractRoutes(router)
	app.registerSilverAnalyticsRoutes(router)
	app.registerExplorerRoutes(router)
	app.registerTokenAndDecodeRoutes(router)
	app.registerGoldRoutes(router)
	app.registerSemanticRoutes(router)
}
