package main

import (
	"log"

	"github.com/gorilla/mux"
)

func (app *application) registerHorizonCompatRoutes(router *mux.Router) {
	handlers := NewHorizonCompatHandlers(app)
	sub := router.PathPrefix("/api/v1/horizon-compat").Subrouter()

	sub.HandleFunc("/transactions/{hash}", handlers.HandleTransaction).Methods("GET")
	sub.HandleFunc("/transactions/{hash}/operations", handlers.HandleTransactionOperations).Methods("GET")
	sub.HandleFunc("/transactions/{hash}/payments", handlers.HandleTransactionPayments).Methods("GET")
	sub.HandleFunc("/transactions/{hash}/effects", handlers.HandleTransactionEffects).Methods("GET")
	sub.HandleFunc("/accounts/{id}/operations", handlers.HandleAccountOperations).Methods("GET")
	sub.HandleFunc("/accounts/{id}/payments", handlers.HandleAccountPayments).Methods("GET")
	sub.HandleFunc("/accounts/{id}/effects", handlers.HandleAccountEffects).Methods("GET")
	sub.HandleFunc("/operations", handlers.HandleOperations).Methods("GET")
	sub.HandleFunc("/payments", handlers.HandlePayments).Methods("GET")
	sub.HandleFunc("/effects", handlers.HandleEffects).Methods("GET")

	log.Println("Registering Horizon compatibility endpoints:")
	log.Println("  ✓ /api/v1/horizon-compat/transactions/{hash}")
	log.Println("  ✓ /api/v1/horizon-compat/transactions/{hash}/operations")
	log.Println("  ✓ /api/v1/horizon-compat/transactions/{hash}/payments")
	log.Println("  ✓ /api/v1/horizon-compat/transactions/{hash}/effects")
	log.Println("  ✓ /api/v1/horizon-compat/accounts/{id}/operations")
	log.Println("  ✓ /api/v1/horizon-compat/accounts/{id}/payments")
	log.Println("  ✓ /api/v1/horizon-compat/accounts/{id}/effects")
	log.Println("  ✓ /api/v1/horizon-compat/operations")
	log.Println("  ✓ /api/v1/horizon-compat/payments")
	log.Println("  ✓ /api/v1/horizon-compat/effects")
}
