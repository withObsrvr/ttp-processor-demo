package main

import (
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

// PriceHandlers contains HTTP handlers for price data queries
type PriceHandlers struct {
	reader *UnifiedDuckDBReader
}

// NewPriceHandlers creates new price API handlers
func NewPriceHandlers(reader *UnifiedDuckDBReader) *PriceHandlers {
	return &PriceHandlers{reader: reader}
}

// HandleOHLCCandles returns OHLC price candles for a trading pair
// @Summary Get OHLC price candles
// @Description Returns OHLC (Open/High/Low/Close) candles for a trading pair, aggregated by time interval
// @Tags Prices
// @Produce json
// @Param base path string true "Base asset code (e.g., XLM, USDC)"
// @Param counter path string true "Counter asset code"
// @Param interval query string false "Time interval: 1m, 5m, 15m, 1h, 4h, 1d, 1w" default(1h)
// @Param base_issuer query string false "Base asset issuer (omit for XLM)"
// @Param counter_issuer query string false "Counter asset issuer (omit for XLM)"
// @Param start query string false "Start time (RFC3339)" default(24 hours ago)
// @Param end query string false "End time (RFC3339)" default(now)
// @Success 200 {object} map[string]interface{} "OHLC candles"
// @Failure 400 {object} map[string]interface{} "Invalid parameters"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/prices/{base}/{counter}/ohlc [get]
func (h *PriceHandlers) HandleOHLCCandles(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	base := vars["base"]
	counter := vars["counter"]
	if base == "" || counter == "" {
		respondError(w, "base and counter asset codes required", http.StatusBadRequest)
		return
	}

	interval := r.URL.Query().Get("interval")
	if interval == "" {
		interval = "1h"
	}

	baseIssuer := r.URL.Query().Get("base_issuer")
	counterIssuer := r.URL.Query().Get("counter_issuer")

	endTime := time.Now().UTC()
	startTime := endTime.Add(-24 * time.Hour)

	if s := r.URL.Query().Get("start"); s != "" {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			startTime = t
		}
	}
	if s := r.URL.Query().Get("end"); s != "" {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			endTime = t
		}
	}

	candles, err := h.reader.GetOHLCCandles(r.Context(), base, baseIssuer, counter, counterIssuer, interval, startTime, endTime)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, map[string]interface{}{
		"base":     base,
		"counter":  counter,
		"interval": interval,
		"candles":  candles,
		"count":    len(candles),
	})
}

// HandleLatestPrice returns the most recent price for a trading pair
// @Summary Get latest price
// @Description Returns the most recent trade price for a trading pair with 24h volume
// @Tags Prices
// @Produce json
// @Param base path string true "Base asset code"
// @Param counter path string true "Counter asset code"
// @Param base_issuer query string false "Base asset issuer"
// @Param counter_issuer query string false "Counter asset issuer"
// @Success 200 {object} LatestPrice "Latest price"
// @Failure 404 {object} map[string]interface{} "No trades found"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/prices/{base}/{counter}/latest [get]
func (h *PriceHandlers) HandleLatestPrice(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	base := vars["base"]
	counter := vars["counter"]
	if base == "" || counter == "" {
		respondError(w, "base and counter asset codes required", http.StatusBadRequest)
		return
	}

	baseIssuer := r.URL.Query().Get("base_issuer")
	counterIssuer := r.URL.Query().Get("counter_issuer")

	price, err := h.reader.GetLatestPrice(r.Context(), base, baseIssuer, counter, counterIssuer)
	if err != nil {
		if strings.Contains(err.Error(), "no trades found") {
			respondError(w, err.Error(), http.StatusNotFound)
			return
		}
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, price)
}

// HandleTradePairs returns available trading pairs
// @Summary List trading pairs
// @Description Returns available trading pairs with trade counts (minimum 5 trades)
// @Tags Prices
// @Produce json
// @Success 200 {object} map[string]interface{} "Trading pairs"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/prices/pairs [get]
func (h *PriceHandlers) HandleTradePairs(w http.ResponseWriter, r *http.Request) {
	pairs, err := h.reader.GetTradePairs(r.Context())
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, map[string]interface{}{
		"pairs": pairs,
		"count": len(pairs),
	})
}
