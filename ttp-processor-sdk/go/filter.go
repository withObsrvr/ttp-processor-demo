package main

import (
	"strconv"
	"strings"

	"github.com/stellar/go-stellar-sdk/processors/token_transfer"
	"go.uber.org/zap"
)

// FilterConfig defines filtering rules for token transfer events
type FilterConfig struct {
	// Event types to include (if empty, all types included)
	IncludeEventTypes []string

	// Event types to exclude (takes precedence over include)
	ExcludeEventTypes []string

	// Minimum amount threshold (in stroops)
	MinAmount *int64

	// Contract addresses to filter by (for Soroban events)
	ContractAddresses []string

	// Whether filtering is enabled
	Enabled bool
}

// LoadFilterConfig loads filtering configuration from environment variables
func LoadFilterConfig(logger *zap.Logger) *FilterConfig {
	config := &FilterConfig{
		IncludeEventTypes: parseCSV(getEnv("FILTER_EVENT_TYPES", "")),
		ExcludeEventTypes: parseCSV(getEnv("EXCLUDE_EVENT_TYPES", "")),
		ContractAddresses: parseCSV(getEnv("FILTER_CONTRACT_IDS", "")),
	}

	// Parse minimum amount
	if minAmtStr := getEnv("FILTER_MIN_AMOUNT", ""); minAmtStr != "" {
		if amt, err := strconv.ParseInt(minAmtStr, 10, 64); err == nil {
			config.MinAmount = &amt
		} else {
			logger.Warn("Invalid FILTER_MIN_AMOUNT, ignoring",
				zap.String("value", minAmtStr),
				zap.Error(err))
		}
	}

	// Determine if filtering is enabled
	config.Enabled = len(config.IncludeEventTypes) > 0 ||
		len(config.ExcludeEventTypes) > 0 ||
		config.MinAmount != nil ||
		len(config.ContractAddresses) > 0

	if config.Enabled {
		logger.Info("Event filtering enabled",
			zap.Strings("include_types", config.IncludeEventTypes),
			zap.Strings("exclude_types", config.ExcludeEventTypes),
			zap.Int64p("min_amount", config.MinAmount),
			zap.Strings("contract_ids", config.ContractAddresses))
	}

	return config
}

// ShouldIncludeEvent determines if an event should be included based on filters
func (f *FilterConfig) ShouldIncludeEvent(event *token_transfer.TokenTransferEvent, logger *zap.Logger) bool {
	if !f.Enabled {
		return true
	}

	eventType := getEventType(event)

	// Check exclude list first (takes precedence)
	if len(f.ExcludeEventTypes) > 0 {
		for _, excluded := range f.ExcludeEventTypes {
			if strings.EqualFold(excluded, eventType) {
				logger.Debug("Event excluded by type",
					zap.String("event_type", eventType),
					zap.String("ledger", strconv.FormatUint(uint64(event.Meta.LedgerSequence), 10)))
				return false
			}
		}
	}

	// Check include list (if specified)
	if len(f.IncludeEventTypes) > 0 {
		found := false
		for _, included := range f.IncludeEventTypes {
			if strings.EqualFold(included, eventType) {
				found = true
				break
			}
		}
		if !found {
			logger.Debug("Event not in include list",
				zap.String("event_type", eventType))
			return false
		}
	}

	// Check minimum amount
	if f.MinAmount != nil {
		amount := getEventAmount(event)
		if amount < *f.MinAmount {
			logger.Debug("Event below minimum amount",
				zap.String("event_type", eventType),
				zap.Int64("amount", amount),
				zap.Int64("min_amount", *f.MinAmount))
			return false
		}
	}

	// Check contract addresses (for Soroban events)
	if len(f.ContractAddresses) > 0 && event.Meta.ContractAddress != "" {
		found := false
		for _, addr := range f.ContractAddresses {
			if event.Meta.ContractAddress == addr {
				found = true
				break
			}
		}
		if !found {
			logger.Debug("Contract address not in filter list",
				zap.String("contract_address", event.Meta.ContractAddress))
			return false
		}
	}

	return true
}

// getEventType returns the string type of the event
func getEventType(event *token_transfer.TokenTransferEvent) string {
	switch event.Event.(type) {
	case *token_transfer.TokenTransferEvent_Transfer:
		return "transfer"
	case *token_transfer.TokenTransferEvent_Mint:
		return "mint"
	case *token_transfer.TokenTransferEvent_Burn:
		return "burn"
	case *token_transfer.TokenTransferEvent_Clawback:
		return "clawback"
	case *token_transfer.TokenTransferEvent_Fee:
		return "fee"
	default:
		return "unknown"
	}
}

// getEventAmount returns the amount from the event
func getEventAmount(event *token_transfer.TokenTransferEvent) int64 {
	var amountStr string
	switch evt := event.Event.(type) {
	case *token_transfer.TokenTransferEvent_Transfer:
		amountStr = evt.Transfer.Amount
	case *token_transfer.TokenTransferEvent_Mint:
		amountStr = evt.Mint.Amount
	case *token_transfer.TokenTransferEvent_Burn:
		amountStr = evt.Burn.Amount
	case *token_transfer.TokenTransferEvent_Clawback:
		amountStr = evt.Clawback.Amount
	case *token_transfer.TokenTransferEvent_Fee:
		amountStr = evt.Fee.Amount
	default:
		return 0
	}

	// Parse amount string to int64
	amount, err := strconv.ParseInt(amountStr, 10, 64)
	if err != nil {
		return 0
	}
	return amount
}

// parseCSV parses a comma-separated string into a slice
func parseCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
