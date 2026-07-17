package main

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	defaultInteractiveQueryTimeout = 4 * time.Second
	defaultLedgerFullQueryTimeout  = 3500 * time.Millisecond
	defaultHomeSummaryQueryTimeout = 2500 * time.Millisecond
	defaultSmartWalletQueryTimeout = 1500 * time.Millisecond
	defaultOptionalQueryTimeout    = 500 * time.Millisecond
	defaultRecentLedgerWindow      = int64(2000)
)

func interactiveQueryTimeout() time.Duration {
	return durationEnv("QUERY_API_INTERACTIVE_TIMEOUT", defaultInteractiveQueryTimeout)
}

func optionalQueryTimeout() time.Duration {
	return durationEnv("QUERY_API_OPTIONAL_TIMEOUT", defaultOptionalQueryTimeout)
}

func ledgerFullQueryTimeout() time.Duration {
	return durationEnv("QUERY_API_LEDGER_FULL_TIMEOUT", defaultLedgerFullQueryTimeout)
}

func homeSummaryQueryTimeout() time.Duration {
	return durationEnv("QUERY_API_HOME_SUMMARY_TIMEOUT", defaultHomeSummaryQueryTimeout)
}

func smartWalletQueryTimeout() time.Duration {
	return durationEnv("QUERY_API_SMART_WALLET_TIMEOUT", defaultSmartWalletQueryTimeout)
}

func recentLedgerWindow() int64 {
	value := strings.TrimSpace(envOrDefault("QUERY_API_RECENT_LEDGER_WINDOW", ""))
	if value == "" {
		return defaultRecentLedgerWindow
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 {
		return defaultRecentLedgerWindow
	}
	return parsed
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(envOrDefault(key, ""))
	if value == "" {
		return fallback
	}
	if d, err := time.ParseDuration(value); err == nil && d > 0 {
		return d
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return fallback
}

func withInteractiveQueryTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, interactiveQueryTimeout())
}

func withOptionalQueryTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, optionalQueryTimeout())
}

func withHomeSummaryQueryTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, homeSummaryQueryTimeout())
}

func withSmartWalletQueryTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, smartWalletQueryTimeout())
}

func isQueryTimeout(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "context deadline exceeded") ||
		strings.Contains(msg, "interrupted") ||
		strings.Contains(msg, "canceling statement")
}

func respondQueryTimeout(w http.ResponseWriter, source string) {
	if source == "" {
		source = "query"
	}
	respondError(w, source+" timed out; retry with a narrower filter or ledger range", http.StatusServiceUnavailable)
}

func defaultLedgerWindow(latest int64) (int64, int64) {
	if latest <= 0 {
		return 0, 0
	}
	start := latest - recentLedgerWindow()
	if start < 1 {
		start = 1
	}
	return start, latest
}
