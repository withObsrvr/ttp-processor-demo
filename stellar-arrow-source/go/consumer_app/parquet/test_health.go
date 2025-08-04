package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

type HealthResponse struct {
	Status     string `json:"status"`
	Guarantees struct {
		MaxLatencyP99    string  `json:"max_latency_p99"`
		MaxRetryLatency  string  `json:"max_retry_latency"`
		MinUptime        float64 `json:"min_uptime"`
	} `json:"guarantees"`
	Metrics struct {
		AverageLatency      string                 `json:"average_latency"`
		CircuitBreakerState string                 `json:"circuit_breaker_state"`
		ErrorCount          int                    `json:"error_count"`
		ErrorTypes          map[string]int         `json:"error_types"`
		LastError           string                 `json:"last_error"`
		LastErrorTime       string                 `json:"last_error_time"`
		LastSequence        uint32                 `json:"last_sequence"`
		RetryCount          int                    `json:"retry_count"`
		SuccessCount        int                    `json:"success_count"`
		TotalBytes          int64                  `json:"total_bytes"`
		TotalLatency        string                 `json:"total_latency"`
		TotalProcessed      int                    `json:"total_processed"`
		UptimePercent       float64                `json:"uptime_percent"`
	} `json:"metrics"`
}

func main() {
	// Check health endpoint
	resp, err := http.Get("http://localhost:8088/health")
	if err != nil {
		fmt.Printf("Error fetching health status: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("Error reading response: %v\n", err)
		os.Exit(1)
	}

	var health HealthResponse
	if err := json.Unmarshal(body, &health); err != nil {
		fmt.Printf("Error parsing JSON: %v\n", err)
		fmt.Printf("Raw response: %s\n", string(body))
		os.Exit(1)
	}

	fmt.Println("=== Stellar Live Source Datalake Health Status ===")
	fmt.Printf("Status: %s\n", health.Status)
	fmt.Printf("Circuit Breaker State: %s\n", health.Metrics.CircuitBreakerState)
	fmt.Printf("\nProcessing Statistics:\n")
	fmt.Printf("  Total Processed: %d ledgers\n", health.Metrics.TotalProcessed)
	fmt.Printf("  Success Count: %d\n", health.Metrics.SuccessCount)
	fmt.Printf("  Error Count: %d\n", health.Metrics.ErrorCount)
	fmt.Printf("  Last Successful Sequence: %d\n", health.Metrics.LastSequence)
	fmt.Printf("  Total Bytes Processed: %.2f MB\n", float64(health.Metrics.TotalBytes)/(1024*1024))
	fmt.Printf("  Average Latency: %s\n", health.Metrics.AverageLatency)
	fmt.Printf("  Uptime: %.6f%%\n", health.Metrics.UptimePercent)
	
	if health.Metrics.LastError != "" {
		fmt.Printf("\nLast Error:\n")
		fmt.Printf("  Time: %s\n", health.Metrics.LastErrorTime)
		fmt.Printf("  Error: %s\n", health.Metrics.LastError)
	}

	if len(health.Metrics.ErrorTypes) > 0 {
		fmt.Printf("\nError Breakdown:\n")
		for errorType, count := range health.Metrics.ErrorTypes {
			fmt.Printf("  %s: %d\n", errorType, count)
		}
	}

	// Based on the metrics, suggest ledger ranges that might work
	if health.Metrics.LastSequence > 0 {
		fmt.Printf("\n=== Suggested Test Ranges ===\n")
		fmt.Printf("Based on the last successful sequence (%d), try:\n", health.Metrics.LastSequence)
		
		// Suggest ranges around the last successful sequence
		suggestedStart := health.Metrics.LastSequence - 1000
		fmt.Printf("  1. Near last success: %d to %d\n", suggestedStart, health.Metrics.LastSequence)
		fmt.Printf("  2. Before last success: %d to %d\n", suggestedStart-5000, suggestedStart)
		
		// Calculate average ledgers per batch
		if health.Metrics.SuccessCount > 0 {
			avgPerBatch := health.Metrics.TotalProcessed / health.Metrics.SuccessCount
			fmt.Printf("\nAverage ledgers per successful request: %d\n", avgPerBatch)
		}
	}
}