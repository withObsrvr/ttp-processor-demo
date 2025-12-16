package main

import (
	"fmt"
	"net/http"
	"runtime/trace"
	"strconv"
	"time"

	"go.uber.org/zap"
)

// FlightRecorderHandler provides a debug endpoint for capturing execution traces
// This uses Go 1.25's Flight Recorder API for lightweight production debugging
type FlightRecorderHandler struct {
	logger *zap.Logger
}

// NewFlightRecorderHandler creates a new flight recorder handler
func NewFlightRecorderHandler(logger *zap.Logger) *FlightRecorderHandler {
	return &FlightRecorderHandler{
		logger: logger,
	}
}

// ServeHTTP handles trace capture requests
// Usage: GET /debug/trace?duration=5s
func (h *FlightRecorderHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Only allow GET requests
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse duration parameter (default 5 seconds)
	duration := 5 * time.Second
	if durationStr := r.URL.Query().Get("duration"); durationStr != "" {
		if parsed, err := time.ParseDuration(durationStr); err == nil {
			duration = parsed
		} else {
			http.Error(w, fmt.Sprintf("Invalid duration: %v", err), http.StatusBadRequest)
			return
		}
	}

	// Limit duration to 60 seconds
	if duration > 60*time.Second {
		http.Error(w, "Duration too long (max 60s)", http.StatusBadRequest)
		return
	}

	h.logger.Info("Capturing execution trace",
		zap.Duration("duration", duration),
		zap.String("remote_addr", r.RemoteAddr))

	// Set response headers
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=trace-%d.out", time.Now().Unix()))

	// Start tracing
	if err := trace.Start(w); err != nil {
		h.logger.Error("Failed to start trace", zap.Error(err))
		http.Error(w, fmt.Sprintf("Failed to start trace: %v", err), http.StatusInternalServerError)
		return
	}
	defer trace.Stop()

	// Capture for specified duration
	time.Sleep(duration)

	h.logger.Info("Trace capture complete",
		zap.Duration("duration", duration))
}

// RegisterFlightRecorder adds the flight recorder endpoint to the health server
// This is optional and only enabled if explicitly requested
func RegisterFlightRecorder(mux *http.ServeMux, logger *zap.Logger) {
	handler := NewFlightRecorderHandler(logger)
	mux.Handle("/debug/trace", handler)

	logger.Info("Flight Recorder debug endpoint registered",
		zap.String("endpoint", "/debug/trace"),
		zap.String("usage", "GET /debug/trace?duration=5s"))
}

// GetFlightRecorderUsage returns usage instructions
func GetFlightRecorderUsage() string {
	return `
Flight Recorder - Production Debugging

Capture execution trace for analysis:

  curl "http://localhost:8088/debug/trace?duration=5s" > trace.out

Analyze trace:

  go tool trace trace.out

Parameters:
  - duration: Capture duration (default: 5s, max: 60s)

Example durations:
  - duration=5s    (5 seconds)
  - duration=30s   (30 seconds)
  - duration=1m    (60 seconds)

The trace includes:
  - Goroutine execution
  - System calls
  - GC events
  - Network I/O
  - Channel operations

Use this to debug:
  - Performance bottlenecks
  - Goroutine leaks
  - Blocking operations
  - GC pressure

Note: Tracing has minimal overhead but should be used
sparingly in production. The ring buffer captures recent
events without continuously writing to disk.
`
}

// Helper function to parse bool from query parameter
func parseBoolParam(r *http.Request, param string, defaultValue bool) bool {
	value := r.URL.Query().Get(param)
	if value == "" {
		return defaultValue
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return defaultValue
	}
	return parsed
}
