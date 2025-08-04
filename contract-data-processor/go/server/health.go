package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/withObsrvr/ttp-processor-demo/contract-data-processor/logging"
)

// HealthServer provides health check endpoints
type HealthServer struct {
	logger         *logging.ComponentLogger
	port           int
	server         *http.Server
	
	// Component health
	mu             sync.RWMutex
	components     map[string]*ComponentHealth
}

// ComponentHealth tracks health of a component
type ComponentHealth struct {
	Name        string        `json:"name"`
	Healthy     bool          `json:"healthy"`
	LastCheck   time.Time     `json:"last_check"`
	LastError   string        `json:"last_error,omitempty"`
	Metrics     interface{}   `json:"metrics,omitempty"`
}

// HealthStatus represents overall service health
type HealthStatus struct {
	Status      string                     `json:"status"` // healthy, degraded, unhealthy
	Version     string                     `json:"version"`
	Uptime      string                     `json:"uptime"`
	Components  map[string]*ComponentHealth `json:"components"`
	Timestamp   time.Time                  `json:"timestamp"`
}

// NewHealthServer creates a new health server
func NewHealthServer(logger *logging.ComponentLogger, port int) *HealthServer {
	return &HealthServer{
		logger:     logger,
		port:       port,
		components: make(map[string]*ComponentHealth),
	}
}

// RegisterComponent registers a component for health monitoring
func (h *HealthServer) RegisterComponent(name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	h.components[name] = &ComponentHealth{
		Name:      name,
		Healthy:   false,
		LastCheck: time.Now(),
	}
}

// UpdateComponentHealth updates a component's health status
func (h *HealthServer) UpdateComponentHealth(name string, healthy bool, err error, metrics interface{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	component, exists := h.components[name]
	if !exists {
		component = &ComponentHealth{Name: name}
		h.components[name] = component
	}
	
	component.Healthy = healthy
	component.LastCheck = time.Now()
	component.Metrics = metrics
	
	if err != nil {
		component.LastError = err.Error()
	} else {
		component.LastError = ""
	}
}

// Start starts the health server
func (h *HealthServer) Start(ctx context.Context, startTime time.Time) error {
	mux := http.NewServeMux()
	
	// Health endpoint
	mux.HandleFunc("/health", h.handleHealth(startTime))
	
	// Ready endpoint
	mux.HandleFunc("/ready", h.handleReady())
	
	// Metrics endpoint (Prometheus format)
	mux.HandleFunc("/metrics", h.handleMetrics())
	
	h.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", h.port),
		Handler: mux,
	}
	
	h.logger.Info().
		Int("port", h.port).
		Msg("Starting health server")
	
	go func() {
		if err := h.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			h.logger.Error().
				Err(err).
				Msg("Health server error")
		}
	}()
	
	// Wait for server to start
	time.Sleep(100 * time.Millisecond)
	
	return nil
}

// Stop stops the health server
func (h *HealthServer) Stop() error {
	if h.server == nil {
		return nil
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	return h.server.Shutdown(ctx)
}

// Handler implementations

func (h *HealthServer) handleHealth(startTime time.Time) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h.mu.RLock()
		defer h.mu.RUnlock()
		
		uptime := time.Since(startTime)
		
		// Determine overall status
		status := "healthy"
		unhealthyCount := 0
		for _, comp := range h.components {
			if !comp.Healthy {
				unhealthyCount++
			}
		}
		
		if unhealthyCount > 0 {
			if unhealthyCount == len(h.components) {
				status = "unhealthy"
			} else {
				status = "degraded"
			}
		}
		
		health := HealthStatus{
			Status:     status,
			Version:    "v1.0.0",
			Uptime:     uptime.String(),
			Components: h.components,
			Timestamp:  time.Now(),
		}
		
		// Set appropriate status code
		statusCode := http.StatusOK
		if status == "unhealthy" {
			statusCode = http.StatusServiceUnavailable
		} else if status == "degraded" {
			statusCode = http.StatusOK // Still return 200 for degraded
		}
		
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		json.NewEncoder(w).Encode(health)
	}
}

func (h *HealthServer) handleReady() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h.mu.RLock()
		defer h.mu.RUnlock()
		
		// Check if all critical components are healthy
		// For now, we consider data source as critical
		dataSourceHealthy := true
		if comp, exists := h.components["data_source"]; exists {
			dataSourceHealthy = comp.Healthy
		}
		
		if dataSourceHealthy {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("ready\n"))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("not ready\n"))
		}
	}
}

func (h *HealthServer) handleMetrics() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h.mu.RLock()
		defer h.mu.RUnlock()
		
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		
		// Export basic metrics in Prometheus format
		fmt.Fprintf(w, "# HELP contract_data_processor_up Whether the processor is running\n")
		fmt.Fprintf(w, "# TYPE contract_data_processor_up gauge\n")
		fmt.Fprintf(w, "contract_data_processor_up 1\n\n")
		
		// Component health metrics
		fmt.Fprintf(w, "# HELP contract_data_processor_component_healthy Component health status\n")
		fmt.Fprintf(w, "# TYPE contract_data_processor_component_healthy gauge\n")
		for name, comp := range h.components {
			healthValue := 0
			if comp.Healthy {
				healthValue = 1
			}
			fmt.Fprintf(w, "contract_data_processor_component_healthy{component=\"%s\"} %d\n", name, healthValue)
		}
		fmt.Fprintln(w)
		
		// Add specific metrics from components
		if comp, exists := h.components["data_source"]; exists && comp.Metrics != nil {
			if metrics, ok := comp.Metrics.(DataSourceMetrics); ok {
				fmt.Fprintf(w, "# HELP contract_data_processor_ledgers_received_total Total ledgers received\n")
				fmt.Fprintf(w, "# TYPE contract_data_processor_ledgers_received_total counter\n")
				fmt.Fprintf(w, "contract_data_processor_ledgers_received_total %d\n", metrics.LedgersReceived)
				
				fmt.Fprintf(w, "# HELP contract_data_processor_bytes_received_total Total bytes received\n")
				fmt.Fprintf(w, "# TYPE contract_data_processor_bytes_received_total counter\n")
				fmt.Fprintf(w, "contract_data_processor_bytes_received_total %d\n", metrics.BytesReceived)
				
				fmt.Fprintf(w, "# HELP contract_data_processor_ingest_rate Ledgers per second ingestion rate\n")
				fmt.Fprintf(w, "# TYPE contract_data_processor_ingest_rate gauge\n")
				fmt.Fprintf(w, "contract_data_processor_ingest_rate %f\n", metrics.IngestRate)
			}
		}
		
		if comp, exists := h.components["stream_manager"]; exists && comp.Metrics != nil {
			if metrics, ok := comp.Metrics.(StreamMetrics); ok {
				fmt.Fprintf(w, "# HELP contract_data_processor_ledgers_processed_total Total ledgers processed\n")
				fmt.Fprintf(w, "# TYPE contract_data_processor_ledgers_processed_total counter\n")
				fmt.Fprintf(w, "contract_data_processor_ledgers_processed_total %d\n", metrics.ProcessedCount)
				
				fmt.Fprintf(w, "# HELP contract_data_processor_contracts_found_total Total contracts found\n")
				fmt.Fprintf(w, "# TYPE contract_data_processor_contracts_found_total counter\n")
				fmt.Fprintf(w, "contract_data_processor_contracts_found_total %d\n", metrics.ContractsFound)
				
				fmt.Fprintf(w, "# HELP contract_data_processor_processing_errors_total Total processing errors\n")
				fmt.Fprintf(w, "# TYPE contract_data_processor_processing_errors_total counter\n")
				fmt.Fprintf(w, "contract_data_processor_processing_errors_total %d\n", metrics.ErrorCount)
				
				fmt.Fprintf(w, "# HELP contract_data_processor_queue_depth Current processing queue depth\n")
				fmt.Fprintf(w, "# TYPE contract_data_processor_queue_depth gauge\n")
				fmt.Fprintf(w, "contract_data_processor_queue_depth %d\n", metrics.QueueDepth)
			}
		}
	}
}

// MonitorDataSource monitors the data source health
func (h *HealthServer) MonitorDataSource(ctx context.Context, dataSource *DataSourceClient) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			metrics := dataSource.GetMetrics()
			h.UpdateComponentHealth("data_source", dataSource.IsHealthy(), metrics.LastError, metrics)
		}
	}
}

// MonitorStreamManager monitors the stream manager health
func (h *HealthServer) MonitorStreamManager(ctx context.Context, streamManager *StreamManager) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			metrics := streamManager.GetMetrics()
			healthy := metrics.Running && metrics.ProcessingLag < 30*time.Second
			h.UpdateComponentHealth("stream_manager", healthy, nil, metrics)
		}
	}
}