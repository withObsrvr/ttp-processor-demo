package health

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/withObsrvr/ttp-processor-demo/obsrvr-lake/radar-network-ingester/go/checkpoint"
)

type Server struct {
	mu         sync.RWMutex
	port       int
	startTime  time.Time
	checkpoint *checkpoint.Checkpoint
	server     *http.Server

	ErrorCount    uint64
	LastError     string
	LastErrorTime time.Time
}

type HealthResponse struct {
	Status     string `json:"status"`
	Uptime     string `json:"uptime"`
	LastScanID uint32 `json:"last_scan_id"`
	TotalScans uint64 `json:"total_scans"`
	TotalNodes uint64 `json:"total_nodes"`
	TotalOrgs  uint64 `json:"total_orgs"`
	ErrorCount uint64 `json:"error_count"`
	LastError  string `json:"last_error,omitempty"`
}

func NewServer(port int, cp *checkpoint.Checkpoint) *Server {
	return &Server{
		port:       port,
		startTime:  time.Now(),
		checkpoint: cp,
	}
}

func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/metrics", s.handleMetrics)

	s.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: mux,
	}

	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("Health server error: %v\n", err)
		}
	}()
	return nil
}

func (s *Server) Stop() error {
	if s.server != nil {
		return s.server.Close()
	}
	return nil
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	lastScanID, totalScans, totalNodes, totalOrgs := s.checkpoint.GetStats()

	resp := HealthResponse{
		Status:     "healthy",
		Uptime:     time.Since(s.startTime).String(),
		LastScanID: lastScanID,
		TotalScans: totalScans,
		TotalNodes: totalNodes,
		TotalOrgs:  totalOrgs,
		ErrorCount: s.ErrorCount,
	}
	if s.LastError != "" {
		resp.LastError = s.LastError
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	lastScanID, totalScans, totalNodes, totalOrgs := s.checkpoint.GetStats()

	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "# HELP stellarbeat_network_ingester_last_scan_id Last ingested scan ID\n")
	fmt.Fprintf(w, "# TYPE stellarbeat_network_ingester_last_scan_id gauge\n")
	fmt.Fprintf(w, "stellarbeat_network_ingester_last_scan_id %d\n", lastScanID)
	fmt.Fprintf(w, "# HELP stellarbeat_network_ingester_total_scans Total scans ingested\n")
	fmt.Fprintf(w, "# TYPE stellarbeat_network_ingester_total_scans counter\n")
	fmt.Fprintf(w, "stellarbeat_network_ingester_total_scans %d\n", totalScans)
	fmt.Fprintf(w, "# HELP stellarbeat_network_ingester_total_nodes Total node measurements ingested\n")
	fmt.Fprintf(w, "# TYPE stellarbeat_network_ingester_total_nodes counter\n")
	fmt.Fprintf(w, "stellarbeat_network_ingester_total_nodes %d\n", totalNodes)
	fmt.Fprintf(w, "# HELP stellarbeat_network_ingester_total_orgs Total org measurements ingested\n")
	fmt.Fprintf(w, "# TYPE stellarbeat_network_ingester_total_orgs counter\n")
	fmt.Fprintf(w, "stellarbeat_network_ingester_total_orgs %d\n", totalOrgs)
	fmt.Fprintf(w, "# HELP stellarbeat_network_ingester_errors Total errors\n")
	fmt.Fprintf(w, "# TYPE stellarbeat_network_ingester_errors counter\n")
	fmt.Fprintf(w, "stellarbeat_network_ingester_errors %d\n", s.ErrorCount)
}

func (s *Server) RecordError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ErrorCount++
	s.LastError = err.Error()
	s.LastErrorTime = time.Now()
}
