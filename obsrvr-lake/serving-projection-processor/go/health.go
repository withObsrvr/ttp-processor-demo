package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"
)

type ProjectorRunner interface {
	Name() string
	RunOnce(context.Context) (RunStats, error)
}

type HealthServer struct {
	port         int
	serviceName  string
	tickInterval time.Duration
	startTime    time.Time

	mu         sync.RWMutex
	projectors map[string]*ProjectorRuntimeStatus
	server     *http.Server
}

type HealthResponse struct {
	Status       string                   `json:"status"`
	Service      string                   `json:"service"`
	Uptime       string                   `json:"uptime"`
	TickInterval string                   `json:"tick_interval"`
	Projectors   []ProjectorRuntimeStatus `json:"projectors"`
}

func NewHealthServer(serviceName string, port int, tickInterval time.Duration) *HealthServer {
	return &HealthServer{
		port:         port,
		serviceName:  serviceName,
		tickInterval: tickInterval,
		startTime:    time.Now(),
		projectors:   map[string]*ProjectorRuntimeStatus{},
	}
}

func (hs *HealthServer) RegisterProjector(name string) {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	if _, ok := hs.projectors[name]; ok {
		return
	}
	hs.projectors[name] = &ProjectorRuntimeStatus{Name: name}
}

func (hs *HealthServer) Start() error {
	if hs.port <= 0 {
		return nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", hs.handleHealth)
	mux.HandleFunc("/status", hs.handleStatus)
	mux.HandleFunc("/metrics", hs.handleMetrics)

	hs.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", hs.port),
		Handler: mux,
	}

	go func() {
		if err := hs.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("health server error: %v\n", err)
		}
	}()

	return nil
}

func (hs *HealthServer) Stop(ctx context.Context) error {
	if hs.server == nil {
		return nil
	}
	return hs.server.Shutdown(ctx)
}

func (hs *HealthServer) RunProjector(ctx context.Context, p ProjectorRunner) error {
	name := p.Name()
	hs.RegisterProjector(name)

	startedAt := time.Now().UTC()
	hs.mu.Lock()
	status := hs.projectors[name]
	status.LastStartedAt = &startedAt
	status.TotalRuns++
	hs.mu.Unlock()

	runStats, err := p.RunOnce(ctx)
	finishedAt := time.Now().UTC()
	durationMs := finishedAt.Sub(startedAt).Milliseconds()

	hs.mu.Lock()
	defer hs.mu.Unlock()
	status = hs.projectors[name]
	status.LastCompletedAt = &finishedAt
	status.LastDurationMs = durationMs
	status.LastRowsApplied = runStats.RowsApplied
	status.LastRowsDeleted = runStats.RowsDeleted
	if runStats.Checkpoint > 0 {
		status.LastCheckpoint = runStats.Checkpoint
	}

	if err != nil {
		status.TotalFailures++
		status.ConsecutiveErrors++
		status.LastError = err.Error()
		status.LastErrorAt = &finishedAt
		return err
	}

	status.TotalSuccesses++
	status.TotalRowsApplied += runStats.RowsApplied
	status.TotalRowsDeleted += runStats.RowsDeleted
	status.LastSuccessAt = &finishedAt
	status.ConsecutiveErrors = 0
	status.LastError = ""
	status.LastErrorAt = nil
	return nil
}

func (hs *HealthServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	resp := hs.snapshot()
	code := http.StatusOK
	if resp.Status == "degraded" {
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, resp)
}

func (hs *HealthServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, hs.snapshot())
}

func (hs *HealthServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	hs.mu.RLock()
	projectors := make([]ProjectorRuntimeStatus, 0, len(hs.projectors))
	for _, p := range hs.projectors {
		projectors = append(projectors, *p)
	}
	hs.mu.RUnlock()

	sort.Slice(projectors, func(i, j int) bool { return projectors[i].Name < projectors[j].Name })

	fmt.Fprintf(w, "# HELP serving_projection_processor_uptime_seconds Service uptime in seconds\n")
	fmt.Fprintf(w, "# TYPE serving_projection_processor_uptime_seconds gauge\n")
	fmt.Fprintf(w, "serving_projection_processor_uptime_seconds %.0f\n", time.Since(hs.startTime).Seconds())

	for _, p := range projectors {
		labels := fmt.Sprintf("projector=%q", p.Name)
		fmt.Fprintf(w, "# HELP serving_projection_projector_last_duration_ms Last projector run duration in milliseconds\n")
		fmt.Fprintf(w, "# TYPE serving_projection_projector_last_duration_ms gauge\n")
		fmt.Fprintf(w, "serving_projection_projector_last_duration_ms{%s} %d\n", labels, p.LastDurationMs)

		fmt.Fprintf(w, "# HELP serving_projection_projector_last_rows_applied Rows applied in the last successful run\n")
		fmt.Fprintf(w, "# TYPE serving_projection_projector_last_rows_applied gauge\n")
		fmt.Fprintf(w, "serving_projection_projector_last_rows_applied{%s} %d\n", labels, p.LastRowsApplied)

		fmt.Fprintf(w, "# HELP serving_projection_projector_last_rows_deleted Rows deleted in the last successful run\n")
		fmt.Fprintf(w, "# TYPE serving_projection_projector_last_rows_deleted gauge\n")
		fmt.Fprintf(w, "serving_projection_projector_last_rows_deleted{%s} %d\n", labels, p.LastRowsDeleted)

		fmt.Fprintf(w, "# HELP serving_projection_projector_total_runs Total projector runs\n")
		fmt.Fprintf(w, "# TYPE serving_projection_projector_total_runs counter\n")
		fmt.Fprintf(w, "serving_projection_projector_total_runs{%s} %d\n", labels, p.TotalRuns)

		fmt.Fprintf(w, "# HELP serving_projection_projector_total_failures Total projector failures\n")
		fmt.Fprintf(w, "# TYPE serving_projection_projector_total_failures counter\n")
		fmt.Fprintf(w, "serving_projection_projector_total_failures{%s} %d\n", labels, p.TotalFailures)

		fmt.Fprintf(w, "# HELP serving_projection_projector_last_checkpoint Last saved projector checkpoint\n")
		fmt.Fprintf(w, "# TYPE serving_projection_projector_last_checkpoint gauge\n")
		fmt.Fprintf(w, "serving_projection_projector_last_checkpoint{%s} %d\n", labels, p.LastCheckpoint)

		fmt.Fprintf(w, "# HELP serving_projection_projector_consecutive_errors Consecutive projector errors\n")
		fmt.Fprintf(w, "# TYPE serving_projection_projector_consecutive_errors gauge\n")
		fmt.Fprintf(w, "serving_projection_projector_consecutive_errors{%s} %d\n", labels, p.ConsecutiveErrors)

		if p.LastSuccessAt != nil {
			fmt.Fprintf(w, "# HELP serving_projection_projector_last_success_timestamp_seconds Unix timestamp of last projector success\n")
			fmt.Fprintf(w, "# TYPE serving_projection_projector_last_success_timestamp_seconds gauge\n")
			fmt.Fprintf(w, "serving_projection_projector_last_success_timestamp_seconds{%s} %d\n", labels, p.LastSuccessAt.Unix())
		}

		if p.LastErrorAt != nil {
			fmt.Fprintf(w, "# HELP serving_projection_projector_last_error_timestamp_seconds Unix timestamp of last projector error\n")
			fmt.Fprintf(w, "# TYPE serving_projection_projector_last_error_timestamp_seconds gauge\n")
			fmt.Fprintf(w, "serving_projection_projector_last_error_timestamp_seconds{%s} %d\n", labels, p.LastErrorAt.Unix())
		}
	}
}

func (hs *HealthServer) snapshot() HealthResponse {
	hs.mu.RLock()
	defer hs.mu.RUnlock()

	projectors := make([]ProjectorRuntimeStatus, 0, len(hs.projectors))
	status := "healthy"
	starting := false
	for _, p := range hs.projectors {
		projectors = append(projectors, *p)
		if p.TotalRuns == 0 {
			starting = true
		}
		if p.ConsecutiveErrors > 0 {
			status = "degraded"
		}
	}
	if status == "healthy" && starting {
		status = "starting"
	}

	sort.Slice(projectors, func(i, j int) bool { return projectors[i].Name < projectors[j].Name })

	return HealthResponse{
		Status:       status,
		Service:      hs.serviceName,
		Uptime:       time.Since(hs.startTime).Round(time.Second).String(),
		TickInterval: hs.tickInterval.String(),
		Projectors:   projectors,
	}
}

func (hs *HealthServer) GetProjectorCheckpoint(name string) int64 {
	hs.mu.RLock()
	defer hs.mu.RUnlock()
	if p, ok := hs.projectors[name]; ok {
		return p.LastCheckpoint
	}
	return 0
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
