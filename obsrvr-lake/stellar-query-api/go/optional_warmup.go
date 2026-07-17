package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"sync"
	"time"
)

const optionalIndexWarmupTimeout = 5 * time.Second

type OptionalWarmupStatus struct {
	mu          sync.RWMutex
	State       string    `json:"state"`
	StartedAt   time.Time `json:"started_at,omitempty"`
	CompletedAt time.Time `json:"completed_at,omitempty"`
	Error       string    `json:"error,omitempty"`
}

func newOptionalWarmupStatus(enabled bool) *OptionalWarmupStatus {
	state := "disabled"
	if enabled {
		state = "pending"
	}
	return &OptionalWarmupStatus{State: state}
}

func (s *OptionalWarmupStatus) Snapshot() map[string]interface{} {
	if s == nil {
		return map[string]interface{}{"state": "unavailable"}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := map[string]interface{}{"state": s.State}
	if !s.StartedAt.IsZero() {
		result["started_at"] = s.StartedAt.Format(time.RFC3339)
	}
	if !s.CompletedAt.IsZero() {
		result["completed_at"] = s.CompletedAt.Format(time.RFC3339)
	}
	if s.Error != "" {
		result["error"] = s.Error
	}
	return result
}

func (s *OptionalWarmupStatus) set(state, errMessage string, completed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.State = state
	s.Error = errMessage
	if state == "warming" {
		s.StartedAt = time.Now().UTC()
		s.CompletedAt = time.Time{}
	}
	if completed {
		s.CompletedAt = time.Now().UTC()
	}
}

func startOptionalWarmup(name string, enabled bool, timeout time.Duration, status *OptionalWarmupStatus, probe func(context.Context) error) {
	if status == nil || !enabled {
		return
	}
	if timeout <= 0 {
		timeout = optionalIndexWarmupTimeout
	}

	go func() {
		status.set("warming", "", false)
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		err := probe(ctx)
		if err == nil {
			status.set("ready", "", true)
			log.Printf("%s optional warm-up ready", name)
			return
		}

		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			status.set("degraded", "warm-up timed out", true)
			log.Printf("%s optional warm-up timed out after %s; API remains available", name, timeout)
			return
		}
		status.set("degraded", err.Error(), true)
		log.Printf("%s optional warm-up degraded: %v", name, err)
	}()
}

// startOptionalDBWarmup reserves a second connection while the asynchronous
// probe runs. API DuckDB pools normally have one connection, so running the
// probe on that connection would make startup non-blocking while still
// blocking every request until the probe completed.
func startOptionalDBWarmup(name string, enabled bool, timeout time.Duration, db *sql.DB, status *OptionalWarmupStatus, probe func(context.Context, *sql.Conn) error) {
	startOptionalWarmup(name, enabled, timeout, status, func(ctx context.Context) error {
		if db == nil {
			return errors.New("warm-up database is unavailable")
		}

		previousMax := db.Stats().MaxOpenConnections
		if previousMax > 0 {
			db.SetMaxOpenConns(previousMax + 1)
			defer db.SetMaxOpenConns(previousMax)
		}

		conn, err := db.Conn(ctx)
		if err != nil {
			return err
		}
		defer conn.Close()
		return probe(ctx, conn)
	})
}

func optionalWarmupHealth(enabled bool, status *OptionalWarmupStatus) map[string]interface{} {
	if !enabled {
		return map[string]interface{}{"state": "disabled"}
	}
	if status == nil {
		return map[string]interface{}{"state": "unavailable"}
	}
	return status.Snapshot()
}
