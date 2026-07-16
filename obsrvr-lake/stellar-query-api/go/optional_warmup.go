package main

import (
	"context"
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

func optionalWarmupHealth(enabled bool, status *OptionalWarmupStatus) map[string]interface{} {
	if !enabled {
		return map[string]interface{}{"state": "disabled"}
	}
	if status == nil {
		return map[string]interface{}{"state": "unavailable"}
	}
	return status.Snapshot()
}
