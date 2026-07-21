package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestRunStreamWithReconnectResumesAfterCheckpoint(t *testing.T) {
	cp, err := NewCheckpoint(filepath.Join(t.TempDir(), "checkpoint.json"))
	if err != nil {
		t.Fatal(err)
	}
	health := NewHealthServer(0, cp)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var starts []uint32
	stream := func(_ context.Context, start, _ uint32) error {
		starts = append(starts, start)
		if len(starts) == 1 {
			if err := cp.Update(100, "hash", 0, 0, 0); err != nil {
				t.Fatal(err)
			}
			return errors.New("rpc error: code = Unavailable desc = reconnect")
		}
		cancel()
		return context.Canceled
	}
	sleep := func(context.Context, time.Duration) error { return nil }

	err = runStreamWithReconnect(ctx, 100, 0, cp, health, time.Millisecond, time.Second, stream, sleep)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("runStreamWithReconnect error = %v, want context.Canceled", err)
	}
	if want := []uint32{100, 101}; !reflect.DeepEqual(starts, want) {
		t.Fatalf("stream starts = %v, want %v", starts, want)
	}
}

func TestHealthReportsReconnectAndStaleProgress(t *testing.T) {
	cp, err := NewCheckpoint(filepath.Join(t.TempDir(), "checkpoint.json"))
	if err != nil {
		t.Fatal(err)
	}
	health := NewHealthServerWithStaleAfter(0, cp, time.Minute)
	now := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	health.now = func() time.Time { return now }
	health.startTime = now
	health.metrics.LastLedgerTime = now

	health.MarkStreamError(errors.New("source unavailable"))
	recorder := httptest.NewRecorder()
	health.handleHealth(recorder, httptest.NewRequest(http.MethodGet, "/health", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("reconnecting health status = %d, want 503", recorder.Code)
	}

	health.MarkStreamConnected()
	health.UpdateMetrics(1, 0, 0)
	recorder = httptest.NewRecorder()
	health.handleHealth(recorder, httptest.NewRequest(http.MethodGet, "/health", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("streaming health status = %d, want 200", recorder.Code)
	}

	now = now.Add(2 * time.Minute)
	recorder = httptest.NewRecorder()
	health.handleHealth(recorder, httptest.NewRequest(http.MethodGet, "/health", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("stale health status = %d, want 503", recorder.Code)
	}
	var response HealthResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Status != "degraded" || response.StreamState != "stale" {
		t.Fatalf("stale health response = %+v", response)
	}
}
