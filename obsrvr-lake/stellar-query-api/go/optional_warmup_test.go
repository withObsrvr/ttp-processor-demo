package main

import (
	"context"
	"testing"
	"time"
)

func TestOptionalWarmupDoesNotBlockStartup(t *testing.T) {
	status := newOptionalWarmupStatus(true)
	release := make(chan struct{})

	started := time.Now()
	startOptionalWarmup("test index", true, time.Second, status, func(context.Context) error {
		<-release
		return nil
	})
	if elapsed := time.Since(started); elapsed > 50*time.Millisecond {
		t.Fatalf("startOptionalWarmup blocked for %s", elapsed)
	}

	close(release)
	waitForWarmupState(t, status, "ready")
}

func TestOptionalWarmupTimeoutIsDegraded(t *testing.T) {
	status := newOptionalWarmupStatus(true)
	startOptionalWarmup("test index", true, 10*time.Millisecond, status, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})

	waitForWarmupState(t, status, "degraded")
	if got := status.Snapshot()["error"]; got != "warm-up timed out" {
		t.Fatalf("error = %v, want warm-up timed out", got)
	}
}

func TestDisabledOptionalWarmupDoesNotRun(t *testing.T) {
	status := newOptionalWarmupStatus(false)
	called := make(chan struct{}, 1)
	startOptionalWarmup("test index", false, time.Second, status, func(context.Context) error {
		called <- struct{}{}
		return nil
	})

	select {
	case <-called:
		t.Fatal("disabled warm-up executed")
	case <-time.After(20 * time.Millisecond):
	}
	if got := status.Snapshot()["state"]; got != "disabled" {
		t.Fatalf("state = %v, want disabled", got)
	}
}

func waitForWarmupState(t *testing.T, status *OptionalWarmupStatus, want string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if got := status.Snapshot()["state"]; got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("warm-up state = %v, want %s", status.Snapshot()["state"], want)
}
