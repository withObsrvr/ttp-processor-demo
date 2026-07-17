package main

import (
	"context"
	"database/sql"
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

func TestOptionalDBWarmupUsesDedicatedConnectionAndAllowsEmptyTable(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open DuckDB: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if _, err := db.Exec("CREATE TABLE optional_index (id INTEGER)"); err != nil {
		t.Fatalf("create empty index table: %v", err)
	}

	status := newOptionalWarmupStatus(true)
	probeStarted := make(chan struct{})
	releaseProbe := make(chan struct{})
	startOptionalDBWarmup("test index", true, time.Second, db, status, func(ctx context.Context, conn *sql.Conn) error {
		close(probeStarted)
		select {
		case <-releaseProbe:
		case <-ctx.Done():
			return ctx.Err()
		}
		var exists bool
		return conn.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM optional_index LIMIT 1)").Scan(&exists)
	})
	<-probeStarted

	queryDone := make(chan error, 1)
	go func() {
		var marker int
		queryDone <- db.QueryRow("SELECT 1").Scan(&marker)
	}()
	select {
	case err := <-queryDone:
		if err != nil {
			t.Fatalf("request query failed during warm-up: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("request query waited behind optional warm-up")
	}

	close(releaseProbe)
	waitForWarmupState(t, status, "ready")
	if got := db.Stats().MaxOpenConnections; got != 1 {
		t.Fatalf("max open connections = %d after warm-up, want 1", got)
	}
}

func TestOptionalWarmupRetryClearsCompletedAt(t *testing.T) {
	status := newOptionalWarmupStatus(true)
	status.set("ready", "", true)
	if _, ok := status.Snapshot()["completed_at"]; !ok {
		t.Fatal("completed_at missing after completed warm-up")
	}

	status.set("warming", "", false)
	if _, ok := status.Snapshot()["completed_at"]; ok {
		t.Fatal("completed_at remained set while retry was warming")
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
