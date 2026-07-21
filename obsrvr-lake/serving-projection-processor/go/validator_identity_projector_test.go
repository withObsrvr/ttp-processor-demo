package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestValidatorIdentityProjectorLoadsAndNormalizesRadarNodes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/node" {
			t.Fatalf("unexpected Radar path %s", r.URL.Path)
		}
		fmt.Fprint(w, `[{
			"publicKey":" GVALIDATOR ",
			"name":" SDF Testnet 1 ",
			"alias":" sdf_testnet_1 ",
			"homeDomain":null,
			"organizationId":null,
			"dateUpdated":"2026-07-20T17:27:16.008Z",
			"isValidator":true,
			"ignoredVolatileField":42
		}]`)
	}))
	defer server.Close()

	projector := NewValidatorIdentityProjector("testnet", server.URL, time.Second, nil)
	nodes, err := projector.loadNodes(context.Background())
	if err != nil {
		t.Fatalf("loadNodes: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("got %d nodes, want 1", len(nodes))
	}
	if nodes[0].PublicKey != "GVALIDATOR" || nodes[0].Name != "SDF Testnet 1" || nodes[0].DisplayName != "SDF Testnet 1" {
		t.Fatalf("node was not normalized: %+v", nodes[0])
	}
	if !nodes[0].IsValidator {
		t.Fatalf("validator flag was lost: %+v", nodes[0])
	}
}

func TestValidatorIdentityFingerprintIgnoresVolatileRadarFields(t *testing.T) {
	before := normalizeRadarNodeIdentity(radarNodeIdentity{PublicKey: "GVALIDATOR", Name: "Node", DateUpdated: "2026-07-20T12:00:00Z", IsValidator: true})
	after := before
	after.DateUpdated = "2026-07-20T13:00:00Z"
	if validatorIdentityFingerprint(before) != validatorIdentityFingerprint(after) {
		t.Fatal("Radar observation timestamp incorrectly creates a new identity version")
	}
}

func TestValidatorIdentityProjectorRejectsOversizedRadarResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		_, _ = w.Write([]byte(strings.Repeat(" ", maxRadarNodeResponseBytes+1)))
	}))
	defer server.Close()

	projector := NewValidatorIdentityProjector("testnet", server.URL, time.Second, nil)
	_, err := projector.loadNodes(context.Background())
	if err == nil || !strings.Contains(err.Error(), "response exceeds") {
		t.Fatalf("loadNodes error = %v, want explicit response-size error", err)
	}
}

func TestValidatorIdentityProjectorRejectsEmptySnapshot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[]`))
	}))
	defer server.Close()

	projector := NewValidatorIdentityProjector("testnet", server.URL, time.Second, nil)
	_, err := projector.loadNodes(context.Background())
	if err == nil || !strings.Contains(err.Error(), "empty snapshot") {
		t.Fatalf("loadNodes error = %v, want empty-snapshot error", err)
	}
}

func TestValidatorIdentityProjectorRejectsTrailingJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"publicKey":"GVALIDATOR"}] {}`))
	}))
	defer server.Close()

	projector := NewValidatorIdentityProjector("testnet", server.URL, time.Second, nil)
	_, err := projector.loadNodes(context.Background())
	if err == nil || !strings.Contains(err.Error(), "trailing JSON") {
		t.Fatalf("loadNodes error = %v, want trailing-JSON error", err)
	}
}

type recordingValidatorIdentityTx struct {
	pgx.Tx
	queries []string
	args    [][]any
}

func (tx *recordingValidatorIdentityTx) Exec(_ context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	tx.queries = append(tx.queries, query)
	tx.args = append(tx.args, args)
	if strings.Contains(query, "DELETE FROM") {
		return pgconn.NewCommandTag("DELETE 2"), nil
	}
	return pgconn.NewCommandTag("UPDATE 2"), nil
}

func TestReconcileValidatorIdentitySnapshotClosesHistoryAndDeletesCurrent(t *testing.T) {
	tx := &recordingValidatorIdentityTx{}
	observedAt := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	keys := []string{"GONE", "GTWO"}

	retired, err := reconcileValidatorIdentitySnapshot(context.Background(), tx, "testnet", keys, observedAt)
	if err != nil {
		t.Fatalf("reconcileValidatorIdentitySnapshot: %v", err)
	}
	if retired != 2 {
		t.Fatalf("retired = %d, want 2", retired)
	}
	if len(tx.queries) != 2 {
		t.Fatalf("executed %d statements, want 2", len(tx.queries))
	}
	if !strings.Contains(tx.queries[0], "UPDATE serving.sv_validator_identity_history") ||
		!strings.Contains(tx.queries[0], "source = 'radar'") ||
		!strings.Contains(tx.queries[0], "valid_to IS NULL") {
		t.Fatalf("history reconciliation is not scoped correctly: %s", tx.queries[0])
	}
	if !strings.Contains(tx.queries[1], "DELETE FROM serving.sv_validator_identity_current") ||
		!strings.Contains(tx.queries[1], "source = 'radar'") {
		t.Fatalf("current reconciliation is not scoped correctly: %s", tx.queries[1])
	}
	if got, ok := tx.args[0][1].([]string); !ok || len(got) != len(keys) || got[0] != keys[0] || got[1] != keys[1] {
		t.Fatalf("observed keys argument = %#v, want %#v", tx.args[0][1], keys)
	}
}

func TestReconcileValidatorIdentitySnapshotRejectsEmptyKeys(t *testing.T) {
	tx := &recordingValidatorIdentityTx{}
	_, err := reconcileValidatorIdentitySnapshot(context.Background(), tx, "testnet", nil, time.Now())
	if err == nil || !strings.Contains(err.Error(), "refusing empty snapshot") {
		t.Fatalf("reconcile error = %v, want empty-snapshot guard", err)
	}
	if len(tx.queries) != 0 {
		t.Fatalf("empty snapshot executed %d statements", len(tx.queries))
	}
}
