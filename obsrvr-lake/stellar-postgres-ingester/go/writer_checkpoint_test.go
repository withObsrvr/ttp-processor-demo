package main

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type fakeTx struct {
	commitErr error
	commits   int
}

func (f *fakeTx) Begin(ctx context.Context) (pgx.Tx, error) {
	return nil, errors.New("not implemented")
}
func (f *fakeTx) Commit(ctx context.Context) error {
	f.commits++
	return f.commitErr
}
func (f *fakeTx) Rollback(ctx context.Context) error { return nil }
func (f *fakeTx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("not implemented")
}
func (f *fakeTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults { return nil }
func (f *fakeTx) LargeObjects() pgx.LargeObjects                               { return pgx.LargeObjects{} }
func (f *fakeTx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	return nil, errors.New("not implemented")
}
func (f *fakeTx) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("not implemented")
}
func (f *fakeTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return nil, errors.New("not implemented")
}
func (f *fakeTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row { return nil }
func (f *fakeTx) Conn() *pgx.Conn                                               { return nil }

func TestCommitAndPersistCheckpointDoesNotAdvanceOnCommitFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkpoint.json")
	checkpoint, err := NewCheckpoint(path)
	if err != nil {
		t.Fatalf("NewCheckpoint: %v", err)
	}
	if err := checkpoint.Update(9, "hash-9", 1, 90, 900); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}
	if err := checkpoint.Save(); err != nil {
		t.Fatalf("save seed checkpoint: %v", err)
	}

	commitErr := errors.New("commit failed")
	tx := &fakeTx{commitErr: commitErr}
	err = commitAndPersistCheckpoint(context.Background(), tx, checkpoint, []checkpointUpdate{
		{ledgerSeq: 10, ledgerHash: "hash-10", ledgerRange: 1, txCount: 100, opCount: 1000},
		{ledgerSeq: 11, ledgerHash: "hash-11", ledgerRange: 1, txCount: 110, opCount: 1100},
	})
	if err == nil {
		t.Fatal("expected commit error")
	}
	if tx.commits != 1 {
		t.Fatalf("expected one commit attempt, got %d", tx.commits)
	}
	if got := checkpoint.GetLastLedger(); got != 9 {
		t.Fatalf("in-memory checkpoint advanced on failed commit: got %d want 9", got)
	}

	reloaded, err := NewCheckpoint(path)
	if err != nil {
		t.Fatalf("reload checkpoint: %v", err)
	}
	if got := reloaded.GetLastLedger(); got != 9 {
		t.Fatalf("persisted checkpoint advanced on failed commit: got %d want 9", got)
	}
}

func TestCommitAndPersistCheckpointAdvancesAfterSuccessfulCommit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkpoint.json")
	checkpoint, err := NewCheckpoint(path)
	if err != nil {
		t.Fatalf("NewCheckpoint: %v", err)
	}
	if err := checkpoint.Update(9, "hash-9", 1, 90, 900); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}
	if err := checkpoint.Save(); err != nil {
		t.Fatalf("save seed checkpoint: %v", err)
	}

	tx := &fakeTx{}
	err = commitAndPersistCheckpoint(context.Background(), tx, checkpoint, []checkpointUpdate{
		{ledgerSeq: 10, ledgerHash: "hash-10", ledgerRange: 1, txCount: 100, opCount: 1000},
		{ledgerSeq: 11, ledgerHash: "hash-11", ledgerRange: 1, txCount: 110, opCount: 1100},
	})
	if err != nil {
		t.Fatalf("commitAndPersistCheckpoint: %v", err)
	}
	if tx.commits != 1 {
		t.Fatalf("expected one commit, got %d", tx.commits)
	}

	reloaded, err := NewCheckpoint(path)
	if err != nil {
		t.Fatalf("reload checkpoint: %v", err)
	}
	lastLedger, totalLedgers, totalTx, totalOps := reloaded.GetStats()
	if lastLedger != 11 {
		t.Fatalf("last ledger = %d, want 11", lastLedger)
	}
	if totalLedgers != 3 {
		t.Fatalf("total ledgers = %d, want 3", totalLedgers)
	}
	if totalTx != 300 {
		t.Fatalf("total transactions = %d, want 300", totalTx)
	}
	if totalOps != 3000 {
		t.Fatalf("total operations = %d, want 3000", totalOps)
	}
}
