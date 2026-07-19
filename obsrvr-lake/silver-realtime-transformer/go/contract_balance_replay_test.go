package main

import (
	"context"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func TestRunContractBalanceReplayIsCheckpointNeutralAndBatched(t *testing.T) {
	bronzeDB, bronzeMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("bronze sqlmock: %v", err)
	}
	defer bronzeDB.Close()

	silverDB, silverMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("silver sqlmock: %v", err)
	}
	defer silverDB.Close()

	for rangeIndex := 0; rangeIndex < 2; rangeIndex++ {
		silverMock.ExpectBegin()
		bronzeMock.ExpectQuery(`(?s).*contract_data_snapshot_v1.*`).
			WillReturnRows(sqlmock.NewRows([]string{
				"contract_id", "balance_holder", "balance", "asset_type", "asset_code", "asset_issuer",
				"symbol", "decimals", "key_hash", "ledger_sequence", "closed_at", "deleted",
			}))
		silverMock.ExpectCommit()
	}

	sourceManager := NewSourceManager(NewBronzeReader(bronzeDB), nil, false)
	transformer := NewRealtimeTransformer(
		&Config{}, sourceManager, NewSilverWriter(silverDB), nil, silverDB,
	)
	if err := transformer.RunContractBalanceReplay(context.Background(), 100, 104, 3, false); err != nil {
		t.Fatalf("RunContractBalanceReplay: %v", err)
	}
	if sourceManager.GetMode() != SourceModeHot {
		t.Fatalf("source mode = %s, want hot", sourceManager.GetMode())
	}
	if err := bronzeMock.ExpectationsWereMet(); err != nil {
		t.Fatalf("bronze expectations: %v", err)
	}
	if err := silverMock.ExpectationsWereMet(); err != nil {
		t.Fatalf("silver expectations: %v", err)
	}
}

func TestRunContractBalanceReplayRejectsInvalidRange(t *testing.T) {
	transformer := &RealtimeTransformer{}
	if err := transformer.RunContractBalanceReplay(context.Background(), 0, 100, 1000, false); err == nil {
		t.Fatal("expected invalid range error")
	}
}
