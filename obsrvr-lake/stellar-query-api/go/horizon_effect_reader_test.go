package main

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stellar/go-stellar-sdk/toid"
)

var horizonEffectColumns = []string{
	"ledger_sequence", "transaction_hash", "operation_index", "effect_index",
	"operation_id", "effect_type", "effect_type_string", "account_id",
	"amount", "asset_code", "asset_issuer", "asset_type",
	"details_json",
	"trustline_limit", "authorize_flag", "clawback_flag",
	"signer_account", "signer_weight", "offer_id", "seller_account",
	"created_at",
}

func TestUnifiedGetEffectsOperationFilterUsesHotFirst(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	operationID := toid.New(123, 1, 1).ToInt64()
	reader := &UnifiedDuckDBReader{db: db, hotSchema: "hot", coldSchema: "cold"}
	mock.ExpectQuery(regexp.QuoteMeta("FROM hot.effects")).
		WithArgs(int64(123), int64(0), operationID, 2).
		WillReturnRows(sqlmock.NewRows(horizonEffectColumns).AddRow(
			int64(123), "txhash", 0, 0,
			operationID, int32(2), "account_credited", "GA",
			"1.0000000", "USD", "GISSUER", "credit_alphanum4",
			nil, nil, nil, nil, nil, nil, nil, nil,
			time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC),
		))

	effects, _, _, err := reader.GetEffects(context.Background(), EffectFilters{
		LedgerSequence: 123,
		OperationID:    &operationID,
		Limit:          1,
		Order:          "asc",
	})
	if err != nil {
		t.Fatalf("GetEffects: %v", err)
	}
	if len(effects) != 1 || effects[0].OperationID == nil || *effects[0].OperationID != operationID {
		t.Fatalf("effects = %#v", effects)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestUnifiedGetEffectsRecentDescUsesHotFirst(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	operationID := toid.New(200, 1, 1).ToInt64()
	reader := &UnifiedDuckDBReader{db: db, hotSchema: "hot", coldSchema: "cold"}
	mock.ExpectQuery(regexp.QuoteMeta("FROM hot.effects")).
		WithArgs(2).
		WillReturnRows(sqlmock.NewRows(horizonEffectColumns).AddRow(
			int64(200), "txhash", 0, 0,
			operationID, int32(1), "account_created", "GA",
			nil, nil, nil, nil,
			nil, nil, nil, nil, nil, nil, nil, nil,
			time.Date(2026, 7, 10, 12, 1, 0, 0, time.UTC),
		))

	effects, _, _, err := reader.GetEffects(context.Background(), EffectFilters{
		Limit: 1,
		Order: "desc",
	})
	if err != nil {
		t.Fatalf("GetEffects: %v", err)
	}
	if len(effects) != 1 || effects[0].EffectTypeString != "account_created" {
		t.Fatalf("effects = %#v", effects)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestUnifiedGetEffectsHorizonCursorUsesOperationIDOrder(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	operationID := toid.New(200, 1, 1).ToInt64()
	cursorOperationID := toid.New(201, 1, 1).ToInt64()
	reader := &UnifiedDuckDBReader{db: db, hotSchema: "hot", coldSchema: "cold"}
	mock.ExpectQuery("operation_id, effect_index\\) <").
		WithArgs(cursorOperationID, 1, 2).
		WillReturnRows(sqlmock.NewRows(horizonEffectColumns).AddRow(
			int64(200), "txhash", 0, 0,
			operationID, int32(1), "account_created", "GA",
			nil, nil, nil, nil,
			nil, nil, nil, nil, nil, nil, nil, nil,
			time.Date(2026, 7, 10, 12, 1, 0, 0, time.UTC),
		))

	effects, _, _, err := reader.GetEffects(context.Background(), EffectFilters{
		Limit:        1,
		Order:        "desc",
		HorizonOrder: true,
		Cursor:       &EffectCursor{OperationID: &cursorOperationID, EffectIndex: 1},
	})
	if err != nil {
		t.Fatalf("GetEffects: %v", err)
	}
	if len(effects) != 1 || effects[0].OperationID == nil || *effects[0].OperationID != operationID {
		t.Fatalf("effects = %#v", effects)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestUnifiedGetEffectsAccountDescUsesHotThenCold(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	hotOperationID := toid.New(200, 1, 1).ToInt64()
	coldOperationID := toid.New(100, 1, 1).ToInt64()
	reader := &UnifiedDuckDBReader{db: db, hotSchema: "hot", coldSchema: "cold"}
	mock.ExpectQuery(regexp.QuoteMeta("FROM hot.effects")).
		WithArgs("GA", 3).
		WillReturnRows(sqlmock.NewRows(horizonEffectColumns).AddRow(
			int64(200), "hottx", 0, 0,
			hotOperationID, int32(1), "account_created", "GA",
			nil, nil, nil, nil,
			nil, nil, nil, nil, nil, nil, nil, nil,
			time.Date(2026, 7, 10, 12, 1, 0, 0, time.UTC),
		))
	mock.ExpectQuery(regexp.QuoteMeta("FROM cold.effects")).
		WithArgs("GA", 2).
		WillReturnRows(sqlmock.NewRows(horizonEffectColumns).AddRow(
			int64(100), "coldtx", 0, 0,
			coldOperationID, int32(2), "account_credited", "GA",
			"1.0000000", "USD", "GISSUER", "credit_alphanum4",
			nil, nil, nil, nil, nil, nil, nil, nil,
			time.Date(2026, 7, 9, 12, 1, 0, 0, time.UTC),
		))

	effects, _, hasMore, err := reader.GetEffects(context.Background(), EffectFilters{
		AccountID: "GA",
		Limit:     2,
		Order:     "desc",
	})
	if err != nil {
		t.Fatalf("GetEffects: %v", err)
	}
	if hasMore {
		t.Fatal("hasMore = true, want false")
	}
	if len(effects) != 2 || effects[0].TransactionHash != "hottx" || effects[1].TransactionHash != "coldtx" {
		t.Fatalf("effects = %#v", effects)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHorizonEffectReaderUsesServingAccountEffectsWhenCovered(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	operationID := toid.New(20, 1, 1).ToInt64()
	nextOperationID := toid.New(19, 1, 1).ToInt64()
	closedAt := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)

	mock.ExpectQuery("FROM serving.sv_watermarks").
		WillReturnRows(sqlmock.NewRows([]string{"status", "complete_from", "complete_thru"}).
			AddRow("complete", int64(1), int64(20)))
	mock.ExpectQuery("FROM serving.sv_effects_by_account").
		WithArgs("GA", int64(1), int64(20), 2).
		WillReturnRows(sqlmock.NewRows(horizonEffectColumns).
			AddRow(
				int64(20), "txhash", 0, 0,
				operationID, 2, "account_credited", "GA",
				"2.5000000", "USD", "GISSUER", "credit_alphanum4",
				nil, nil, nil, nil, nil, nil, nil, nil,
				closedAt,
			).
			AddRow(
				int64(19), "nexttx", 0, 0,
				nextOperationID, 3, "account_debited", "GA",
				"1.0000000", "XLM", nil, "native",
				nil, nil, nil, nil, nil, nil, nil, nil,
				closedAt,
			))

	reader := NewHorizonEffectReader(
		&UnifiedDuckDBReader{db: db, hotSchema: "hot", coldSchema: "cold"},
		&SilverHotReader{db: db, network: "testnet"},
	)
	effects, next, hasMore, err := reader.GetEffects(context.Background(), EffectFilters{
		AccountID: "GA",
		Limit:     1,
		Order:     "desc",
	})
	if err != nil {
		t.Fatalf("GetEffects: %v", err)
	}
	if !hasMore || next == "" {
		t.Fatalf("pagination = hasMore %v next %q", hasMore, next)
	}
	if len(effects) != 1 {
		t.Fatalf("effects len = %d", len(effects))
	}
	effect := effects[0]
	if effect.OperationID == nil || *effect.OperationID != operationID || effect.TransactionHash != "txhash" {
		t.Fatalf("effect = %+v", effect)
	}
	if effect.Asset == nil || effect.Asset.Code != "USD" || effect.Asset.Issuer == nil || *effect.Asset.Issuer != "GISSUER" {
		t.Fatalf("asset = %+v", effect.Asset)
	}
	if effect.Amount == nil || *effect.Amount != "2.5000000" {
		t.Fatalf("amount = %v", effect.Amount)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHorizonEffectReaderUsesServingTransactionEffectsWhenCovered(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	operationID := toid.New(100, 1, 1).ToInt64()
	closedAt := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)

	mock.ExpectQuery("FROM serving.sv_watermarks").
		WillReturnRows(sqlmock.NewRows([]string{"status", "complete_from", "complete_thru"}).
			AddRow("complete", int64(1), int64(200)))
	mock.ExpectQuery("FROM serving.sv_effects_by_account").
		WithArgs("txhash", int64(100), 2).
		WillReturnRows(sqlmock.NewRows(horizonEffectColumns).
			AddRow(
				int64(100), "txhash", 0, 0,
				operationID, 2, "account_credited", "GA",
				"2.5000000", "USD", "GISSUER", "credit_alphanum4",
				nil, nil, nil, nil, nil, nil, nil, nil,
				closedAt,
			))

	reader := NewHorizonEffectReader(
		&UnifiedDuckDBReader{db: db, hotSchema: "hot", coldSchema: "cold"},
		&SilverHotReader{db: db, network: "testnet"},
	)
	effects, _, hasMore, err := reader.GetEffects(context.Background(), EffectFilters{
		TransactionHash: "txhash",
		LedgerSequence:  100,
		Limit:           1,
		Order:           "desc",
		HorizonOrder:    true,
	})
	if err != nil {
		t.Fatalf("GetEffects: %v", err)
	}
	if hasMore {
		t.Fatalf("hasMore = true, want false")
	}
	if len(effects) != 1 || effects[0].OperationID == nil || *effects[0].OperationID != operationID {
		t.Fatalf("effects = %+v", effects)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHorizonEffectReaderUsesServingOperationEffectsWhenCovered(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	operationID := toid.New(100, 1, 1).ToInt64()
	closedAt := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)

	mock.ExpectQuery("FROM serving.sv_watermarks").
		WillReturnRows(sqlmock.NewRows([]string{"status", "complete_from", "complete_thru"}).
			AddRow("complete", int64(1), int64(200)))
	mock.ExpectQuery("FROM serving.sv_effects_by_account").
		WithArgs(operationID, 2).
		WillReturnRows(sqlmock.NewRows(horizonEffectColumns).
			AddRow(
				int64(100), "txhash", 0, 0,
				operationID, 2, "account_credited", "GA",
				"2.5000000", "USD", "GISSUER", "credit_alphanum4",
				nil, nil, nil, nil, nil, nil, nil, nil,
				closedAt,
			))

	reader := NewHorizonEffectReader(
		&UnifiedDuckDBReader{db: db, hotSchema: "hot", coldSchema: "cold"},
		&SilverHotReader{db: db, network: "testnet"},
	)
	effects, _, hasMore, err := reader.GetEffects(context.Background(), EffectFilters{
		OperationID:  &operationID,
		Limit:        1,
		Order:        "desc",
		HorizonOrder: true,
	})
	if err != nil {
		t.Fatalf("GetEffects: %v", err)
	}
	if hasMore {
		t.Fatalf("hasMore = true, want false")
	}
	if len(effects) != 1 || effects[0].OperationID == nil || *effects[0].OperationID != operationID {
		t.Fatalf("effects = %+v", effects)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHorizonEffectReaderUsesStaleServingAccountEffectsWhenComplete(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	operationID := toid.New(20, 1, 1).ToInt64()
	closedAt := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)

	mock.ExpectQuery("FROM serving.sv_watermarks").
		WillReturnRows(sqlmock.NewRows([]string{"status", "complete_from", "complete_thru"}).
			AddRow("complete", int64(1), int64(20)))
	mock.ExpectQuery("FROM serving.sv_effects_by_account").
		WithArgs("GA", int64(1), int64(20), 2).
		WillReturnRows(sqlmock.NewRows(horizonEffectColumns).
			AddRow(
				int64(20), "txhash", 0, 0,
				operationID, 2, "account_credited", "GA",
				"2.5000000", "USD", "GISSUER", "credit_alphanum4",
				nil, nil, nil, nil, nil, nil, nil, nil,
				closedAt,
			))

	reader := NewHorizonEffectReader(
		&UnifiedDuckDBReader{db: db, hotSchema: "hot", coldSchema: "cold"},
		&SilverHotReader{db: db, network: "testnet"},
	)
	effects, _, hasMore, err := reader.GetEffects(context.Background(), EffectFilters{
		AccountID: "GA",
		Limit:     1,
		Order:     "desc",
	})
	if err != nil {
		t.Fatalf("GetEffects: %v", err)
	}
	if hasMore {
		t.Fatalf("hasMore = true, want false")
	}
	if len(effects) != 1 || effects[0].OperationID == nil || *effects[0].OperationID != operationID {
		t.Fatalf("effects = %+v", effects)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
