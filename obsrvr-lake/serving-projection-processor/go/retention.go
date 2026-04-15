package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func applyRecentRetention(ctx context.Context, tx pgx.Tx, tableName, timeColumn, intervalLiteral string) (int64, error) {
	query := fmt.Sprintf("DELETE FROM %s WHERE %s < NOW() - INTERVAL '%s'", tableName, timeColumn, intervalLiteral)
	ct, err := tx.Exec(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("apply retention %s: %w", tableName, err)
	}
	return ct.RowsAffected(), nil
}
