package main

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema/serving_schema.sql
var servingSchemaSQL string

func EnsureServingSchema(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, servingSchemaSQL); err != nil {
		return fmt.Errorf("apply serving schema: %w", err)
	}
	return nil
}
