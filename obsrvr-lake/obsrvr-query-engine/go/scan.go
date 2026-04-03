package main

import (
	"database/sql"
	"fmt"
	"strconv"
)

// scanRowsToMaps converts sql.Rows into a slice of maps keyed by column name.
// This is used for endpoints where defining typed structs adds no value.
func scanRowsToMaps(rows *sql.Rows) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	var results []map[string]interface{}

	for rows.Next() {
		values := make([]interface{}, len(columns))
		ptrs := make([]interface{}, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		row := make(map[string]interface{}, len(columns))
		for i, col := range columns {
			val := values[i]
			// Convert []byte to string for JSON serialization
			if b, ok := val.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = val
			}
		}
		// Convert numeric amount/balance fields to strings (stellar convention)
		stringifyAmounts(row)
		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	return results, nil
}

// amountFields are column names that should be serialized as strings in JSON
// to match stellar-query-api's convention (amounts are strings to preserve precision).
var amountFields = map[string]bool{
	"amount":             true,
	"balance":            true,
	"selling_amount":     true,
	"buying_amount":      true,
	"source_amount":      true,
	"starting_balance":   true,
	"total_supply":       true,
	"circulating_supply": true,
	"volume_24h":         true,
	"total_volume":       true,
	"total_coins":        true,
	"fee_pool":           true,
	"trust_limit":        true,
	"buying_liabilities":  true,
	"selling_liabilities": true,
	"total_pool_shares":  true,
	"asset_a_amount":     true,
	"asset_b_amount":     true,
}

// stringifyAmounts converts numeric amount fields to strings in a row map.
func stringifyAmounts(row map[string]interface{}) {
	for key, val := range row {
		if !amountFields[key] {
			continue
		}
		switch v := val.(type) {
		case int64:
			row[key] = strconv.FormatInt(v, 10)
		case int32:
			row[key] = strconv.FormatInt(int64(v), 10)
		case float64:
			row[key] = strconv.FormatFloat(v, 'f', -1, 64)
		case float32:
			row[key] = strconv.FormatFloat(float64(v), 'f', -1, 32)
		}
	}
}

// scanRowsToMapsStringified scans rows and converts amount fields to strings.
func scanRowsToMapsStringified(rows *sql.Rows) ([]map[string]interface{}, error) {
	results, err := scanRowsToMaps(rows)
	if err != nil {
		return nil, err
	}
	for _, row := range results {
		stringifyAmounts(row)
	}
	return results, nil
}
