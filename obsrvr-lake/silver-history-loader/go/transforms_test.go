package main

import (
	"strings"
	"testing"
)

func testLoader() *Loader {
	return &Loader{cfg: Config{Network: "mainnet", BronzeAlias: "bronze_catalog", BronzeSchema: "bronze", SilverAlias: "silver_catalog", SilverSchema: "silver"}}
}

func TestSelectContractMetadataUsesContractCreations(t *testing.T) {
	sql := selectContractMetadata(testLoader(), 100, 200)
	if !strings.Contains(sql, "contract_creations_v1") {
		t.Fatalf("expected contract_metadata to read contract_creations_v1, got SQL: %s", sql)
	}
	if strings.Contains(sql, "operations_row_v2") || strings.Contains(sql, "GROUP BY") {
		t.Fatalf("contract_metadata should not synthesize metadata from operations: %s", sql)
	}
	if !strings.Contains(sql, "created_ledger BETWEEN 100 AND 200") {
		t.Fatalf("expected created_ledger range filter, got SQL: %s", sql)
	}
}

func TestSelectTradesNormalizesAmountsAndFractionalPrice(t *testing.T) {
	sql := selectTrades(testLoader(), 100, 200)
	for _, want := range []string{
		"TRY_CAST(selling_amount AS BIGINT)",
		"TRY_CAST(buying_amount AS BIGINT)",
		"contains(CAST(price AS VARCHAR), '/')",
		"DECIMAL(20, 7)",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("expected %q in trades SQL: %s", want, sql)
		}
	}
}
