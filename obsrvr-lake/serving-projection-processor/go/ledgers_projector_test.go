package main

import (
	"database/sql"
	"strings"
	"testing"
)

func TestClaimableBalanceOperationDiscriminants(t *testing.T) {
	containsType := func(list string, operationType string) bool {
		compact := "," + strings.ReplaceAll(list, " ", "") + ","
		return strings.Contains(compact, ","+operationType+",")
	}

	if containsType(claimableBalanceOperationTypesSQL, "19") || !containsType(claimableBalanceOperationTypesSQL, "20") {
		t.Fatalf("claimable-balance operation types = %q, want type 20 and not type 19", claimableBalanceOperationTypesSQL)
	}
	if containsType(categorizedOperationTypesSQL, "19") || !containsType(categorizedOperationTypesSQL, "20") {
		t.Fatalf("categorized operation types = %q, want type 20 and not type 19", categorizedOperationTypesSQL)
	}
}

func TestCategoryCountTotalCoversEveryCategory(t *testing.T) {
	counts := ledgerOperationCategoryCounts{
		AccountCreation:   sql.NullInt32{Int32: 1, Valid: true},
		Payments:          sql.NullInt32{Int32: 2, Valid: true},
		OffersAndAMMs:     sql.NullInt32{Int32: 3, Valid: true},
		Trustlines:        sql.NullInt32{Int32: 4, Valid: true},
		ClaimableBalances: sql.NullInt32{Int32: 5, Valid: true},
		Sponsorship:       sql.NullInt32{Int32: 6, Valid: true},
		Soroban:           sql.NullInt32{Int32: 7, Valid: true},
		Other:             sql.NullInt32{Int32: 8, Valid: true},
	}

	if got := categoryCountTotal(counts); got != 36 {
		t.Fatalf("categoryCountTotal()=%d, want 36", got)
	}
}
