package main

import (
	"encoding/base64"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestDecodeTxMetaXDRHandlesV4TxLevelChanges(t *testing.T) {
	before := testAccountLedgerEntry(10000000)
	after := testAccountLedgerEntry(25000000)

	stateChange, err := xdr.NewLedgerEntryChange(xdr.LedgerEntryChangeTypeLedgerEntryState, before)
	if err != nil {
		t.Fatalf("state change: %v", err)
	}
	updatedChange, err := xdr.NewLedgerEntryChange(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, after)
	if err != nil {
		t.Fatalf("updated change: %v", err)
	}

	txMeta, err := xdr.NewTransactionMeta(4, xdr.TransactionMetaV4{
		TxChangesBefore: xdr.LedgerEntryChanges{stateChange},
		TxChangesAfter:  xdr.LedgerEntryChanges{updatedChange},
	})
	if err != nil {
		t.Fatalf("transaction meta: %v", err)
	}
	metaBytes, err := txMeta.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal tx meta: %v", err)
	}

	balanceChanges, stateChanges, err := decodeTxMetaXDR(base64.StdEncoding.EncodeToString(metaBytes))
	if err != nil {
		t.Fatalf("decode tx meta: %v", err)
	}
	if len(stateChanges) != 0 {
		t.Fatalf("expected no state changes, got %d", len(stateChanges))
	}
	if len(balanceChanges) != 1 {
		t.Fatalf("expected 1 balance change, got %d: %#v", len(balanceChanges), balanceChanges)
	}

	change := balanceChanges[0]
	if change.AssetCode != "XLM" || change.AssetType != "native" {
		t.Fatalf("unexpected asset: %#v", change)
	}
	if change.Before != "1.0000000" || change.After != "2.5000000" || change.Delta != "+1.5000000" {
		t.Fatalf("unexpected balance delta: %#v", change)
	}
}

func TestDecodeTxMetaXDRHandlesV4OperationChanges(t *testing.T) {
	created := testAccountLedgerEntry(30000000)
	createdChange, err := xdr.NewLedgerEntryChange(xdr.LedgerEntryChangeTypeLedgerEntryCreated, created)
	if err != nil {
		t.Fatalf("created change: %v", err)
	}

	txMeta, err := xdr.NewTransactionMeta(4, xdr.TransactionMetaV4{
		Operations: []xdr.OperationMetaV2{{
			Changes: xdr.LedgerEntryChanges{createdChange},
		}},
	})
	if err != nil {
		t.Fatalf("transaction meta: %v", err)
	}
	metaBytes, err := txMeta.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal tx meta: %v", err)
	}

	balanceChanges, _, err := decodeTxMetaXDR(base64.StdEncoding.EncodeToString(metaBytes))
	if err != nil {
		t.Fatalf("decode tx meta: %v", err)
	}
	if len(balanceChanges) != 1 {
		t.Fatalf("expected 1 balance change, got %d: %#v", len(balanceChanges), balanceChanges)
	}
	if balanceChanges[0].Before != "0.0000000" || balanceChanges[0].After != "3.0000000" || balanceChanges[0].Delta != "+3.0000000" {
		t.Fatalf("unexpected balance delta: %#v", balanceChanges[0])
	}
}

func testAccountLedgerEntry(balance int64) xdr.LedgerEntry {
	account := xdr.AccountEntry{
		AccountId: xdr.MustAddress("GCR22L3WS7TP72S4Z27YTO6JIQYDJK2KLS2TQNHK6Y7XYPA3AGT3X4FH"),
		Balance:   xdr.Int64(balance),
	}
	return xdr.LedgerEntry{
		Data: xdr.LedgerEntryData{
			Type:    xdr.LedgerEntryTypeAccount,
			Account: &account,
		},
	}
}
