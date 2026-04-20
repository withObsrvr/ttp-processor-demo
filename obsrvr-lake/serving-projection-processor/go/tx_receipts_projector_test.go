package main

import (
	"testing"
	"time"
)

func TestSelectCompleteLedgerBatch_TrimsPartialOverflowLedger(t *testing.T) {
	now := time.Unix(1710000000, 0).UTC()
	candidates := []txMeta{
		{TxHash: "tx-101-a", LedgerSequence: 101, CreatedAt: now},
		{TxHash: "tx-101-b", LedgerSequence: 101, CreatedAt: now},
		{TxHash: "tx-102-a", LedgerSequence: 102, CreatedAt: now},
		{TxHash: "tx-102-b", LedgerSequence: 102, CreatedAt: now},
		{TxHash: "tx-102-c", LedgerSequence: 102, CreatedAt: now},
	}

	selected, overflowLedger, needFullLedger := selectCompleteLedgerBatch(candidates, 3)
	if needFullLedger {
		t.Fatalf("expected trim behavior, got needFullLedger=true")
	}
	if overflowLedger != 102 {
		t.Fatalf("expected overflow ledger 102, got %d", overflowLedger)
	}
	if len(selected) != 2 {
		t.Fatalf("expected 2 selected candidates, got %d", len(selected))
	}
	for i, got := range selected {
		if got.LedgerSequence != 101 {
			t.Fatalf("selected[%d] ledger=%d, want 101", i, got.LedgerSequence)
		}
	}
}

func TestSelectCompleteLedgerBatch_RequestsWholeLedgerWhenFirstLedgerExceedsBatch(t *testing.T) {
	now := time.Unix(1710000000, 0).UTC()
	candidates := []txMeta{
		{TxHash: "tx-101-a", LedgerSequence: 101, CreatedAt: now},
		{TxHash: "tx-101-b", LedgerSequence: 101, CreatedAt: now},
		{TxHash: "tx-101-c", LedgerSequence: 101, CreatedAt: now},
		{TxHash: "tx-101-d", LedgerSequence: 101, CreatedAt: now},
	}

	selected, overflowLedger, needFullLedger := selectCompleteLedgerBatch(candidates, 3)
	if !needFullLedger {
		t.Fatalf("expected needFullLedger=true")
	}
	if overflowLedger != 101 {
		t.Fatalf("expected overflow ledger 101, got %d", overflowLedger)
	}
	if len(selected) != 0 {
		t.Fatalf("expected no selected candidates before full-ledger reload, got %d", len(selected))
	}
}

func TestSelectCompleteLedgerBatch_ReturnsAllWhenNoOverflow(t *testing.T) {
	now := time.Unix(1710000000, 0).UTC()
	candidates := []txMeta{
		{TxHash: "tx-101-a", LedgerSequence: 101, CreatedAt: now},
		{TxHash: "tx-101-b", LedgerSequence: 101, CreatedAt: now},
		{TxHash: "tx-102-a", LedgerSequence: 102, CreatedAt: now},
	}

	selected, overflowLedger, needFullLedger := selectCompleteLedgerBatch(candidates, 3)
	if needFullLedger {
		t.Fatalf("expected needFullLedger=false")
	}
	if overflowLedger != 0 {
		t.Fatalf("expected overflow ledger 0, got %d", overflowLedger)
	}
	if len(selected) != len(candidates) {
		t.Fatalf("expected all candidates to be selected, got %d want %d", len(selected), len(candidates))
	}
}

func TestBuildReceiptSemanticPayload_PrefersOperationDerivedSummary(t *testing.T) {
	asset := "VNM"
	source := "GD5EJ3SGABU3N7OQ44NSPT4I6YQFW3CD54Z7DYZXOLFNC4MM3JWJDT2Q"
	ops := []receiptOperation{{
		Index:         0,
		Type:          6,
		TypeName:      "change_trust",
		SourceAccount: &source,
		AssetCode:     &asset,
	}}

	summary := buildReceiptSummary(ops, nil)
	payload := buildReceiptSemanticPayload(txMeta{
		TxHash:         "tx",
		LedgerSequence: 123,
		CreatedAt:      time.Unix(1710000000, 0).UTC(),
		SourceAccount:  source,
		Successful:     false,
	}, ops, nil, summary)
	if payload.Classification.TxType != "trustline_change" {
		t.Fatalf("expected classification tx_type=trustline_change, got %q", payload.Classification.TxType)
	}
	if payload.LegacySummary.Type != "change_trust" {
		t.Fatalf("expected legacy summary type=change_trust, got %q", payload.LegacySummary.Type)
	}
	if payload.LegacySummary.Description != "Added trustline for VNM" {
		t.Fatalf("expected trustline description, got %q", payload.LegacySummary.Description)
	}
}

func TestDeriveBalanceDiffs_SignedNetByAccountAndAsset(t *testing.T) {
	accountFrom := "GBEHX6KXV5RIZWHL7U7NVCMGUUXR43RNMT33WKLPSHS7QLEJZUBHBNKT"
	accountTo := "GDQF6BZW4WEB3SMUNZ4FJXWXWSPGPES6ETUYRUVIS5LKUDDWYZJ22X4D"
	assetCode := "DSD"
	assetIssuer := "GAVRROR6AWJGKOHWLKUGO3OR3UHZ47GBXHHW6PBIVPG55VBMVD3TMLNA"
	amount := "0.2900000"

	diffs := deriveBalanceDiffs([]receiptEffect{
		{EffectType: "account_credited", AccountID: &accountTo, AssetCode: &assetCode, AssetIssuer: &assetIssuer, Amount: &amount},
		{EffectType: "account_debited", AccountID: &accountFrom, AssetCode: &assetCode, AssetIssuer: &assetIssuer, Amount: &amount},
	})
	if len(diffs) != 2 {
		t.Fatalf("expected 2 diffs, got %d", len(diffs))
	}
	got := map[string]string{}
	for _, diff := range diffs {
		got[diff.Account] = diff.Delta
		if diff.Asset != "DSD:GAVRROR6AWJGKOHWLKUGO3OR3UHZ47GBXHHW6PBIVPG55VBMVD3TMLNA" {
			t.Fatalf("unexpected asset key %q", diff.Asset)
		}
	}
	if got[accountFrom] != "-0.2900000" {
		t.Fatalf("expected sender delta -0.2900000, got %q", got[accountFrom])
	}
	if got[accountTo] != "0.2900000" {
		t.Fatalf("expected receiver delta 0.2900000, got %q", got[accountTo])
	}
}

func TestDeriveBalanceDiffs_NetsOpposingEffects(t *testing.T) {
	account := "GA123"
	assetCode := "USD"
	issuer := "GI123"
	credit := "5.0000000"
	debit := "2.0000000"

	diffs := deriveBalanceDiffs([]receiptEffect{
		{EffectType: "account_credited", AccountID: &account, AssetCode: &assetCode, AssetIssuer: &issuer, Amount: &credit},
		{EffectType: "account_debited", AccountID: &account, AssetCode: &assetCode, AssetIssuer: &issuer, Amount: &debit},
	})
	if len(diffs) != 1 {
		t.Fatalf("expected 1 diff, got %d", len(diffs))
	}
	if diffs[0].Delta != "3.0000000" {
		t.Fatalf("expected net delta 3.0000000, got %q", diffs[0].Delta)
	}
}
