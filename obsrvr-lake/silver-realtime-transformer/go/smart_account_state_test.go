package main

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func TestParseSmartAccountSignerExtractsExternalCredential(t *testing.T) {
	credential := "DEADBEEF00112233"
	rawBytes := strings.Repeat("a", 130) + credential
	input := []interface{}{
		"External",
		map[string]interface{}{"type": "address", "address": "GABC"},
		map[string]interface{}{"type": "bytes", "hex": rawBytes, "length": len(rawBytes) / 2},
	}

	signer, ok := parseSmartAccountSigner(input)
	if !ok {
		t.Fatal("parseSmartAccountSigner returned ok=false")
	}
	if !signer.signerType.Valid || signer.signerType.String != "External" {
		t.Fatalf("signer_type = %#v, want External", signer.signerType)
	}
	if !signer.signerAddress.Valid || signer.signerAddress.String != "GABC" {
		t.Fatalf("signer_address = %#v, want GABC", signer.signerAddress)
	}
	if !signer.rawBytes.Valid || signer.rawBytes.String != strings.ToLower(rawBytes) {
		t.Fatalf("raw_bytes = %#v, want %s", signer.rawBytes, strings.ToLower(rawBytes))
	}
	if !signer.credentialID.Valid || signer.credentialID.String != strings.ToLower(credential) {
		t.Fatalf("credential_id = %#v, want %s", signer.credentialID, strings.ToLower(credential))
	}
}

func TestParseSmartAccountSignerDoesNotTreatPublicKeyAsCredential(t *testing.T) {
	rawBytes := strings.Repeat("a", 130)
	input := []interface{}{
		"External",
		map[string]interface{}{"type": "contract", "address": "CVERIFIER"},
		map[string]interface{}{"type": "bytes", "hex": rawBytes, "length": len(rawBytes) / 2},
	}

	signer, ok := parseSmartAccountSigner(input)
	if !ok {
		t.Fatal("parseSmartAccountSigner returned ok=false")
	}
	if !signer.rawBytes.Valid || signer.rawBytes.String != rawBytes {
		t.Fatalf("raw_bytes = %#v, want %s", signer.rawBytes, rawBytes)
	}
	if signer.credentialID.Valid {
		t.Fatalf("credential_id = %#v, want invalid", signer.credentialID)
	}
}

func TestParseSmartAccountSignerHandlesDelegatedAccount(t *testing.T) {
	input := []interface{}{
		"Delegated",
		map[string]interface{}{"type": "account", "address": "GDELEGATED"},
	}

	signer, ok := parseSmartAccountSigner(input)
	if !ok {
		t.Fatal("parseSmartAccountSigner returned ok=false")
	}
	if !signer.signerType.Valid || signer.signerType.String != "Delegated" {
		t.Fatalf("signer_type = %#v, want Delegated", signer.signerType)
	}
	if !signer.signerAddress.Valid || signer.signerAddress.String != "GDELEGATED" {
		t.Fatalf("signer_address = %#v, want GDELEGATED", signer.signerAddress)
	}
	if signer.rawBytes.Valid {
		t.Fatalf("raw_bytes = %#v, want invalid", signer.rawBytes)
	}
	if signer.credentialID.Valid {
		t.Fatalf("credential_id = %#v, want invalid", signer.credentialID)
	}
}

func TestSmartAccountDataEntriesHandlesNormalizedMap(t *testing.T) {
	entries, err := smartAccountDataEntries(sql.NullString{Valid: true, String: `{"type":"map","entries":{"signer_ids":[1,2],"policy_id":{"u32":7}},"keys":["signer_ids","policy_id"]}`})
	if err != nil {
		t.Fatalf("smartAccountDataEntries: %v", err)
	}
	if ids := smartAccountIDList(entries["signer_ids"]); len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Fatalf("signer_ids = %#v, want [1 2]", ids)
	}
	if id, ok := smartAccountEntryID(entries["policy_id"]); !ok || id != 7 {
		t.Fatalf("policy_id = %d/%v, want 7/true", id, ok)
	}
}

func TestSmartAccountEventOrderUsesTOIDWhenAvailable(t *testing.T) {
	order := smartAccountEventOrder(smartAccountEvent{
		ledgerSequence: 100,
		transactionID:  sql.NullInt64{Int64: 429496729600, Valid: true},
		operationIndex: sql.NullInt64{Int64: 2, Valid: true},
	})
	if order != 429496729603 {
		t.Fatalf("event order = %d, want 429496729603", order)
	}
}

func TestSmartAccountTopicIDHandlesJSONDecodedSCVal(t *testing.T) {
	id, ok := smartAccountTopicID(sql.NullString{Valid: true, String: `{"type":"u32","value":42}`})
	if !ok || id != 42 {
		t.Fatalf("topic id = %d/%v, want 42/true", id, ok)
	}
}

func TestApplySmartAccountContextRuleAddRemoveReAdd(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	add := smartAccountTestEvent("context_rule_added", 100, 1)
	remove := smartAccountTestEvent("context_rule_removed", 101, 2)
	reAdd := smartAccountTestEvent("context_rule_added", 102, 3)
	entries := map[string]interface{}{
		"signers": []interface{}{
			[]interface{}{
				"Delegated",
				map[string]interface{}{"type": "account", "address": "GOWNER"},
			},
		},
	}

	expectRuleUpsert(mock, add, true)
	expectRuleSignerUpsert(mock, add, true)
	expectRuleUpsert(mock, remove, false)
	expectRuleUpsert(mock, reAdd, true)
	expectRuleSignerUpsert(mock, reAdd, true)
	mock.ExpectCommit()

	rt := &RealtimeTransformer{}
	cache := newSmartAccountCache()
	if err := rt.applySmartAccountEvent(context.Background(), tx, add, entries, cache); err != nil {
		t.Fatalf("apply add: %v", err)
	}
	if err := rt.applySmartAccountEvent(context.Background(), tx, remove, map[string]interface{}{}, cache); err != nil {
		t.Fatalf("apply remove: %v", err)
	}
	if err := rt.applySmartAccountEvent(context.Background(), tx, reAdd, entries, cache); err != nil {
		t.Fatalf("apply re-add: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func smartAccountTestEvent(eventName string, ledger, eventIndex int64) smartAccountEvent {
	ev := smartAccountEvent{
		contractID:      "CCONTRACT",
		ledgerSequence:  ledger,
		transactionHash: "txhash",
		eventName:       eventName,
		topic1:          sql.NullString{String: "7", Valid: true},
		operationIndex:  sql.NullInt64{Int64: 0, Valid: true},
		eventIndex:      sql.NullInt64{Int64: eventIndex, Valid: true},
		transactionID:   sql.NullInt64{Int64: ledger << 32, Valid: true},
	}
	ev.eventOrder = smartAccountEventOrder(ev)
	return ev
}

func expectRuleUpsert(mock sqlmock.Sqlmock, ev smartAccountEvent, active bool) {
	mock.ExpectExec("INSERT INTO smart_account_context_rules").
		WithArgs(
			ev.contractID,
			int64(7),
			active,
			nil,
			ev.eventName,
			ev.ledgerSequence,
			ev.transactionHash,
			int(ev.operationIndex.Int64),
			int(ev.eventIndex.Int64),
			ev.eventOrder,
			nil,
		).
		WillReturnResult(sqlmock.NewResult(1, 1))
}

func expectRuleSignerUpsert(mock sqlmock.Sqlmock, ev smartAccountEvent, active bool) {
	mock.ExpectExec("INSERT INTO smart_account_signers").
		WithArgs(
			ev.contractID,
			"rule",
			int64(7),
			"Delegated|GOWNER|",
			nil,
			"Delegated",
			"GOWNER",
			nil,
			nil,
			active,
			ev.eventName,
			ev.ledgerSequence,
			ev.transactionHash,
			int(ev.operationIndex.Int64),
			int(ev.eventIndex.Int64),
			ev.eventOrder,
			nil,
		).
		WillReturnResult(sqlmock.NewResult(1, 1))
}
