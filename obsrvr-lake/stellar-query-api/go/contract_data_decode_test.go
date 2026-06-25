package main

import (
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestDecodeContractDataXDRFieldsDecodesKeyAndValue(t *testing.T) {
	key, err := xdr.NewScVal(xdr.ScValTypeScvSymbol, xdr.ScSymbol("Balance"))
	if err != nil {
		t.Fatalf("key scval: %v", err)
	}
	amountKey, _ := xdr.NewScVal(xdr.ScValTypeScvSymbol, xdr.ScSymbol("amount"))
	amountVal, _ := xdr.NewScVal(xdr.ScValTypeScvI128, xdr.Int128Parts{Hi: 0, Lo: 123456789})
	m := xdr.ScMap{{Key: amountKey, Val: amountVal}}
	value, err := xdr.NewScVal(xdr.ScValTypeScvMap, &m)
	if err != nil {
		t.Fatalf("value scval: %v", err)
	}

	var cid xdr.ContractId
	contract, err := xdr.NewScAddress(xdr.ScAddressTypeScAddressTypeContract, cid)
	if err != nil {
		t.Fatalf("contract address: %v", err)
	}
	entry := xdr.ContractDataEntry{
		Contract:   contract,
		Key:        key,
		Durability: xdr.ContractDataDurabilityPersistent,
		Val:        value,
	}
	raw, err := xdr.MarshalBase64(entry)
	if err != nil {
		t.Fatalf("marshal contract data: %v", err)
	}

	keyXDR, valueXDR, keyDecoded, valueDecoded, keyErr, valueErr := DecodeContractDataXDRFields(raw)
	if keyErr != nil || valueErr != nil {
		t.Fatalf("decode errors: key=%v value=%v", keyErr, valueErr)
	}
	if keyXDR == nil || *keyXDR == "" || valueXDR == nil || *valueXDR == "" {
		t.Fatalf("expected key_xdr and value_xdr")
	}
	if keyDecoded == nil || keyDecoded.Type != "symbol" || keyDecoded.Value != "Balance" {
		t.Fatalf("unexpected key decode: %#v", keyDecoded)
	}
	if valueDecoded == nil || valueDecoded.Type != "map" {
		t.Fatalf("unexpected value decode: %#v", valueDecoded)
	}
}

func TestDecodeContractDataXDRFieldsReturnsPerRowErrors(t *testing.T) {
	_, _, keyDecoded, valueDecoded, keyErr, valueErr := DecodeContractDataXDRFields("not-base64")
	if keyDecoded != nil || valueDecoded != nil {
		t.Fatalf("expected no decoded values for malformed XDR")
	}
	if keyErr == nil || valueErr == nil {
		t.Fatalf("expected key and value errors")
	}
}
