package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"

	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
)

// computeContractDataKeyHash builds the LedgerKey for a Soroban contract-data entry
// and returns its hex-encoded SHA-256 hash (same scheme the ingester uses when
// writing ttl_current.key_hash).
//
// Exactly one of keyB64 (base64 XDR-marshaled ScVal) or keyType ("instance") must
// be provided. durability must be "persistent" or "temporary"; defaults to
// "persistent" when empty.
func computeContractDataKeyHash(contractID, durability, keyB64, keyType string) (string, error) {
	rawContract, err := strkey.Decode(strkey.VersionByteContract, contractID)
	if err != nil {
		return "", fmt.Errorf("decode contract_id: %w", err)
	}
	if len(rawContract) != 32 {
		return "", fmt.Errorf("unexpected contract_id length: %d", len(rawContract))
	}
	var contractHash xdr.ContractId
	copy(contractHash[:], rawContract)

	dur, err := parseDurability(durability)
	if err != nil {
		return "", err
	}

	var keyScVal xdr.ScVal
	switch {
	case keyType == "instance":
		keyScVal = xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance}
	case keyB64 != "":
		decoded, err := base64.StdEncoding.DecodeString(keyB64)
		if err != nil {
			return "", fmt.Errorf("decode key (base64): %w", err)
		}
		if err := xdr.SafeUnmarshal(decoded, &keyScVal); err != nil {
			return "", fmt.Errorf("unmarshal key ScVal: %w", err)
		}
	default:
		return "", fmt.Errorf("key or key_type required")
	}

	ledgerKey := xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeContractData,
		ContractData: &xdr.LedgerKeyContractData{
			Contract: xdr.ScAddress{
				Type:       xdr.ScAddressTypeScAddressTypeContract,
				ContractId: &contractHash,
			},
			Key:        keyScVal,
			Durability: dur,
		},
	}

	raw, err := ledgerKey.MarshalBinary()
	if err != nil {
		return "", fmt.Errorf("marshal ledger key: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func parseDurability(s string) (xdr.ContractDataDurability, error) {
	switch s {
	case "", "persistent":
		return xdr.ContractDataDurabilityPersistent, nil
	case "temporary":
		return xdr.ContractDataDurabilityTemporary, nil
	default:
		return 0, fmt.Errorf("durability must be 'persistent' or 'temporary', got %q", s)
	}
}
