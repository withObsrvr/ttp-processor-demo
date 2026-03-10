package main

import (
	"encoding/hex"

	"github.com/stellar/go-stellar-sdk/strkey"
)

// hexToStrKey converts a 64-char hex contract ID to a 56-char Stellar C-encoded strkey.
// If the input is already a valid strkey (56 chars starting with 'C'), it is returned as-is.
func hexToStrKey(hexID string) (string, error) {
	if len(hexID) == 56 && hexID[0] == 'C' {
		return hexID, nil
	}

	hashBytes, err := hex.DecodeString(hexID)
	if err != nil {
		return "", err
	}

	return strkey.Encode(strkey.VersionByteContract, hashBytes)
}
