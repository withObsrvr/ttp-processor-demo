package main

import (
	"fmt"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// DecodeContractDataXDRFields decodes the full ContractDataEntry XDR persisted in
// contract_data_current.data_value. The persisted XDR contains both the original
// storage key and value, so query-time decoding does not require a backfill.
func DecodeContractDataXDRFields(contractDataXDR string) (keyXDR *string, valueXDR *string, keyDecoded *DecodedScVal, valueDecoded *DecodedScVal, keyErr *string, valueErr *string) {
	if contractDataXDR == "" {
		err := "empty contract data XDR"
		return nil, nil, nil, nil, &err, &err
	}

	var cd xdr.ContractDataEntry
	if err := xdr.SafeUnmarshalBase64(contractDataXDR, &cd); err != nil {
		errStr := fmt.Sprintf("decode contract_data_xdr: %v", err)
		return nil, nil, nil, nil, &errStr, &errStr
	}

	if encoded, err := xdr.MarshalBase64(cd.Key); err == nil {
		keyXDR = &encoded
	} else {
		errStr := fmt.Sprintf("marshal key_xdr: %v", err)
		keyErr = &errStr
	}
	if encoded, err := xdr.MarshalBase64(cd.Val); err == nil {
		valueXDR = &encoded
	} else {
		errStr := fmt.Sprintf("marshal value_xdr: %v", err)
		valueErr = &errStr
	}

	if key, err := scValToDecoded(cd.Key); err != nil {
		errStr := err.Error()
		keyErr = &errStr
	} else {
		keyDecoded = &key
	}

	if value, err := scValToDecoded(cd.Val); err != nil {
		errStr := err.Error()
		valueErr = &errStr
	} else {
		valueDecoded = &value
	}

	return keyXDR, valueXDR, keyDecoded, valueDecoded, keyErr, valueErr
}

func applyDecodedContractDataFields(d *ContractData) {
	if d == nil || d.DataValueXDR == nil || *d.DataValueXDR == "" {
		return
	}
	d.KeyXDR, d.ValueXDR, d.KeyDecoded, d.ValueDecoded, d.KeyDecodedError, d.ValueDecodedError = DecodeContractDataXDRFields(*d.DataValueXDR)
}

func applyDecodedContractStorageFields(e *ContractStorageEntry) {
	if e == nil || e.DataValue == nil || *e.DataValue == "" {
		return
	}
	e.KeyXDR, e.ValueXDR, e.KeyDecoded, e.ValueDecoded, e.KeyDecodedError, e.ValueDecodedError = DecodeContractDataXDRFields(*e.DataValue)
}
