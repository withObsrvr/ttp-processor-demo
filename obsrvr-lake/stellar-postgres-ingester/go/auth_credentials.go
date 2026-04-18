package main

import (
	"encoding/hex"
	"fmt"
	"io"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
	extract "github.com/withObsrvr/stellar-extract"
)

// authCredsEntry holds the parallel credential-type / authorizer-address
// arrays for a single InvokeHostFunction operation that has at least one
// SorobanAuthorizationEntry.
type authCredsEntry struct {
	types []string
	addrs []string
}

// buildAuthCredentialsMap walks the decoded ledger and returns a map keyed
// by "<tx_hash>:<op_index>" of Soroban auth credentials per InvokeHostFunction
// op. Only ops with at least one auth entry are included — ops missing from
// the map should leave their SorobanAuth* fields nil (interpreted as SQL NULL).
//
// The external stellar-extract library does not surface credentials at the
// time of writing, so we re-walk the LCM here via the SDK's transaction
// reader. This is one extra pass over the already-decoded meta and so adds
// negligible cost compared to extraction itself.
func buildAuthCredentialsMap(input *extract.LedgerInput) (map[string]authCredsEntry, error) {
	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(input.NetworkPassphrase, input.LCM)
	if err != nil {
		return nil, fmt.Errorf("new tx reader: %w", err)
	}
	defer reader.Close()

	out := make(map[string]authCredsEntry)
	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return out, fmt.Errorf("tx read: %w", err)
		}

		txHash := hex.EncodeToString(tx.Result.TransactionHash[:])
		for opIdx, op := range tx.Envelope.Operations() {
			if op.Body.Type != xdr.OperationTypeInvokeHostFunction {
				continue
			}
			invokeOp, ok := op.Body.GetInvokeHostFunctionOp()
			if !ok {
				continue
			}
			if len(invokeOp.Auth) == 0 {
				continue
			}
			types, addrs := extractAuthCredentials(invokeOp.Auth)
			if len(types) == 0 {
				continue
			}
			out[authKey(txHash, opIdx)] = authCredsEntry{types: types, addrs: addrs}
		}
	}
	return out, nil
}

// authKey is the key scheme used by buildAuthCredentialsMap. Shared so the
// consumer can look up a row using the same format.
func authKey(txHash string, opIndex int) string {
	return fmt.Sprintf("%s:%d", txHash, opIndex)
}

// applyAuthCredentials copies credentials from the map into each matching
// OperationData. Ops without an entry are left untouched (nil arrays → SQL
// NULL in the insert path).
func applyAuthCredentials(ops []OperationData, creds map[string]authCredsEntry) {
	if len(creds) == 0 {
		return
	}
	for i := range ops {
		op := &ops[i]
		if e, ok := creds[authKey(op.TransactionHash, op.OperationIndex)]; ok {
			op.SorobanAuthCredentialsTypes = e.types
			op.SorobanAuthAddresses = e.addrs
		}
	}
}
