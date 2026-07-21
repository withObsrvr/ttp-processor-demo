package main

import (
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestCountEnvelopeOperationsIncludesAllDeclaredFailedOperations(t *testing.T) {
	envelopes := []xdr.TransactionEnvelope{
		transactionEnvelopeWithOperationCount(29),
		transactionEnvelopeWithOperationCount(8),
	}
	if got := countEnvelopeOperations(envelopes); got != 37 {
		t.Fatalf("countEnvelopeOperations() = %d, want 37", got)
	}
}

func transactionEnvelopeWithOperationCount(count int) xdr.TransactionEnvelope {
	return xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1: &xdr.TransactionV1Envelope{
			Tx: xdr.Transaction{Operations: make([]xdr.Operation, count)},
		},
	}
}
