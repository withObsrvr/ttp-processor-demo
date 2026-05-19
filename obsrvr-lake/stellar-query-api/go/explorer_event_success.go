package main

import "database/sql"

func boolPtrFromNullBool(v sql.NullBool) *bool {
	if !v.Valid {
		return nil
	}
	b := v.Bool
	return &b
}

func applyExplorerEventSuccess(e *ExplorerEvent, transactionSuccessful sql.NullBool, inSuccessfulContractCall sql.NullBool) {
	e.TransactionSuccessful = boolPtrFromNullBool(transactionSuccessful)
	e.InSuccessfulContractCall = boolPtrFromNullBool(inSuccessfulContractCall)
	// `successful` is retained only as a compatibility alias for transaction_successful.
	// Never derive it from in_successful_contract_call.
	e.Successful = transactionSuccessful.Valid && transactionSuccessful.Bool
}
