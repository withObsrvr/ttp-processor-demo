package main

import "database/sql"

func applyGenericEventSuccess(e *GenericEvent, transactionSuccessful sql.NullBool) {
	e.TransactionSuccessful = boolPtrFromNullBool(transactionSuccessful)
}
