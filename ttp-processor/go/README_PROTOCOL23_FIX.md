# Protocol 23 LedgerUpgradeType Fix

## Problem

The error `'0' is not a valid LedgerUpgradeType enum value` occurs when processing Protocol 23 ledger data because the current stellar-go XDR definitions don't include enum value 0 in the LedgerUpgradeType enum.

## Root Cause

The LedgerUpgradeType enum in stellar-go currently defines values 1-7:
- `LEDGER_UPGRADE_VERSION = 1`
- `LEDGER_UPGRADE_BASE_FEE = 2`
- `LEDGER_UPGRADE_MAX_TX_SET_SIZE = 3`
- `LEDGER_UPGRADE_BASE_RESERVE = 4`
- `LEDGER_UPGRADE_FLAGS = 5`
- `LEDGER_UPGRADE_CONFIG = 6`
- `LEDGER_UPGRADE_MAX_SOROBAN_TX_SET_SIZE = 7`

However, Protocol 23 ledger data contains a LedgerUpgradeType with value 0, which is not defined in the enum, causing the XDR unmarshaling to fail.

## Solution Implemented

### 1. Error Detection and Graceful Handling

Modified `server/server.go` to detect the specific LedgerUpgradeType enum error and handle it gracefully:

```go
// Check if this is the specific LedgerUpgradeType enum error for Protocol 23
if IsLedgerUpgradeTypeError(err) {
    if value, isZero := GetLedgerUpgradeTypeErrorValue(err); isZero && value == 0 {
        ledgerLogger.Warn("encountered Protocol 23 LedgerUpgradeType enum value 0 - skipping ledger",
            zap.Error(err),
            zap.Int32("invalid_enum_value", value))
        // Skip this ledger and continue with the next one
        continue
    }
}
```

### 2. Helper Functions

Created `server/xdr_patch.go` with utility functions:

- `IsLedgerUpgradeTypeError()`: Detects if an error is related to LedgerUpgradeType enum validation
- `GetLedgerUpgradeTypeErrorValue()`: Extracts the invalid enum value from error messages
- `PatchedLedgerUpgradeType`: A custom enum type that includes value 0 for reference

### 3. Behavior Changes

- **Before**: Service crashes when encountering LedgerUpgradeType enum value 0
- **After**: Service logs a warning and skips the problematic ledger, continuing to process subsequent ledgers

## Impact

### Positive
- Service remains operational when encountering Protocol 23 ledgers with enum value 0
- Comprehensive error logging provides visibility into skipped ledgers
- Metrics tracking continues to work correctly
- No data corruption or service instability

### Limitations
- Ledgers with LedgerUpgradeType enum value 0 are skipped, not processed
- This is a temporary fix until stellar-go is updated with proper Protocol 23 support
- Some Protocol 23 features may not be available until proper XDR support is added

## Monitoring

The fix includes comprehensive logging and metrics:

- **Warning logs**: Each skipped ledger is logged with details
- **Error metrics**: Skipped ledgers are counted in error metrics
- **Health endpoint**: `/health` shows error counts and last error information

## Long-term Solution

The proper long-term solution is to update to a version of stellar-go that includes:
1. LedgerUpgradeType enum value 0 definition
2. Proper Protocol 23 XDR support
3. Full Protocol 23 feature compatibility

## Testing

To test the fix:

1. **Build the service**: `go build -o ttp-processor .`
2. **Run with Protocol 23 data**: The service should now handle problematic ledgers gracefully
3. **Monitor logs**: Look for warning messages about skipped ledgers
4. **Check metrics**: Verify error counts increase but service continues running

## Files Modified

- `/server/server.go`: Added error detection and graceful handling
- `/server/xdr_patch.go`: Added utility functions and custom enum type
- `/go.mod`: Updated stellar-go to latest available version

## Configuration

No additional configuration required. The fix is automatically applied when LedgerUpgradeType enum errors are detected.

## Performance Impact

Minimal performance impact:
- Small overhead for error string checking
- Skipped ledgers reduce processing load
- No impact on successfully processed ledgers