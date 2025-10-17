# Protocol 23 Migration Quick Start Guide

**Status:** Protocol 23 released September 3, 2025 - Project needs update
**Priority:** HIGH

## TL;DR

Your project is using pre-Protocol 23 dependencies. Protocol 23 has been live on mainnet for over a month. You need to update.

**Current versions:**
- `stellar/go`: July-September 2025 commits (pre-release)
- `stellar-rpc`: v0.9.6 from January 2025

**Target versions:**
- `stellar/go`: v23.0.0 (released Aug 14, 2025)
- `stellar-rpc`: v23.0.4 (released Oct 1, 2025)

## Quick Commands

### 1. Update All Dependencies (Automated)
```bash
./scripts/update-protocol-23.sh
```

### 2. Build All Services
```bash
./scripts/build-all.sh
```

### 3. Test All Services
```bash
./scripts/test-all.sh
```

### 4. Manual Update (Single Service)
```bash
cd <service>/go
go get github.com/stellar/go@v23.0.0
go get github.com/stellar/stellar-rpc@v23.0.4
go mod tidy
```

## What Changed in Protocol 23?

### Breaking Changes
- ❌ Removed `getLedgerEntry` RPC endpoint
- ❌ Removed `pagingToken` from `getEvents`
- ✅ New `TransactionMetaV4` and `LedgerCloseMetaV2`
- ✅ Dual BucketList architecture (live + hot archive)
- ✅ Parallel transaction execution support

### Services Affected
All 7 services need updates:
1. ✅ stellar-live-source (has uncommitted Protocol 23 work)
2. ❌ stellar-live-source-datalake
3. ❌ ttp-processor
4. ❌ contract-invocation-processor
5. ❌ contract-data-processor
6. ❌ stellar-arrow-source
7. ❌ cli_tool

## Detailed Documentation

See full implementation plan:
- **Full Plan:** `ai_docs/protocol-23-dependency-update-plan.md`
- **Original Plan:** `ai_docs/protocol-23-upgrade-implementation-plan.md` (datalake)
- **Live Source Plan:** `ai_docs/stellar-live-source-protocol-23-implementation-plan.md`

## Recommended Update Order

1. **stellar-live-source** (foundation service, has partial work)
2. **stellar-live-source-datalake** (alternative data source)
3. **ttp-processor** (depends on stellar-live-source)
4. **contract-invocation-processor** (processor)
5. **contract-data-processor** (processor)
6. **stellar-arrow-source** (arrow integration)
7. **cli_tool** (utility)

## Testing After Update

### Basic Functionality Test
```bash
# Start stellar-live-source
cd stellar-live-source/go
export RPC_ENDPOINT="https://soroban-testnet.stellar.org"
export NETWORK_PASSPHRASE="Test SDF Network ; September 2015"
go run main.go

# In another terminal, check health
curl http://localhost:8088/health
```

### Integration Test
```bash
# Full pipeline test
# Terminal 1: Start data source
cd stellar-live-source/go && make run

# Terminal 2: Start processor
cd ttp-processor/go
export LIVE_SOURCE_ENDPOINT="localhost:50052"
make run

# Terminal 3: Start consumer
cd consumer_app/node
npm start -- 100000 100010
```

## Rollback Plan

If updates cause issues:

```bash
# Revert all changes
git checkout stellar-live-source/go/go.mod
git checkout stellar-live-source/go/go.sum
# ... repeat for other services

# Rebuild
./scripts/build-all.sh
```

## Need Help?

1. Check error logs: `journalctl -u <service-name>`
2. Verify RPC connectivity: `curl -X POST <RPC_ENDPOINT> ...`
3. Review breaking changes: See `ai_docs/protocol-23-dependency-update-plan.md`
4. Check Stellar docs: https://developers.stellar.org/docs/networks/software-versions

## Timeline

Recommended schedule:
- **Week 1:** Update foundation services (stellar-live-source, datalake)
- **Week 2:** Update processors (ttp, contract-invocation, contract-data)
- **Week 3:** Update supporting services (arrow, cli) and test
- **Week 4:** Deploy to production

Total: ~4 weeks for complete migration

## Success Criteria

- [ ] All services build without errors
- [ ] All tests pass
- [ ] Can connect to Protocol 23 RPC endpoints
- [ ] Can process Protocol 23 ledgers
- [ ] No data loss
- [ ] Performance within 10% of baseline

---

**Created:** October 11, 2025
**Last Updated:** October 11, 2025
**Next Review:** After Phase 1 completion
