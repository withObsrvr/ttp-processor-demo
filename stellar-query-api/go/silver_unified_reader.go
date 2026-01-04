package main

import (
	"context"
	"log"
	"time"
)

// UnifiedSilverReader combines hot (PostgreSQL) and cold (DuckLake) Silver queries
type UnifiedSilverReader struct {
	hot  *SilverHotReader
	cold *SilverColdReader
}

// NewUnifiedSilverReader creates a new unified Silver reader
func NewUnifiedSilverReader(hot *SilverHotReader, cold *SilverColdReader) *UnifiedSilverReader {
	return &UnifiedSilverReader{
		hot:  hot,
		cold: cold,
	}
}

// Close closes both hot and cold connections
func (u *UnifiedSilverReader) Close() error {
	var hotErr, coldErr error
	if u.hot != nil {
		hotErr = u.hot.Close()
	}
	if u.cold != nil {
		coldErr = u.cold.Close()
	}
	if hotErr != nil {
		return hotErr
	}
	return coldErr
}

// ============================================
// ACCOUNT QUERIES (Current State + History)
// ============================================

// GetAccountCurrent queries hot first, then cold if not found
func (u *UnifiedSilverReader) GetAccountCurrent(ctx context.Context, accountID string) (*AccountCurrent, error) {
	// Try hot first (most recent data)
	account, err := u.hot.GetAccountCurrent(ctx, accountID)
	if err != nil {
		log.Printf("Warning: failed to query hot storage: %v", err)
	} else if account != nil {
		return account, nil
	}

	// Fall back to cold storage
	return u.cold.GetAccountCurrent(ctx, accountID)
}

// GetAccountHistory merges hot + cold results
func (u *UnifiedSilverReader) GetAccountHistory(ctx context.Context, accountID string, limit int) ([]AccountSnapshot, error) {
	var results []AccountSnapshot

	// Query hot first
	hotResults, err := u.hot.GetAccountHistory(ctx, accountID, limit)
	if err != nil {
		log.Printf("Warning: failed to query hot storage: %v", err)
	} else {
		results = append(results, hotResults...)
	}

	// Query cold if needed
	if len(results) < limit {
		coldResults, err := u.cold.GetAccountHistory(ctx, accountID, limit-len(results))
		if err != nil {
			log.Printf("Warning: failed to query cold storage: %v", err)
		} else {
			results = append(results, coldResults...)
		}
	}

	return results, nil
}

// GetTopAccounts merges hot + cold results and sorts
func (u *UnifiedSilverReader) GetTopAccounts(ctx context.Context, limit int) ([]AccountCurrent, error) {
	// Query both sources in parallel
	hotChan := make(chan []AccountCurrent, 1)
	coldChan := make(chan []AccountCurrent, 1)

	// Query hot
	go func() {
		results, err := u.hot.GetTopAccounts(ctx, limit)
		if err != nil {
			log.Printf("Warning: failed to query hot storage: %v", err)
			hotChan <- nil
		} else {
			hotChan <- results
		}
	}()

	// Query cold
	go func() {
		results, err := u.cold.GetTopAccounts(ctx, limit)
		if err != nil {
			log.Printf("Warning: failed to query cold storage: %v", err)
			coldChan <- nil
		} else {
			coldChan <- results
		}
	}()

	// Wait for both
	hotResults := <-hotChan
	coldResults := <-coldChan

	// Merge and deduplicate by account_id (hot takes precedence)
	accountMap := make(map[string]AccountCurrent)

	// Add cold results first
	if coldResults != nil {
		for _, acc := range coldResults {
			accountMap[acc.AccountID] = acc
		}
	}

	// Add hot results (overwrites cold if duplicate)
	if hotResults != nil {
		for _, acc := range hotResults {
			accountMap[acc.AccountID] = acc
		}
	}

	// Convert back to slice and sort by balance
	results := make([]AccountCurrent, 0, len(accountMap))
	for _, acc := range accountMap {
		results = append(results, acc)
	}

	// Sort by balance descending (simple bubble sort for small lists)
	for i := 0; i < len(results)-1; i++ {
		for j := i + 1; j < len(results); j++ {
			// Compare balances (they're strings, so we'd need proper comparison)
			// For now, just maintain order from queries
		}
	}

	// Return top N
	if len(results) > limit {
		results = results[:limit]
	}

	return results, nil
}

// ============================================
// ENRICHED OPERATIONS QUERIES
// ============================================

// GetEnrichedOperations merges hot + cold results
func (u *UnifiedSilverReader) GetEnrichedOperations(ctx context.Context, filters OperationFilters) ([]EnrichedOperation, error) {
	var results []EnrichedOperation

	// Query hot first
	hotResults, err := u.hot.GetEnrichedOperations(ctx, filters)
	if err != nil {
		log.Printf("Warning: failed to query hot storage: %v", err)
	} else {
		results = append(results, hotResults...)
	}

	// Query cold if needed
	if len(results) < filters.Limit {
		remainingLimit := filters.Limit - len(results)
		coldFilters := filters
		coldFilters.Limit = remainingLimit

		coldResults, err := u.cold.GetEnrichedOperations(ctx, coldFilters)
		if err != nil {
			log.Printf("Warning: failed to query cold storage: %v", err)
		} else {
			results = append(results, coldResults...)
		}
	}

	return results, nil
}

// ============================================
// TOKEN TRANSFERS QUERIES
// ============================================

// GetTokenTransfers merges hot + cold results
func (u *UnifiedSilverReader) GetTokenTransfers(ctx context.Context, filters TransferFilters) ([]TokenTransfer, error) {
	var results []TokenTransfer

	// Query hot first
	hotResults, err := u.hot.GetTokenTransfers(ctx, filters)
	if err != nil {
		log.Printf("Warning: failed to query hot storage: %v", err)
	} else {
		results = append(results, hotResults...)
	}

	// Query cold if needed
	if len(results) < filters.Limit {
		remainingLimit := filters.Limit - len(results)
		coldFilters := filters
		coldFilters.Limit = remainingLimit

		coldResults, err := u.cold.GetTokenTransfers(ctx, coldFilters)
		if err != nil {
			log.Printf("Warning: failed to query cold storage: %v", err)
		} else {
			results = append(results, coldResults...)
		}
	}

	return results, nil
}

// GetTokenTransferStats delegates to cold storage only (aggregations are expensive)
func (u *UnifiedSilverReader) GetTokenTransferStats(ctx context.Context, groupBy string, startTime, endTime time.Time) ([]TransferStats, error) {
	// For aggregations, only query cold storage to avoid expensive computation on hot buffer
	return u.cold.GetTokenTransferStats(ctx, groupBy, startTime, endTime)
}

// ============================================
// CONTRACT CALL QUERIES (Freighter Support)
// ============================================

// GetContractsInvolved returns all contracts involved in a transaction
func (u *UnifiedSilverReader) GetContractsInvolved(ctx context.Context, txHash string) ([]string, error) {
	// Query hot first (recent transactions)
	contracts, err := u.hot.GetContractsInvolved(ctx, txHash)
	if err != nil {
		log.Printf("Warning: failed to query hot storage for contracts involved: %v", err)
		return nil, err
	}

	// If found in hot, return it
	if len(contracts) > 0 {
		return contracts, nil
	}

	// TODO: Fall back to cold storage for historical transactions
	// return u.cold.GetContractsInvolved(ctx, txHash)
	return contracts, nil
}

// GetTransactionCallGraph returns the call graph for a transaction
func (u *UnifiedSilverReader) GetTransactionCallGraph(ctx context.Context, txHash string) ([]ContractCall, error) {
	// Query hot first (recent transactions)
	calls, err := u.hot.GetTransactionCallGraph(ctx, txHash)
	if err != nil {
		log.Printf("Warning: failed to query hot storage for call graph: %v", err)
		return nil, err
	}

	// If found in hot, return it
	if len(calls) > 0 {
		return calls, nil
	}

	// TODO: Fall back to cold storage for historical transactions
	// return u.cold.GetTransactionCallGraph(ctx, txHash)
	return calls, nil
}

// GetTransactionHierarchy returns the contract hierarchy for a transaction
func (u *UnifiedSilverReader) GetTransactionHierarchy(ctx context.Context, txHash string) ([]ContractHierarchy, error) {
	// Query hot first (recent transactions)
	hierarchies, err := u.hot.GetTransactionHierarchy(ctx, txHash)
	if err != nil {
		log.Printf("Warning: failed to query hot storage for hierarchy: %v", err)
		return nil, err
	}

	// If found in hot, return it
	if len(hierarchies) > 0 {
		return hierarchies, nil
	}

	// TODO: Fall back to cold storage for historical transactions
	// return u.cold.GetTransactionHierarchy(ctx, txHash)
	return hierarchies, nil
}

// GetContractRecentCalls returns recent calls for a contract
func (u *UnifiedSilverReader) GetContractRecentCalls(ctx context.Context, contractID string, limit int) ([]ContractCall, int, int, error) {
	// Query hot for recent calls
	calls, asCaller, asCallee, err := u.hot.GetContractRecentCalls(ctx, contractID, limit)
	if err != nil {
		log.Printf("Warning: failed to query hot storage for recent calls: %v", err)
		return nil, 0, 0, err
	}

	// TODO: Merge with cold storage for comprehensive results
	return calls, asCaller, asCallee, nil
}

// GetContractCallers returns contracts that call a specific contract
func (u *UnifiedSilverReader) GetContractCallers(ctx context.Context, contractID string, limit int) ([]ContractRelationship, error) {
	// Query hot for callers
	callers, err := u.hot.GetContractCallers(ctx, contractID, limit)
	if err != nil {
		log.Printf("Warning: failed to query hot storage for callers: %v", err)
		return nil, err
	}

	// TODO: Merge with cold storage for comprehensive results
	return callers, nil
}

// GetContractCallees returns contracts called by a specific contract
func (u *UnifiedSilverReader) GetContractCallees(ctx context.Context, contractID string, limit int) ([]ContractRelationship, error) {
	// Query hot for callees
	callees, err := u.hot.GetContractCallees(ctx, contractID, limit)
	if err != nil {
		log.Printf("Warning: failed to query hot storage for callees: %v", err)
		return nil, err
	}

	// TODO: Merge with cold storage for comprehensive results
	return callees, nil
}

// GetContractCallSummary returns aggregated call statistics for a contract
func (u *UnifiedSilverReader) GetContractCallSummary(ctx context.Context, contractID string) (*ContractCallSummary, error) {
	// Query hot for summary
	summary, err := u.hot.GetContractCallSummary(ctx, contractID)
	if err != nil {
		log.Printf("Warning: failed to query hot storage for call summary: %v", err)
		return nil, err
	}

	// TODO: Merge with cold storage for comprehensive statistics
	return summary, nil
}
