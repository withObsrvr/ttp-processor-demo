package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"strings"
)

// Cold-only CAP-67 / SEP-41 queries backed by DuckLake silver tables.

func (r *SilverColdReader) GetUnifiedEvents(ctx context.Context, filters UnifiedEventFilters) ([]UnifiedEvent, string, bool, error) {
	whereClause := "WHERE transaction_successful = true"
	args := []interface{}{}
	argNum := 1

	if filters.ContractID != "" {
		whereClause += fmt.Sprintf(" AND token_contract_id = $%d", argNum)
		args = append(args, filters.ContractID)
		argNum++
	}
	if filters.Address != "" {
		whereClause += fmt.Sprintf(" AND (from_account = $%d OR to_account = $%d)", argNum, argNum)
		args = append(args, filters.Address)
		argNum++
	}
	if filters.TxHash != "" {
		whereClause += fmt.Sprintf(" AND transaction_hash = $%d", argNum)
		args = append(args, filters.TxHash)
		argNum++
	}
	if filters.EventType != "" {
		switch filters.EventType {
		case "mint":
			whereClause += " AND from_account IS NULL AND to_account IS NOT NULL"
		case "burn":
			whereClause += " AND to_account IS NULL AND from_account IS NOT NULL"
		case "transfer":
			whereClause += " AND from_account IS NOT NULL AND to_account IS NOT NULL"
		}
	}
	if filters.SourceType != "" {
		whereClause += fmt.Sprintf(" AND source_type = $%d", argNum)
		args = append(args, filters.SourceType)
		argNum++
	}
	if filters.StartLedger > 0 {
		whereClause += fmt.Sprintf(" AND ledger_sequence >= $%d", argNum)
		args = append(args, filters.StartLedger)
		argNum++
	}
	if filters.EndLedger > 0 {
		whereClause += fmt.Sprintf(" AND ledger_sequence <= $%d", argNum)
		args = append(args, filters.EndLedger)
		argNum++
	}

	orderDir := "DESC"
	cursorOp := "<"
	if filters.Order == "asc" {
		orderDir = "ASC"
		cursorOp = ">"
	}

	cursorClause := ""
	if filters.Cursor != nil {
		cursorClause = fmt.Sprintf(" AND (ledger_sequence %s $%d OR (ledger_sequence = $%d AND transaction_hash %s= $%d))",
			cursorOp, argNum, argNum, cursorOp, argNum+1)
		args = append(args, filters.Cursor.LedgerSequence, filters.Cursor.TxHash)
		argNum += 2
	}

	requestLimit := filters.Limit + 1
	if filters.Cursor != nil {
		requestLimit += filters.Cursor.EventIndex + 1
	}

	query := fmt.Sprintf(`
		SELECT
			timestamp,
			transaction_hash,
			ledger_sequence,
			source_type,
			from_account,
			to_account,
			asset_code,
			asset_issuer,
			CAST(amount AS HUGEINT) as amount,
			token_contract_id,
			operation_type
		FROM %s.%s.token_transfers_raw
		%s %s
		ORDER BY ledger_sequence %s, transaction_hash %s
		LIMIT $%d
	`, r.catalogName, r.schemaName, whereClause, cursorClause, orderDir, orderDir, argNum)
	args = append(args, requestLimit)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", false, fmt.Errorf("GetUnifiedEvents: %w", err)
	}
	defer rows.Close()

	var events []UnifiedEvent
	var lastLedger int64
	var lastTxHash string
	txEventCounter := 0
	for rows.Next() {
		var e UnifiedEvent
		var timestamp string
		var amount *big.Int
		if err := rows.Scan(&timestamp, &e.TxHash, &e.LedgerSequence, &e.SourceType,
			&e.From, &e.To, &e.AssetCode, &e.AssetIssuer,
			&amount, &e.ContractID, &e.OperationType); err != nil {
			return nil, "", false, err
		}
		if e.LedgerSequence != lastLedger || e.TxHash != lastTxHash {
			txEventCounter = 0
			lastLedger = e.LedgerSequence
			lastTxHash = e.TxHash
		}
		if filters.Cursor != nil &&
			e.LedgerSequence == filters.Cursor.LedgerSequence &&
			e.TxHash == filters.Cursor.TxHash &&
			txEventCounter <= filters.Cursor.EventIndex {
			txEventCounter++
			continue
		}
		e.ClosedAt = timestamp
		e.EventIndex = txEventCounter
		txEventCounter++
		switch {
		case e.From == nil && e.To != nil:
			e.EventType = "mint"
		case e.To == nil && e.From != nil:
			e.EventType = "burn"
		default:
			e.EventType = "transfer"
		}
		if amount != nil {
			s := amount.String()
			e.Amount = &s
		}
		e.EventID = fmt.Sprintf("%d-%s-%d", e.LedgerSequence, e.TxHash[:8], e.EventIndex)
		events = append(events, e)
	}

	hasMore := len(events) > filters.Limit
	if hasMore {
		events = events[:filters.Limit]
	}

	var nextCursor string
	if len(events) > 0 && hasMore {
		last := events[len(events)-1]
		nextCursor = (UnifiedEventCursor{LedgerSequence: last.LedgerSequence, TxHash: last.TxHash, EventIndex: last.EventIndex, Order: filters.Order}).Encode()
	}

	return events, nextCursor, hasMore, nil
}

func (r *SilverColdReader) GetTransactionEvents(ctx context.Context, txHash string) ([]UnifiedEvent, error) {
	filters := UnifiedEventFilters{TxHash: txHash, Limit: 1000, Order: "asc"}
	events, _, _, err := r.GetUnifiedEvents(ctx, filters)
	if err != nil {
		return nil, err
	}
	r.enrichTransactionEvents(ctx, events)
	return events, nil
}

func (r *SilverColdReader) enrichTransactionEvents(ctx context.Context, events []UnifiedEvent) {
	if r == nil || r.db == nil || len(events) == 0 {
		return
	}

	contractIDs := make([]string, 0)
	seen := make(map[string]struct{})
	for _, e := range events {
		if e.ContractID == nil || *e.ContractID == "" {
			continue
		}
		if _, ok := seen[*e.ContractID]; ok {
			continue
		}
		seen[*e.ContractID] = struct{}{}
		contractIDs = append(contractIDs, *e.ContractID)
	}
	if len(contractIDs) == 0 {
		return
	}

	placeholders := make([]string, len(contractIDs))
	args := make([]any, len(contractIDs))
	for i, id := range contractIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT contract_id, token_name, token_symbol, token_decimals, token_type
		FROM %s.%s.token_registry
		WHERE contract_id IN (%s)
	`, r.catalogName, r.schemaName, strings.Join(placeholders, ","))
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return
	}
	defer rows.Close()

	infoByContract := make(map[string]tokenRegistryInfo, len(contractIDs))
	for rows.Next() {
		var contractID string
		var name, symbol, tokenType sql.NullString
		var decimals sql.NullInt32
		if err := rows.Scan(&contractID, &name, &symbol, &decimals, &tokenType); err != nil {
			continue
		}
		info := tokenRegistryInfo{}
		if name.Valid && name.String != "" {
			s := name.String
			info.Name = &s
		}
		if symbol.Valid && symbol.String != "" {
			s := symbol.String
			info.Symbol = &s
		}
		if decimals.Valid {
			d := int(decimals.Int32)
			info.Decimals = &d
		}
		if tokenType.Valid && tokenType.String != "" {
			s := tokenType.String
			info.TokenType = &s
		}
		info.Issuer = inferIssuerFromTokenInfo(info.Name, info.Symbol, info.TokenType)
		infoByContract[contractID] = info
	}

	for i := range events {
		if events[i].ContractID == nil {
			continue
		}
		info, ok := infoByContract[*events[i].ContractID]
		if !ok {
			continue
		}
		applyTokenRegistryInfoToEvent(&events[i], info)
	}
}

func (r *SilverColdReader) GetAddressEvents(ctx context.Context, addr string, filters UnifiedEventFilters) ([]UnifiedEvent, string, bool, error) {
	filters.Address = addr
	return r.GetUnifiedEvents(ctx, filters)
}

func (r *SilverColdReader) GetSEP41TokenMetadata(ctx context.Context, contractID string) (*SEP41TokenMetadata, error) {
	query := fmt.Sprintf(`
		WITH combined AS (
			SELECT source_type, asset_code, asset_issuer, timestamp, from_account, to_account
			FROM %s.%s.token_transfers_raw
			WHERE token_contract_id = $1 AND transaction_successful = true
		),
		unique_addresses AS (
			SELECT from_account as address FROM combined WHERE from_account IS NOT NULL
			UNION
			SELECT to_account as address FROM combined WHERE to_account IS NOT NULL
		)
		SELECT
			COALESCE(MAX(c.asset_code), '') as asset_code,
			MAX(c.asset_issuer) as asset_issuer,
			COALESCE(MAX(c.source_type), 'soroban') as source_type,
			(SELECT COUNT(*) FROM unique_addresses) as holder_count,
			COUNT(*) as transfer_count,
			MIN(c.timestamp) as first_seen,
			MAX(c.timestamp) as last_activity
		FROM combined c
	`, r.catalogName, r.schemaName)

	var meta SEP41TokenMetadata
	meta.ContractID = contractID
	var assetCode string
	var firstSeen, lastActivity sql.NullString
	if err := r.db.QueryRowContext(ctx, query, contractID).Scan(&assetCode, &meta.AssetIssuer, &meta.SourceType, &meta.HolderCount, &meta.TransferCount, &firstSeen, &lastActivity); err != nil {
		return nil, fmt.Errorf("GetSEP41TokenMetadata: %w", err)
	}
	if firstSeen.Valid {
		meta.FirstSeen = firstSeen.String
	}
	if lastActivity.Valid {
		meta.LastActivity = lastActivity.String
	}
	if assetCode != "" {
		meta.AssetCode = &assetCode
	}

	var regName, regSymbol, regTokenType sql.NullString
	var regDecimals sql.NullInt32
	regQuery := fmt.Sprintf(`SELECT token_name, token_symbol, token_decimals, token_type FROM %s.%s.token_registry WHERE contract_id = $1 LIMIT 1`, r.catalogName, r.schemaName)
	if err := r.db.QueryRowContext(ctx, regQuery, contractID).Scan(&regName, &regSymbol, &regDecimals, &regTokenType); err == nil {
		if regName.Valid && regName.String != "" {
			meta.Name = &regName.String
		}
		if regSymbol.Valid && regSymbol.String != "" {
			meta.Symbol = &regSymbol.String
		}
		if regDecimals.Valid {
			meta.Decimals = int(regDecimals.Int32)
		} else {
			meta.Decimals = 7
		}
		if regTokenType.Valid {
			meta.TokenType = regTokenType.String
		}
	} else {
		meta.Decimals = 7
		if meta.AssetCode != nil {
			meta.TokenType = "sac"
		} else {
			meta.TokenType = "custom_soroban"
		}
	}

	return &meta, nil
}

func (r *SilverColdReader) GetSEP41Balances(ctx context.Context, filters SEP41BalanceFilters) ([]SEP41Balance, string, bool, error) {
	requestLimit := filters.Limit + 1
	cursorClause := ""
	args := []interface{}{filters.ContractID}
	argNum := 2

	if filters.Cursor != nil {
		cursorClause = fmt.Sprintf(" HAVING net_balance < $%d OR (net_balance = $%d AND address > $%d)", argNum, argNum, argNum+1)
		args = append(args, filters.Cursor.Balance, filters.Cursor.Balance, filters.Cursor.Address)
		argNum += 2
	}
	if filters.MinBalance > 0 && cursorClause == "" {
		cursorClause = fmt.Sprintf(" HAVING net_balance >= $%d", argNum)
		args = append(args, filters.MinBalance)
		argNum++
	} else if filters.MinBalance > 0 {
		cursorClause += fmt.Sprintf(" AND net_balance >= $%d", argNum)
		args = append(args, filters.MinBalance)
		argNum++
	}

	query := fmt.Sprintf(`
		WITH transfers AS (
			SELECT from_account, to_account, amount, ledger_sequence, timestamp
			FROM %s.%s.token_transfers_raw
			WHERE token_contract_id = $1 AND transaction_successful = true
		),
		balances AS (
			SELECT address, SUM(received) as total_received, SUM(sent) as total_sent,
			       SUM(received) - SUM(sent) as net_balance,
			       COUNT(*) as tx_count, MAX(ledger_sequence) as last_ledger, MAX(timestamp) as last_seen
			FROM (
				SELECT to_account as address, amount as received, 0 as sent, ledger_sequence, timestamp
				FROM transfers WHERE to_account IS NOT NULL
				UNION ALL
				SELECT from_account as address, 0 as received, amount as sent, ledger_sequence, timestamp
				FROM transfers WHERE from_account IS NOT NULL
			) addr_amounts
			GROUP BY address
			%s
		)
		SELECT address, CAST(net_balance AS BIGINT), CAST(total_received AS BIGINT), CAST(total_sent AS BIGINT), tx_count, last_ledger, last_seen
		FROM balances
		WHERE net_balance > 0
		ORDER BY net_balance DESC, address ASC
		LIMIT $%d
	`, r.catalogName, r.schemaName, cursorClause, argNum)
	args = append(args, requestLimit)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", false, fmt.Errorf("GetSEP41Balances: %w", err)
	}
	defer rows.Close()

	var balances []SEP41Balance
	for rows.Next() {
		var b SEP41Balance
		if err := rows.Scan(&b.Address, &b.BalanceRaw, &b.Received, &b.Sent, &b.TxCount, &b.LastLedger, &b.LastSeen); err != nil {
			return nil, "", false, err
		}
		b.Balance = formatStroopsLocal(b.BalanceRaw)
		balances = append(balances, b)
	}

	hasMore := len(balances) > filters.Limit
	if hasMore {
		balances = balances[:filters.Limit]
	}

	var nextCursor string
	if len(balances) > 0 && hasMore {
		last := balances[len(balances)-1]
		nextCursor = (SEP41BalanceCursor{Balance: last.BalanceRaw, Address: last.Address}).Encode()
	}

	return balances, nextCursor, hasMore, nil
}

func (r *SilverColdReader) GetSEP41SingleBalance(ctx context.Context, contractID, address string) (*SEP41Balance, error) {
	query := fmt.Sprintf(`
		WITH transfers AS (
			SELECT from_account, to_account, amount, ledger_sequence, timestamp
			FROM %s.%s.token_transfers_raw
			WHERE token_contract_id = $1 AND transaction_successful = true
			  AND (from_account = $2 OR to_account = $2)
		)
		SELECT CAST(COALESCE(SUM(CASE WHEN to_account = $2 THEN amount ELSE 0 END), 0) AS BIGINT) as total_received,
		       CAST(COALESCE(SUM(CASE WHEN from_account = $2 THEN amount ELSE 0 END), 0) AS BIGINT) as total_sent,
		       COUNT(*) as tx_count,
		       COALESCE(MAX(ledger_sequence), 0) as last_ledger,
		       COALESCE(MAX(timestamp), '1970-01-01'::timestamp) as last_seen
		FROM transfers
	`, r.catalogName, r.schemaName)

	var b SEP41Balance
	b.Address = address
	if err := r.db.QueryRowContext(ctx, query, contractID, address).Scan(&b.Received, &b.Sent, &b.TxCount, &b.LastLedger, &b.LastSeen); err != nil {
		return nil, fmt.Errorf("GetSEP41SingleBalance: %w", err)
	}
	b.BalanceRaw = b.Received - b.Sent
	b.Balance = formatStroopsLocal(b.BalanceRaw)
	return &b, nil
}

func (r *SilverColdReader) GetSEP41Transfers(ctx context.Context, contractID string, filters UnifiedEventFilters) ([]UnifiedEvent, string, bool, error) {
	filters.ContractID = contractID
	return r.GetUnifiedEvents(ctx, filters)
}

func (r *SilverColdReader) GetSEP41TokenStats(ctx context.Context, contractID string) (*SEP41TokenStats, error) {
	query := fmt.Sprintf(`
		WITH transfers AS (
			SELECT from_account, to_account, amount, timestamp, asset_code
			FROM %s.%s.token_transfers_raw
			WHERE token_contract_id = $1 AND transaction_successful = true
		),
		holders AS (
			SELECT address, SUM(received) - SUM(sent) as net_balance
			FROM (
				SELECT to_account as address, amount as received, 0 as sent FROM transfers WHERE to_account IS NOT NULL
				UNION ALL
				SELECT from_account as address, 0 as received, amount as sent FROM transfers WHERE from_account IS NOT NULL
			) addr_amounts
			GROUP BY address
			HAVING SUM(received) - SUM(sent) > 0
		)
		SELECT (SELECT COUNT(*) FROM holders) as holder_count,
		       CAST(COALESCE((SELECT SUM(net_balance) FROM holders), 0) AS BIGINT) as total_supply,
		       (SELECT COUNT(*) FROM transfers WHERE timestamp > NOW() - INTERVAL '24 hours') as transfers_24h,
		       CAST(COALESCE((SELECT SUM(amount) FROM transfers WHERE timestamp > NOW() - INTERVAL '24 hours'), 0) AS BIGINT) as volume_24h,
		       (SELECT MAX(asset_code) FROM transfers) as asset_code
		FROM (SELECT 1) dummy
	`, r.catalogName, r.schemaName)

	var stats SEP41TokenStats
	stats.ContractID = contractID
	if err := r.db.QueryRowContext(ctx, query, contractID).Scan(&stats.HolderCount, &stats.TotalSupplyRaw, &stats.Transfers24h, &stats.Volume24hRaw, &stats.AssetCode); err != nil {
		return nil, fmt.Errorf("GetSEP41TokenStats: %w", err)
	}
	stats.TotalSupply = formatStroopsLocal(stats.TotalSupplyRaw)
	stats.Volume24h = formatStroopsLocal(stats.Volume24hRaw)

	var regName, regSymbol, regTokenType sql.NullString
	var regDecimals sql.NullInt32
	regQuery := fmt.Sprintf(`SELECT token_name, token_symbol, token_decimals, token_type FROM %s.%s.token_registry WHERE contract_id = $1 LIMIT 1`, r.catalogName, r.schemaName)
	if err := r.db.QueryRowContext(ctx, regQuery, contractID).Scan(&regName, &regSymbol, &regDecimals, &regTokenType); err == nil {
		if regName.Valid && regName.String != "" {
			stats.Name = &regName.String
		}
		if regSymbol.Valid && regSymbol.String != "" {
			stats.Symbol = &regSymbol.String
		}
		if regDecimals.Valid {
			stats.Decimals = int(regDecimals.Int32)
		} else {
			stats.Decimals = 7
		}
		if regTokenType.Valid {
			stats.TokenType = regTokenType.String
		}
	} else {
		stats.Decimals = 7
		if stats.AssetCode != nil {
			stats.TokenType = "sac"
		} else {
			stats.TokenType = "custom_soroban"
		}
	}

	return &stats, nil
}

func (r *SilverColdReader) GetAddressTokenPortfolio(ctx context.Context, address string) ([]TokenHolding, error) {
	query := fmt.Sprintf(`
		WITH transfers AS (
			SELECT from_account, to_account, amount, token_contract_id, asset_code, asset_issuer, source_type, timestamp
			FROM %s.%s.token_transfers_raw
			WHERE (from_account = $1 OR to_account = $1) AND transaction_successful = true
		)
		SELECT token_contract_id,
		       MAX(asset_code) as asset_code,
		       MAX(asset_issuer) as asset_issuer,
		       MAX(source_type) as source_type,
		       CAST(SUM(CASE WHEN to_account = $1 THEN amount ELSE 0 END) -
		       SUM(CASE WHEN from_account = $1 THEN amount ELSE 0 END) AS BIGINT) as net_balance,
		       COUNT(*) as tx_count,
		       MAX(timestamp) as last_seen
		FROM transfers
		GROUP BY token_contract_id
		HAVING SUM(CASE WHEN to_account = $1 THEN amount ELSE 0 END) -
		       SUM(CASE WHEN from_account = $1 THEN amount ELSE 0 END) > 0
		ORDER BY net_balance DESC
		LIMIT 100
	`, r.catalogName, r.schemaName)

	rows, err := r.db.QueryContext(ctx, query, address)
	if err != nil {
		return nil, fmt.Errorf("GetAddressTokenPortfolio: %w", err)
	}
	defer rows.Close()

	var holdings []TokenHolding
	regQuery := fmt.Sprintf(`SELECT token_name, token_symbol, token_decimals, token_type FROM %s.%s.token_registry WHERE contract_id = $1 LIMIT 1`, r.catalogName, r.schemaName)
	for rows.Next() {
		var h TokenHolding
		if err := rows.Scan(&h.ContractID, &h.AssetCode, &h.AssetIssuer, &h.SourceType, &h.BalanceRaw, &h.TxCount, &h.LastSeen); err != nil {
			return nil, err
		}
		h.Balance = formatStroopsLocal(h.BalanceRaw)
		h.Decimals = 7
		if h.AssetCode != nil && *h.AssetCode != "" {
			h.TokenType = "sac"
		} else {
			h.TokenType = "custom_soroban"
		}
		if h.ContractID != nil {
			var regName, regSymbol, regTokenType sql.NullString
			var regDecimals sql.NullInt32
			if err := r.db.QueryRowContext(ctx, regQuery, *h.ContractID).Scan(&regName, &regSymbol, &regDecimals, &regTokenType); err == nil {
				if regName.Valid && regName.String != "" {
					h.Name = &regName.String
				}
				if regSymbol.Valid && regSymbol.String != "" {
					h.Symbol = &regSymbol.String
				}
				if regDecimals.Valid {
					h.Decimals = int(regDecimals.Int32)
				}
				if regTokenType.Valid {
					h.TokenType = regTokenType.String
				}
			}
		}
		holdings = append(holdings, h)
	}
	return holdings, nil
}
