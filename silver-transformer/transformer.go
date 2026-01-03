package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

// SilverTransformer handles bronze → silver transformations
type SilverTransformer struct {
	config            Config
	bronzeClient      *BronzeQueryClient
	silverDB          *sql.DB
	checkpointManager *CheckpointManager
	stopChan          chan struct{}

	// Metrics
	totalLedgersProcessed       int64
	totalTransactionsProcessed  int64
	totalOperationsProcessed    int64
	lastProcessedSequence       int64
}

// NewSilverTransformer creates a new transformer instance
func NewSilverTransformer(config Config) (*SilverTransformer, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// Create Query API client for bronze access
	bronzeClient := NewBronzeQueryClient(config.BronzeQueryURL)

	// Test connection to bronze Query API
	if err := bronzeClient.HealthCheck(); err != nil {
		return nil, fmt.Errorf("failed to connect to bronze Query API: %w", err)
	}

	// Open silver catalog (read-write)
	silverDB, err := sql.Open("duckdb", config.SilverPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open silver catalog: %w", err)
	}

	// Silver has single writer
	silverDB.SetMaxOpenConns(1)
	silverDB.SetMaxIdleConns(1)

	return &SilverTransformer{
		config:            config,
		bronzeClient:      bronzeClient,
		silverDB:          silverDB,
		checkpointManager: NewCheckpointManager(silverDB, "silver-transformer"),
		stopChan:          make(chan struct{}),
	}, nil
}

// Start begins the transformation process
func (st *SilverTransformer) Start() error {
	log.Println("🚀 Starting Silver Transformer")
	log.Printf("Bronze Query API: %s", st.config.BronzeQueryURL)
	log.Printf("Silver: %s", st.config.SilverPath)
	log.Printf("Schema: %s", st.config.BronzeSchema)
	log.Printf("Poll Interval: %v", st.config.PollInterval)

	// Initialize silver schema
	if err := InitSilverSchema(st.silverDB); err != nil {
		return fmt.Errorf("failed to initialize silver schema: %w", err)
	}

	// Initialize checkpoint table
	if err := st.checkpointManager.InitCheckpointTable(); err != nil {
		return fmt.Errorf("failed to initialize checkpoint: %w", err)
	}

	// Load checkpoint
	checkpoint, err := st.checkpointManager.Load()
	if err != nil {
		return fmt.Errorf("failed to load checkpoint: %w", err)
	}

	if checkpoint == 0 {
		log.Println("⚠️  No checkpoint found, starting from beginning")
	} else {
		log.Printf("📍 Resuming from sequence: %d", checkpoint)
	}

	st.lastProcessedSequence = checkpoint

	// Start polling loop
	ticker := time.NewTicker(st.config.PollInterval)
	defer ticker.Stop()

	log.Println("✅ Silver Transformer ready - polling for new data...")

	for {
		select {
		case <-ticker.C:
			if err := st.processNewLedgers(); err != nil {
				log.Printf("❌ Error processing ledgers: %v", err)
			}
		case <-st.stopChan:
			log.Println("🛑 Stopping Silver Transformer...")
			return nil
		}
	}
}

// Stop gracefully stops the transformer
func (st *SilverTransformer) Stop() {
	close(st.stopChan)
	if st.silverDB != nil {
		st.silverDB.Close()
	}
}

// processNewLedgers polls bronze for new operations/transactions/ledgers and transforms them
func (st *SilverTransformer) processNewLedgers() error {
	// Query latest ledgers to check for new data
	ledgers, err := st.bronzeClient.QueryLedgers(
		st.config.BronzeSchema,
		st.lastProcessedSequence,
		st.config.BatchSize,
	)
	if err != nil {
		return fmt.Errorf("failed to query bronze: %w", err)
	}

	if len(ledgers) == 0 {
		// No new data
		return nil
	}

	// Track latest sequence
	var maxSequence int64
	for _, ledger := range ledgers {
		if ledger.Sequence > maxSequence {
			maxSequence = ledger.Sequence
		}
		st.totalLedgersProcessed++
	}

	log.Printf("📊 Processing %d new ledgers (up to sequence %d)", len(ledgers), maxSequence)

	// Populate accounts_current table with latest bronze data
	if err := st.populateAccountsCurrent(); err != nil {
		return fmt.Errorf("failed to populate accounts_current: %w", err)
	}

	log.Printf("✅ Populated accounts_current table")

	// Populate account_signers_current table with latest bronze data
	if err := st.populateAccountSignersCurrent(); err != nil {
		return fmt.Errorf("failed to populate account_signers_current: %w", err)
	}

	log.Printf("✅ Populated account_signers_current table")

	// Populate trustlines_current table with latest bronze data
	if err := st.populateTrustlinesCurrent(); err != nil {
		return fmt.Errorf("failed to populate trustlines_current: %w", err)
	}

	log.Printf("✅ Populated trustlines_current table")

	// Populate offers_current table with latest bronze data
	if err := st.populateOffersCurrent(); err != nil {
		return fmt.Errorf("failed to populate offers_current: %w", err)
	}

	log.Printf("✅ Populated offers_current table")

	// Populate claimable_balances_current table with latest bronze data
	if err := st.populateClaimableBalancesCurrent(); err != nil {
		return fmt.Errorf("failed to populate claimable_balances_current: %w", err)
	}

	log.Printf("✅ Populated claimable_balances_current table")

	// Populate liquidity_pools_current table with latest bronze data
	if err := st.populateLiquidityPoolsCurrent(); err != nil {
		return fmt.Errorf("failed to populate liquidity_pools_current: %w", err)
	}

	log.Printf("✅ Populated liquidity_pools_current table")

	// Populate contract_data_current table with latest bronze data
	if err := st.populateContractDataCurrent(); err != nil {
		return fmt.Errorf("failed to populate contract_data_current: %w", err)
	}

	log.Printf("✅ Populated contract_data_current table")

	// Populate contract_code_current table with latest bronze data
	if err := st.populateContractCodeCurrent(); err != nil {
		return fmt.Errorf("failed to populate contract_code_current: %w", err)
	}

	log.Printf("✅ Populated contract_code_current table")

	// Populate config_settings_current table with latest bronze data
	if err := st.populateConfigSettingsCurrent(); err != nil {
		return fmt.Errorf("failed to populate config_settings_current: %w", err)
	}

	log.Printf("✅ Populated config_settings_current table")

	// Populate ttl_current table with latest bronze data
	if err := st.populateTTLCurrent(); err != nil {
		return fmt.Errorf("failed to populate ttl_current: %w", err)
	}

	log.Printf("✅ Populated ttl_current table")

	// Update checkpoint
	if maxSequence > st.lastProcessedSequence {
		if err := st.checkpointManager.Save(maxSequence); err != nil {
			log.Printf("⚠️  Failed to save checkpoint: %v", err)
		}
		st.lastProcessedSequence = maxSequence
	}

	return nil
}

// populateAccountsCurrent fetches current account state from bronze and inserts into silver
func (st *SilverTransformer) populateAccountsCurrent() error {
	// Fetch current accounts from bronze via Query API
	accounts, err := st.bronzeClient.QueryAccountsCurrent(st.config.BronzeSchema)
	if err != nil {
		return fmt.Errorf("failed to query accounts from bronze: %w", err)
	}

	if len(accounts) == 0 {
		log.Println("⚠️  No accounts found in bronze catalog")
		return nil
	}

	log.Printf("📥 Fetched %d accounts from bronze", len(accounts))

	// Begin transaction
	tx, err := st.silverDB.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Clear existing data (full refresh strategy)
	if _, err := tx.Exec("DELETE FROM accounts_current"); err != nil {
		return fmt.Errorf("failed to clear accounts_current: %w", err)
	}

	// Prepare INSERT statement (mapping bronze → silver with NULLs for missing fields)
	stmt, err := tx.Prepare(`
		INSERT INTO accounts_current (
			account_id, balance, buying_liabilities, selling_liabilities,
			sequence_number, num_subentries, num_sponsoring, num_sponsored,
			flags, home_domain, master_weight, threshold_low, threshold_medium,
			threshold_high, last_modified_ledger, ledger_entry_change, deleted,
			batch_id, batch_run_date, batch_insert_ts, closed_at, ledger_sequence
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	// Insert all accounts (mapping bronze fields to silver)
	for _, account := range accounts {
		_, err := stmt.Exec(
			account.AccountID,
			account.Balance,
			nil, // buying_liabilities - not in bronze
			nil, // selling_liabilities - not in bronze
			account.SequenceNumber,
			account.NumSubentries,
			account.NumSponsoring,
			account.NumSponsored,
			account.Flags,
			account.HomeDomain,
			account.MasterWeight,
			account.LowThreshold,  // bronze: low_threshold → silver: threshold_low
			account.MedThreshold,  // bronze: med_threshold → silver: threshold_medium
			account.HighThreshold, // bronze: high_threshold → silver: threshold_high
			nil,                   // last_modified_ledger - not in bronze accounts
			nil,                   // ledger_entry_change - not in bronze accounts
			false,                 // deleted - not in bronze accounts (assume false for current state)
			nil,                   // batch_id - not in bronze
			nil,                   // batch_run_date - not in bronze
			nil,                   // batch_insert_ts - not in bronze
			account.ClosedAt,
			account.LedgerSequence,
		)
		if err != nil {
			return fmt.Errorf("failed to insert account %s: %w", account.AccountID, err)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("💾 Inserted %d accounts into accounts_current", len(accounts))
	return nil
}

// populateAccountSignersCurrent fetches current account signers from bronze and inserts into silver
func (st *SilverTransformer) populateAccountSignersCurrent() error {
	// Fetch current account signers from bronze via Query API
	signers, err := st.bronzeClient.QueryAccountSignersCurrent(st.config.BronzeSchema)
	if err != nil {
		return fmt.Errorf("failed to query account signers from bronze: %w", err)
	}

	if len(signers) == 0 {
		log.Println("⚠️  No account signers found in bronze catalog")
		return nil
	}

	log.Printf("📥 Fetched %d account signers from bronze", len(signers))

	// Begin transaction
	tx, err := st.silverDB.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Clear existing data (full refresh strategy)
	if _, err := tx.Exec("DELETE FROM account_signers_current"); err != nil {
		return fmt.Errorf("failed to clear account_signers_current: %w", err)
	}

	// Prepare INSERT statement (mapping bronze → silver with NULLs for missing fields)
	stmt, err := tx.Prepare(`
		INSERT INTO account_signers_current (
			account_id, signer, ledger_sequence, weight, sponsor,
			deleted, closed_at, batch_id, batch_run_date, batch_insert_ts
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	// Insert all account signers (mapping bronze fields to silver)
	for _, signer := range signers {
		_, err := stmt.Exec(
			signer.AccountID,
			signer.Signer,
			signer.LedgerSequence,
			signer.Weight,
			signer.Sponsor,
			signer.Deleted, // Bronze filtered to false, but we store the value
			signer.ClosedAt,
			nil, // batch_id - not in bronze
			nil, // batch_run_date - not in bronze
			nil, // batch_insert_ts - not in bronze
		)
		if err != nil {
			return fmt.Errorf("failed to insert account signer %s/%s: %w", signer.AccountID, signer.Signer, err)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("💾 Inserted %d account signers into account_signers_current", len(signers))
	return nil
}

// populateTrustlinesCurrent fetches current trustlines from bronze and inserts into silver
func (st *SilverTransformer) populateTrustlinesCurrent() error {
	// Fetch current trustlines from bronze via Query API
	trustlines, err := st.bronzeClient.QueryTrustlinesCurrent(st.config.BronzeSchema)
	if err != nil {
		return fmt.Errorf("failed to query trustlines from bronze: %w", err)
	}

	if len(trustlines) == 0 {
		log.Println("⚠️  No trustlines found in bronze catalog")
		return nil
	}

	log.Printf("📥 Fetched %d trustlines from bronze", len(trustlines))

	// Begin transaction
	tx, err := st.silverDB.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Clear existing data (full refresh strategy)
	if _, err := tx.Exec("DELETE FROM trustlines_current"); err != nil {
		return fmt.Errorf("failed to clear trustlines_current: %w", err)
	}

	// Prepare INSERT statement (mapping bronze → silver with NULLs for missing fields)
	stmt, err := tx.Prepare(`
		INSERT INTO trustlines_current (
			account_id, asset_code, asset_issuer, asset_type, balance, trust_limit,
			buying_liabilities, selling_liabilities,
			authorized, authorized_to_maintain_liabilities, clawback_enabled,
			ledger_sequence, flags, last_modified_ledger,
			batch_id, batch_run_date, batch_insert_ts
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	// Insert all trustlines (mapping bronze fields to silver)
	for _, trustline := range trustlines {
		_, err := stmt.Exec(
			trustline.AccountID,
			trustline.AssetCode,
			trustline.AssetIssuer,
			trustline.AssetType,
			trustline.Balance,
			trustline.TrustLimit,
			trustline.BuyingLiabilities,
			trustline.SellingLiabilities,
			trustline.Authorized,
			trustline.AuthorizedToMaintainLiabilities,
			trustline.ClawbackEnabled,
			trustline.LedgerSequence,
			nil, // flags - not in bronze (auth flags broken out)
			nil, // last_modified_ledger - not in bronze
			nil, // batch_id - not in bronze
			nil, // batch_run_date - not in bronze
			nil, // batch_insert_ts - not in bronze
		)
		if err != nil {
			return fmt.Errorf("failed to insert trustline %s/%s/%s: %w",
				trustline.AccountID, trustline.AssetCode, trustline.AssetIssuer, err)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("💾 Inserted %d trustlines into trustlines_current", len(trustlines))
	return nil
}

// populateOffersCurrent fetches current offers from bronze and inserts into silver
func (st *SilverTransformer) populateOffersCurrent() error {
	offers, err := st.bronzeClient.QueryOffersCurrent(st.config.BronzeSchema)
	if err != nil {
		return fmt.Errorf("failed to query offers from bronze: %w", err)
	}

	if len(offers) == 0 {
		log.Println("⚠️  No offers found in bronze catalog")
		return nil
	}

	log.Printf("📥 Fetched %d offers from bronze", len(offers))

	tx, err := st.silverDB.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec("DELETE FROM offers_current"); err != nil {
		return fmt.Errorf("failed to clear offers_current: %w", err)
	}

	stmt, err := tx.Prepare(`
		INSERT INTO offers_current (
			offer_id, seller_account, ledger_sequence, closed_at,
			selling_asset_type, selling_asset_code, selling_asset_issuer,
			buying_asset_type, buying_asset_code, buying_asset_issuer,
			amount, price, flags,
			sponsor, last_modified_ledger,
			batch_id, batch_run_date, batch_insert_ts
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	for _, offer := range offers {
		_, err := stmt.Exec(
			offer.OfferID, offer.SellerAccount, offer.LedgerSequence, offer.ClosedAt,
			offer.SellingAssetType, offer.SellingAssetCode, offer.SellingAssetIssuer,
			offer.BuyingAssetType, offer.BuyingAssetCode, offer.BuyingAssetIssuer,
			offer.Amount, offer.Price, offer.Flags,
			nil, nil, nil, nil, nil, // sponsor, last_modified_ledger, batch fields - not in bronze
		)
		if err != nil {
			return fmt.Errorf("failed to insert offer %d: %w", offer.OfferID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("💾 Inserted %d offers into offers_current", len(offers))
	return nil
}

// populateClaimableBalancesCurrent fetches current claimable balances from bronze and inserts into silver
func (st *SilverTransformer) populateClaimableBalancesCurrent() error {
	balances, err := st.bronzeClient.QueryClaimableBalancesCurrent(st.config.BronzeSchema)
	if err != nil {
		return fmt.Errorf("failed to query claimable balances from bronze: %w", err)
	}

	if len(balances) == 0 {
		log.Println("⚠️  No claimable balances found in bronze catalog")
		return nil
	}

	log.Printf("📥 Fetched %d claimable balances from bronze", len(balances))

	tx, err := st.silverDB.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec("DELETE FROM claimable_balances_current"); err != nil {
		return fmt.Errorf("failed to clear claimable_balances_current: %w", err)
	}

	stmt, err := tx.Prepare(`
		INSERT INTO claimable_balances_current (
			balance_id, sponsor, ledger_sequence, closed_at,
			asset_type, asset_code, asset_issuer,
			amount, claimants_count, flags,
			claimants, last_modified_ledger,
			batch_id, batch_run_date, batch_insert_ts
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	for _, balance := range balances {
		_, err := stmt.Exec(
			balance.BalanceID, balance.Sponsor, balance.LedgerSequence, balance.ClosedAt,
			balance.AssetType, balance.AssetCode, balance.AssetIssuer,
			balance.Amount, balance.ClaimantsCount, balance.Flags,
			nil, nil, nil, nil, nil, // claimants, last_modified_ledger, batch fields - not in bronze
		)
		if err != nil {
			return fmt.Errorf("failed to insert claimable balance %s: %w", balance.BalanceID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("💾 Inserted %d claimable balances into claimable_balances_current", len(balances))
	return nil
}

// populateLiquidityPoolsCurrent fetches current liquidity pools from bronze and inserts into silver
func (st *SilverTransformer) populateLiquidityPoolsCurrent() error {
	pools, err := st.bronzeClient.QueryLiquidityPoolsCurrent(st.config.BronzeSchema)
	if err != nil {
		return fmt.Errorf("failed to query liquidity pools from bronze: %w", err)
	}

	if len(pools) == 0 {
		log.Println("⚠️  No liquidity pools found in bronze catalog")
		return nil
	}

	log.Printf("📥 Fetched %d liquidity pools from bronze", len(pools))

	tx, err := st.silverDB.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec("DELETE FROM liquidity_pools_current"); err != nil {
		return fmt.Errorf("failed to clear liquidity_pools_current: %w", err)
	}

	stmt, err := tx.Prepare(`
		INSERT INTO liquidity_pools_current (
			liquidity_pool_id, ledger_sequence, closed_at,
			pool_type, fee, trustline_count, total_pool_shares,
			asset_a_type, asset_a_code, asset_a_issuer, asset_a_amount,
			asset_b_type, asset_b_code, asset_b_issuer, asset_b_amount,
			last_modified_ledger, batch_id, batch_run_date, batch_insert_ts
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	for _, pool := range pools {
		_, err := stmt.Exec(
			pool.LiquidityPoolID, pool.LedgerSequence, pool.ClosedAt,
			pool.PoolType, pool.Fee, pool.TrustlineCount, pool.TotalPoolShares,
			pool.AssetAType, pool.AssetACode, pool.AssetAIssuer, pool.AssetAAmount,
			pool.AssetBType, pool.AssetBCode, pool.AssetBIssuer, pool.AssetBAmount,
			nil, nil, nil, nil, // last_modified_ledger, batch fields - not in bronze
		)
		if err != nil {
			return fmt.Errorf("failed to insert liquidity pool %s: %w", pool.LiquidityPoolID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("💾 Inserted %d liquidity pools into liquidity_pools_current", len(pools))
	return nil
}

// populateContractDataCurrent fetches current contract data from bronze and inserts into silver
func (st *SilverTransformer) populateContractDataCurrent() error {
	contractData, err := st.bronzeClient.QueryContractDataCurrent(st.config.BronzeSchema)
	if err != nil {
		return fmt.Errorf("failed to query contract data from bronze: %w", err)
	}

	if len(contractData) == 0 {
		log.Println("⚠️  No contract data found in bronze catalog")
		return nil
	}

	log.Printf("📥 Fetched %d contract data entries from bronze", len(contractData))

	tx, err := st.silverDB.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec("DELETE FROM contract_data_current"); err != nil {
		return fmt.Errorf("failed to clear contract_data_current: %w", err)
	}

	stmt, err := tx.Prepare(`
		INSERT INTO contract_data_current (
			contract_id, ledger_sequence, ledger_key_hash,
			contract_key_type, contract_durability,
			asset_code, asset_issuer, asset_type, balance_holder, balance,
			last_modified_ledger, ledger_entry_change, deleted, closed_at, contract_data_xdr,
			batch_id, batch_run_date, batch_insert_ts
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	for _, data := range contractData {
		_, err := stmt.Exec(
			data.ContractID, data.LedgerSequence, data.LedgerKeyHash,
			data.ContractKeyType, data.ContractDurability,
			data.AssetCode, data.AssetIssuer, data.AssetType, data.BalanceHolder, data.Balance,
			data.LastModifiedLedger, data.LedgerEntryChange, data.Deleted, data.ClosedAt, data.ContractDataXDR,
			nil, nil, nil, // batch fields - not in bronze
		)
		if err != nil {
			return fmt.Errorf("failed to insert contract data %s: %w", data.ContractID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("💾 Inserted %d contract data entries into contract_data_current", len(contractData))
	return nil
}

// populateContractCodeCurrent fetches current contract code from bronze and inserts into silver
func (st *SilverTransformer) populateContractCodeCurrent() error {
	codes, err := st.bronzeClient.QueryContractCodeCurrent(st.config.BronzeSchema)
	if err != nil {
		return fmt.Errorf("failed to query contract code from bronze: %w", err)
	}

	if len(codes) == 0 {
		log.Println("⚠️  No contract code found in bronze catalog")
		return nil
	}

	log.Printf("📥 Fetched %d contract code entries from bronze", len(codes))

	tx, err := st.silverDB.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec("DELETE FROM contract_code_current"); err != nil {
		return fmt.Errorf("failed to clear contract_code_current: %w", err)
	}

	stmt, err := tx.Prepare(`
		INSERT INTO contract_code_current (
			contract_code_hash, ledger_key_hash, contract_code_ext_v,
			last_modified_ledger, ledger_entry_change, deleted,
			closed_at, ledger_sequence,
			n_instructions, n_functions, n_globals, n_table_entries,
			n_types, n_data_segments, n_elem_segments, n_imports,
			n_exports, n_data_segment_bytes,
			batch_id, batch_run_date, batch_insert_ts
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	for _, code := range codes {
		_, err := stmt.Exec(
			code.ContractCodeHash, code.LedgerKeyHash, code.ContractCodeExtV,
			code.LastModifiedLedger, code.LedgerEntryChange, code.Deleted,
			code.ClosedAt, code.LedgerSequence,
			code.NInstructions, code.NFunctions, code.NGlobals, code.NTableEntries,
			code.NTypes, code.NDataSegments, code.NElemSegments, code.NImports,
			code.NExports, code.NDataSegmentBytes,
			nil, nil, nil, // batch fields - not in bronze
		)
		if err != nil {
			return fmt.Errorf("failed to insert contract code %s: %w", code.ContractCodeHash, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("💾 Inserted %d contract code entries into contract_code_current", len(codes))
	return nil
}

// populateConfigSettingsCurrent fetches current config settings from bronze and inserts into silver
func (st *SilverTransformer) populateConfigSettingsCurrent() error {
	settings, err := st.bronzeClient.QueryConfigSettingsCurrent(st.config.BronzeSchema)
	if err != nil {
		return fmt.Errorf("failed to query config settings from bronze: %w", err)
	}

	if len(settings) == 0 {
		log.Println("⚠️  No config settings found in bronze catalog")
		return nil
	}

	log.Printf("📥 Fetched %d config setting entries from bronze", len(settings))

	tx, err := st.silverDB.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec("DELETE FROM config_settings_current"); err != nil {
		return fmt.Errorf("failed to clear config_settings_current: %w", err)
	}

	stmt, err := tx.Prepare(`
		INSERT INTO config_settings_current (
			config_setting_id, ledger_sequence, last_modified_ledger,
			deleted, closed_at,
			ledger_max_instructions, tx_max_instructions,
			fee_rate_per_instructions_increment, tx_memory_limit,
			ledger_max_read_ledger_entries, ledger_max_read_bytes,
			ledger_max_write_ledger_entries, ledger_max_write_bytes,
			tx_max_read_ledger_entries, tx_max_read_bytes,
			tx_max_write_ledger_entries, tx_max_write_bytes,
			contract_max_size_bytes, config_setting_xdr,
			batch_id, batch_run_date, batch_insert_ts
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	for _, setting := range settings {
		_, err := stmt.Exec(
			setting.ConfigSettingID, setting.LedgerSequence, setting.LastModifiedLedger,
			setting.Deleted, setting.ClosedAt,
			setting.LedgerMaxInstructions, setting.TxMaxInstructions,
			setting.FeeRatePerInstructionsIncrement, setting.TxMemoryLimit,
			setting.LedgerMaxReadLedgerEntries, setting.LedgerMaxReadBytes,
			setting.LedgerMaxWriteLedgerEntries, setting.LedgerMaxWriteBytes,
			setting.TxMaxReadLedgerEntries, setting.TxMaxReadBytes,
			setting.TxMaxWriteLedgerEntries, setting.TxMaxWriteBytes,
			setting.ContractMaxSizeBytes, setting.ConfigSettingXDR,
			nil, nil, nil, // batch fields - not in bronze
		)
		if err != nil {
			return fmt.Errorf("failed to insert config setting %d: %w", setting.ConfigSettingID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("💾 Inserted %d config setting entries into config_settings_current", len(settings))
	return nil
}

// populateTTLCurrent fetches current TTL entries from bronze and inserts into silver
func (st *SilverTransformer) populateTTLCurrent() error {
	ttls, err := st.bronzeClient.QueryTTLCurrent(st.config.BronzeSchema)
	if err != nil {
		return fmt.Errorf("failed to query TTL entries from bronze: %w", err)
	}

	if len(ttls) == 0 {
		log.Println("⚠️  No TTL entries found in bronze catalog")
		return nil
	}

	log.Printf("📥 Fetched %d TTL entries from bronze", len(ttls))

	tx, err := st.silverDB.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec("DELETE FROM ttl_current"); err != nil {
		return fmt.Errorf("failed to clear ttl_current: %w", err)
	}

	stmt, err := tx.Prepare(`
		INSERT INTO ttl_current (
			key_hash, ledger_sequence, live_until_ledger_seq,
			ttl_remaining, expired, last_modified_ledger,
			deleted, closed_at,
			batch_id, batch_run_date, batch_insert_ts
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	for _, ttl := range ttls {
		_, err := stmt.Exec(
			ttl.KeyHash, ttl.LedgerSequence, ttl.LiveUntilLedgerSeq,
			ttl.TtlRemaining, ttl.Expired, ttl.LastModifiedLedger,
			ttl.Deleted, ttl.ClosedAt,
			nil, nil, nil, // batch fields - not in bronze
		)
		if err != nil {
			return fmt.Errorf("failed to insert TTL entry %s: %w", ttl.KeyHash, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("💾 Inserted %d TTL entries into ttl_current", len(ttls))
	return nil
}

// GetStats returns current processing statistics
func (st *SilverTransformer) GetStats() (ledgers, transactions, operations int64, currentSeq int64) {
	ledgers, transactions, operations, _ = st.checkpointManager.GetStats()
	return ledgers, transactions, operations, st.lastProcessedSequence
}
