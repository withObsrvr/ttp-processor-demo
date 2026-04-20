package txview

type SwapDetail struct {
	SoldAsset    string `json:"sold_asset"`
	SoldAmount   string `json:"sold_amount"`
	BoughtAsset  string `json:"bought_asset"`
	BoughtAmount string `json:"bought_amount"`
	Router       string `json:"router,omitempty"`
	Trader       string `json:"trader"`
}

type TransferDetail struct {
	Asset  string `json:"asset"`
	Amount string `json:"amount"`
	From   string `json:"from,omitempty"`
	To     string `json:"to,omitempty"`
}

type TxSummary struct {
	Description       string          `json:"description"`
	Type              string          `json:"type"`
	InvolvedContracts []string        `json:"involved_contracts,omitempty"`
	Swap              *SwapDetail     `json:"swap,omitempty"`
	Transfer          *TransferDetail `json:"transfer,omitempty"`
	Mint              *TransferDetail `json:"mint,omitempty"`
	Burn              *TransferDetail `json:"burn,omitempty"`
}

type TransactionInfo struct {
	TxHash                       string  `json:"tx_hash"`
	LedgerSequence               int64   `json:"ledger_sequence"`
	ClosedAt                     string  `json:"closed_at"`
	Successful                   bool    `json:"successful"`
	Fee                          int64   `json:"fee"`
	OperationCount               int     `json:"operation_count"`
	SourceAccount                *string `json:"source_account,omitempty"`
	AccountSequence              *int64  `json:"account_sequence,omitempty"`
	MaxFee                       *int64  `json:"max_fee,omitempty"`
	SorobanResourcesInstructions *int64  `json:"soroban_resources_instructions,omitempty"`
	SorobanResourcesReadBytes    *int64  `json:"soroban_resources_read_bytes,omitempty"`
	SorobanResourcesWriteBytes   *int64  `json:"soroban_resources_write_bytes,omitempty"`
}

type Operation struct {
	Index         int     `json:"index"`
	Type          int32   `json:"type"`
	TypeName      string  `json:"type_name"`
	SourceAccount string  `json:"source_account"`
	ContractID    *string `json:"contract_id,omitempty"`
	FunctionName  *string `json:"function_name,omitempty"`
	ArgumentsJSON *string `json:"arguments_json,omitempty"`
	Destination   *string `json:"destination,omitempty"`
	AssetCode     *string `json:"asset_code,omitempty"`
	Amount        *string `json:"amount,omitempty"`
	IsSorobanOp   bool    `json:"is_soroban_op"`
}

type Event struct {
	EventID        string  `json:"event_id"`
	ContractID     *string `json:"contract_id,omitempty"`
	LedgerSequence int64   `json:"ledger_sequence"`
	TxHash         string  `json:"tx_hash"`
	ClosedAt       string  `json:"closed_at"`
	EventType      string  `json:"event_type"`
	From           *string `json:"from,omitempty"`
	To             *string `json:"to,omitempty"`
	Amount         *string `json:"amount,omitempty"`
	AssetCode      *string `json:"asset_code,omitempty"`
	AssetIssuer    *string `json:"asset_issuer,omitempty"`
	TokenName      *string `json:"token_name,omitempty"`
	TokenSymbol    *string `json:"token_symbol,omitempty"`
	TokenDecimals  *int    `json:"token_decimals,omitempty"`
	TokenType      *string `json:"token_type,omitempty"`
	SourceType     string  `json:"source_type"`
	OperationType  *int32  `json:"operation_type,omitempty"`
	EventIndex     int     `json:"event_index"`
}

type SemanticTransactionClassification struct {
	TxType             string   `json:"tx_type"`
	Subtype            string   `json:"subtype,omitempty"`
	Confidence         string   `json:"confidence"`
	OperationTypes     []string `json:"operation_types,omitempty"`
	WalletInvolved     bool     `json:"wallet_involved"`
	EffectiveActorType string   `json:"effective_actor_type,omitempty"`
}

type SemanticActor struct {
	ActorID    string                 `json:"actor_id"`
	ActorType  string                 `json:"actor_type"`
	Label      *string                `json:"label,omitempty"`
	Roles      []string               `json:"roles"`
	Wallet     *SemanticWalletContext `json:"wallet,omitempty"`
	ContractID *string                `json:"contract_id,omitempty"`
}

type SemanticWalletContext struct {
	WalletType     string  `json:"wallet_type,omitempty"`
	Implementation string  `json:"implementation,omitempty"`
	Confidence     float64 `json:"confidence,omitempty"`
	HasCheckAuth   bool    `json:"has_check_auth,omitempty"`
	SignerCount    int     `json:"signer_count,omitempty"`
}

type SemanticAssetContext struct {
	Sent      *SemanticAssetMovement  `json:"sent,omitempty"`
	Received  *SemanticAssetMovement  `json:"received,omitempty"`
	Movements []SemanticAssetMovement `json:"movements,omitempty"`
	Path      []string                `json:"path,omitempty"`
}

type SemanticAssetMovement struct {
	Amount   string  `json:"amount"`
	Asset    string  `json:"asset"`
	From     *string `json:"from,omitempty"`
	To       *string `json:"to,omitempty"`
	Contract *string `json:"contract,omitempty"`
	Kind     string  `json:"kind,omitempty"`
}

type SemanticTransactionResponse struct {
	Transaction    TransactionInfo                   `json:"transaction"`
	Classification SemanticTransactionClassification `json:"classification"`
	Actors         []SemanticActor                   `json:"actors"`
	Assets         SemanticAssetContext              `json:"assets"`
	Operations     []Operation                       `json:"operations"`
	Events         []Event                           `json:"events"`
	LegacySummary  TxSummary                         `json:"legacy_summary"`
}
