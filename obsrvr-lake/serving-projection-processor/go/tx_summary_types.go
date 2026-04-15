package main

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
