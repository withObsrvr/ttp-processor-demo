package main

// ContractType represents the detected type of a Soroban contract
type ContractType string

const (
	ContractTypeSEP41   ContractType = "sep41_token"
	ContractTypeUnknown ContractType = "unknown"
)

// ContractInterface describes the detected interface of a contract
type ContractInterface struct {
	ContractID   string       `json:"contract_id"`
	ContractType ContractType `json:"contract_type"`
	Functions    []FunctionDef `json:"functions"`
}

// FunctionDef describes a function in a contract interface
type FunctionDef struct {
	Name        string   `json:"name"`
	Description string   `json:"description,omitempty"`
	Params      []string `json:"params,omitempty"`
}

// SEP-41 standard function signatures
var sep41Functions = []FunctionDef{
	{Name: "allowance", Description: "Get the allowance for a spender", Params: []string{"from: address", "spender: address"}},
	{Name: "approve", Description: "Set the allowance for a spender", Params: []string{"from: address", "spender: address", "amount: i128", "expiration_ledger: u32"}},
	{Name: "balance", Description: "Get the balance of an address", Params: []string{"id: address"}},
	{Name: "transfer", Description: "Transfer tokens between addresses", Params: []string{"from: address", "to: address", "amount: i128"}},
	{Name: "transfer_from", Description: "Transfer tokens on behalf of another address", Params: []string{"spender: address", "from: address", "to: address", "amount: i128"}},
	{Name: "burn", Description: "Burn tokens from an address", Params: []string{"from: address", "amount: i128"}},
	{Name: "burn_from", Description: "Burn tokens on behalf of another address", Params: []string{"spender: address", "from: address", "amount: i128"}},
	{Name: "decimals", Description: "Get the number of decimal places", Params: nil},
	{Name: "name", Description: "Get the token name", Params: nil},
	{Name: "symbol", Description: "Get the token symbol", Params: nil},
}

// sep41FunctionNames is the set of required SEP-41 functions for detection
var sep41FunctionNames = map[string]bool{
	"transfer": true,
	"balance":  true,
	"decimals": true,
	"name":     true,
	"symbol":   true,
}

// DetectContractType examines the observed function calls for a contract
// and determines its type. A contract is considered SEP-41 if it has
// been called with at least 3 of the 5 core SEP-41 functions.
func DetectContractType(observedFunctions []string) ContractType {
	matches := 0
	for _, fn := range observedFunctions {
		if sep41FunctionNames[fn] {
			matches++
		}
	}
	if matches >= 3 {
		return ContractTypeSEP41
	}
	return ContractTypeUnknown
}

// GetSEP41Interface returns the static SEP-41 interface definition
func GetSEP41Interface(contractID string) *ContractInterface {
	return &ContractInterface{
		ContractID:   contractID,
		ContractType: ContractTypeSEP41,
		Functions:    sep41Functions,
	}
}
