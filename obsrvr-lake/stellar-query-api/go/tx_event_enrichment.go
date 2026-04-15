package main

import "strings"

type tokenRegistryInfo struct {
	Name      *string
	Symbol    *string
	Decimals  *int
	TokenType *string
	Issuer    *string
}

func inferIssuerFromTokenInfo(name, symbol, tokenType *string) *string {
	if tokenType == nil || *tokenType != "sac" || name == nil {
		return nil
	}
	parts := strings.SplitN(*name, ":", 2)
	if len(parts) != 2 {
		return nil
	}
	if symbol != nil && parts[0] != *symbol {
		return nil
	}
	issuer := parts[1]
	if issuer == "" {
		return nil
	}
	return &issuer
}

func applyTokenRegistryInfoToEvent(e *UnifiedEvent, info tokenRegistryInfo) {
	e.TokenName = info.Name
	e.TokenSymbol = info.Symbol
	e.TokenDecimals = info.Decimals
	e.TokenType = info.TokenType
	if e.AssetCode == nil && info.Symbol != nil {
		e.AssetCode = info.Symbol
	}
	if e.AssetIssuer == nil && info.Issuer != nil {
		e.AssetIssuer = info.Issuer
	}
}
