package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"testing"
)

type fakeContractArtifactRPC struct {
	executable   ContractExecutableReference
	code         ContractCodeReference
	resolveErr   error
	fetchErr     error
	resolveCalls int
	fetchCalls   int
}

type upgradingContractArtifactRPC struct {
	executables []ContractExecutableReference
	codeByHash  map[string]ContractCodeReference
	resolveCall int
	fetchCalls  map[string]int
}

func (f *upgradingContractArtifactRPC) ResolveContractExecutable(context.Context, string) (ContractExecutableReference, error) {
	index := f.resolveCall
	if index >= len(f.executables) {
		index = len(f.executables) - 1
	}
	f.resolveCall++
	return f.executables[index], nil
}

func (f *upgradingContractArtifactRPC) FetchContractCode(_ context.Context, hash string) (ContractCodeReference, error) {
	if f.fetchCalls == nil {
		f.fetchCalls = make(map[string]int)
	}
	f.fetchCalls[hash]++
	code, ok := f.codeByHash[hash]
	if !ok {
		return ContractCodeReference{}, ErrContractCodeAbsent
	}
	return code, nil
}

func (f *fakeContractArtifactRPC) ResolveContractExecutable(context.Context, string) (ContractExecutableReference, error) {
	f.resolveCalls++
	return f.executable, f.resolveErr
}

func (f *fakeContractArtifactRPC) FetchContractCode(context.Context, string) (ContractCodeReference, error) {
	f.fetchCalls++
	return f.code, f.fetchErr
}

func TestContractArtifactServiceCachesImmutableCodeButResolvesExecutableEveryTime(t *testing.T) {
	wasm := testContractWASM(t)
	hashBytes := sha256.Sum256(wasm)
	hash := hex.EncodeToString(hashBytes[:])
	rpc := &fakeContractArtifactRPC{
		executable: ContractExecutableReference{Type: "wasm", WasmHash: hash, InstanceLedger: 90, ResolvedAtLedger: 100},
		code:       ContractCodeReference{WasmHash: hash, WASM: wasm, CodeLedger: 80, ResolvedAtLedger: 100},
	}
	store, err := NewFileContractArtifactStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	service := NewContractArtifactService("testnet", rpc, store, 0)

	response, err := service.Resolve(context.Background(), "CEXAMPLE")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if response.Executable.WasmHash != hash || response.Executable.WasmSizeBytes != len(wasm) || response.Provenance.CodeSource != "stellar_rpc" {
		t.Fatalf("unexpected response: %#v", response)
	}
	wasmResponse, err := service.ResolveWASM(context.Background(), "CEXAMPLE")
	if err != nil {
		t.Fatalf("ResolveWASM: %v", err)
	}
	if string(wasmResponse.WASM) != string(wasm) || wasmResponse.Executable.WasmSizeBytes != len(wasm) || wasmResponse.Provenance.CodeSource != "file_cache" {
		t.Fatalf("unexpected wasm response: %#v", wasmResponse)
	}
	if rpc.resolveCalls != 2 {
		t.Fatalf("resolve calls: got %d, want 2", rpc.resolveCalls)
	}
	if rpc.fetchCalls != 1 {
		t.Fatalf("fetch calls: got %d, want 1", rpc.fetchCalls)
	}

	if _, err := os.Stat(store.directory + "/" + hash + ".wasm"); err != nil {
		t.Fatalf("cached wasm missing: %v", err)
	}
	if _, err := os.Stat(store.directory + "/" + hash + ".json"); err != nil {
		t.Fatalf("cached interface missing: %v", err)
	}
}

func TestContractArtifactServiceEnforcesLimitOnCachedCode(t *testing.T) {
	wasm := testContractWASM(t)
	hashBytes := sha256.Sum256(wasm)
	hash := hex.EncodeToString(hashBytes[:])
	store, err := NewFileContractArtifactStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseContractWASM(wasm)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(&cachedContractCode{
		WasmHash:    hash,
		WASM:        wasm,
		Interface:   parsed.Spec,
		Metadata:    parsed.Metadata,
		Environment: parsed.Environment,
	}); err != nil {
		t.Fatal(err)
	}

	rpc := &fakeContractArtifactRPC{
		executable: ContractExecutableReference{Type: "wasm", WasmHash: hash},
		fetchErr:   errors.New("cached artifact must not be refetched"),
	}
	service := NewContractArtifactService("testnet", rpc, store, int64(len(wasm)-1))

	if _, err := service.Resolve(context.Background(), "CEXAMPLE"); err == nil || !strings.Contains(err.Error(), "exceeds configured") {
		t.Fatalf("expected cached artifact size error, got %v", err)
	}
	if rpc.fetchCalls != 0 {
		t.Fatalf("oversized cached artifact must not fall through to RPC, got %d fetch calls", rpc.fetchCalls)
	}
}

func TestContractArtifactServiceFollowsContractUpgradeAndRetainsBothImmutableArtifacts(t *testing.T) {
	wasmV1 := testContractWASM(t)
	wasmV2 := append(append([]byte(nil), wasmV1...), testWASMCustomSection("upgrade-test", []byte("v2"))...)
	hashV1Bytes := sha256.Sum256(wasmV1)
	hashV2Bytes := sha256.Sum256(wasmV2)
	hashV1 := hex.EncodeToString(hashV1Bytes[:])
	hashV2 := hex.EncodeToString(hashV2Bytes[:])
	rpc := &upgradingContractArtifactRPC{
		executables: []ContractExecutableReference{
			{Type: "wasm", WasmHash: hashV1, InstanceLedger: 90, ResolvedAtLedger: 100},
			{Type: "wasm", WasmHash: hashV2, InstanceLedger: 110, ResolvedAtLedger: 120},
			{Type: "wasm", WasmHash: hashV2, InstanceLedger: 110, ResolvedAtLedger: 121},
		},
		codeByHash: map[string]ContractCodeReference{
			hashV1: {WasmHash: hashV1, WASM: wasmV1, CodeLedger: 80},
			hashV2: {WasmHash: hashV2, WASM: wasmV2, CodeLedger: 105},
		},
	}
	store, err := NewFileContractArtifactStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	service := NewContractArtifactService("testnet", rpc, store, 0)

	beforeUpgrade, err := service.Resolve(context.Background(), "CEXAMPLE")
	if err != nil {
		t.Fatal(err)
	}
	afterUpgrade, err := service.Resolve(context.Background(), "CEXAMPLE")
	if err != nil {
		t.Fatal(err)
	}
	cachedAfterUpgrade, err := service.ResolveWASM(context.Background(), "CEXAMPLE")
	if err != nil {
		t.Fatal(err)
	}

	if beforeUpgrade.Executable.WasmHash != hashV1 || afterUpgrade.Executable.WasmHash != hashV2 || cachedAfterUpgrade.WasmHash != hashV2 {
		t.Fatalf("upgrade was not followed: before=%s after=%s wasm=%s", beforeUpgrade.Executable.WasmHash, afterUpgrade.Executable.WasmHash, cachedAfterUpgrade.WasmHash)
	}
	if rpc.fetchCalls[hashV1] != 1 || rpc.fetchCalls[hashV2] != 1 {
		t.Fatalf("immutable artifacts should each be fetched once: %#v", rpc.fetchCalls)
	}
	for _, hash := range []string{hashV1, hashV2} {
		if _, err := os.Stat(store.directory + "/" + hash + ".wasm"); err != nil {
			t.Fatalf("cached upgrade artifact %s missing: %v", hash, err)
		}
	}
}

func TestContractArtifactServiceRejectsHashMismatch(t *testing.T) {
	wasm := testContractWASM(t)
	rpc := &fakeContractArtifactRPC{
		executable: ContractExecutableReference{Type: "wasm", WasmHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		code:       ContractCodeReference{WASM: wasm},
	}
	service := NewContractArtifactService("testnet", rpc, nil, 0)
	if _, err := service.Resolve(context.Background(), "CEXAMPLE"); err == nil || !strings.Contains(err.Error(), "sha256") {
		t.Fatalf("expected hash mismatch, got %v", err)
	}
}

func TestContractArtifactServiceReturnsStaticSACInterfaceWithoutCodeFetch(t *testing.T) {
	rpc := &fakeContractArtifactRPC{executable: ContractExecutableReference{Type: "stellar_asset", ResolvedAtLedger: 123}}
	service := NewContractArtifactService("testnet", rpc, nil, 0)
	response, err := service.Resolve(context.Background(), "CEXAMPLE")
	if err != nil {
		t.Fatal(err)
	}
	if response.DetectedType != ContractTypeSEP41 || len(response.Interface.Functions) < 16 {
		t.Fatalf("unexpected SAC interface: %#v", response)
	}
	if rpc.fetchCalls != 0 {
		t.Fatalf("SAC must not fetch wasm, got %d calls", rpc.fetchCalls)
	}
	if _, err := service.ResolveWASM(context.Background(), "CEXAMPLE"); !errors.Is(err, ErrContractIsSAC) {
		t.Fatalf("expected ErrContractIsSAC, got %v", err)
	}
}
