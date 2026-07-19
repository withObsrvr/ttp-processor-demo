package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const defaultMaxContractWASMBytes int64 = 4 << 20

var (
	ErrInvalidContractID  = errors.New("invalid contract id")
	ErrContractNotFound   = errors.New("contract not found")
	ErrContractCodeAbsent = errors.New("contract code not found")
	ErrContractIsSAC      = errors.New("stellar asset contract has no wasm artifact")
)

type ContractExecutableReference struct {
	Type             string `json:"type"`
	WasmHash         string `json:"wasm_hash,omitempty"`
	WasmSizeBytes    int    `json:"wasm_size_bytes,omitempty"`
	InstanceLedger   int64  `json:"instance_last_modified_ledger,omitempty"`
	LiveUntilLedger  *int64 `json:"live_until_ledger,omitempty"`
	ResolvedAtLedger int64  `json:"resolved_at_ledger,omitempty"`
}

type ContractCodeReference struct {
	WasmHash         string
	WASM             []byte
	CodeLedger       int64
	LiveUntilLedger  *int64
	ResolvedAtLedger int64
}

type ContractArtifactRPC interface {
	ResolveContractExecutable(context.Context, string) (ContractExecutableReference, error)
	FetchContractCode(context.Context, string) (ContractCodeReference, error)
}

type ContractArtifactResolver interface {
	Resolve(context.Context, string) (*ContractInterfaceResponse, error)
	ResolveWASM(context.Context, string) (*ContractWASMArtifact, error)
}

type ContractInterfaceResponse struct {
	ContractID        string                      `json:"contract_id"`
	Network           string                      `json:"network"`
	DetectedType      ContractType                `json:"detected_type"`
	Executable        ContractExecutableReference `json:"executable"`
	Interface         ContractSpec                `json:"interface"`
	Metadata          []ContractMetadataEntry     `json:"metadata"`
	Environment       ContractEnvironment         `json:"environment"`
	Provenance        ContractArtifactProvenance  `json:"provenance"`
	ObservedFunctions []string                    `json:"observed_functions"`
}

type ContractArtifactProvenance struct {
	ExecutableSource string `json:"executable_source"`
	CodeSource       string `json:"code_source"`
	CodeLedger       int64  `json:"code_last_modified_ledger,omitempty"`
	ResolvedAtLedger int64  `json:"resolved_at_ledger,omitempty"`
}

type ContractWASMArtifact struct {
	ContractID string
	Network    string
	WasmHash   string
	WASM       []byte
	Executable ContractExecutableReference
	Provenance ContractArtifactProvenance
}

type cachedContractCode struct {
	WasmHash    string                  `json:"wasm_hash"`
	WasmSize    int                     `json:"wasm_size"`
	Interface   ContractSpec            `json:"interface"`
	Metadata    []ContractMetadataEntry `json:"metadata"`
	Environment ContractEnvironment     `json:"environment"`
	CodeLedger  int64                   `json:"code_last_modified_ledger,omitempty"`
	WASM        []byte                  `json:"-"`
}

type contractArtifactStore interface {
	Load(string) (*cachedContractCode, error)
	Save(*cachedContractCode) error
}

type FileContractArtifactStore struct {
	directory string
}

func NewFileContractArtifactStore(directory string) (*FileContractArtifactStore, error) {
	directory = strings.TrimSpace(directory)
	if directory == "" {
		return nil, errors.New("contract artifact cache directory is empty")
	}
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return nil, fmt.Errorf("create contract artifact cache: %w", err)
	}
	return &FileContractArtifactStore{directory: directory}, nil
}

func (s *FileContractArtifactStore) Load(hash string) (*cachedContractCode, error) {
	hash, err := normalizeContractWasmHash(hash)
	if err != nil {
		return nil, err
	}
	metadataBytes, err := os.ReadFile(filepath.Join(s.directory, hash+".json"))
	if err != nil {
		return nil, err
	}
	wasm, err := os.ReadFile(filepath.Join(s.directory, hash+".wasm"))
	if err != nil {
		return nil, err
	}
	var cached cachedContractCode
	if err := json.Unmarshal(metadataBytes, &cached); err != nil {
		return nil, fmt.Errorf("decode cached contract metadata: %w", err)
	}
	if cached.WasmHash != hash || cached.WasmSize != len(wasm) {
		return nil, errors.New("cached contract artifact metadata mismatch")
	}
	if err := validateWasmHash(hash, wasm); err != nil {
		return nil, err
	}
	cached.WASM = wasm
	return &cached, nil
}

func (s *FileContractArtifactStore) Save(cached *cachedContractCode) error {
	if cached == nil {
		return errors.New("cannot cache nil contract code")
	}
	hash, err := normalizeContractWasmHash(cached.WasmHash)
	if err != nil {
		return err
	}
	if err := validateWasmHash(hash, cached.WASM); err != nil {
		return err
	}
	cached.WasmHash = hash
	cached.WasmSize = len(cached.WASM)
	metadata, err := json.Marshal(cached)
	if err != nil {
		return fmt.Errorf("encode contract artifact metadata: %w", err)
	}
	if err := atomicWriteFile(s.directory, hash+".wasm", cached.WASM, 0o640); err != nil {
		return err
	}
	if err := atomicWriteFile(s.directory, hash+".json", append(metadata, '\n'), 0o640); err != nil {
		return err
	}
	return nil
}

func atomicWriteFile(directory, name string, data []byte, mode os.FileMode) error {
	temp, err := os.CreateTemp(directory, ".contract-artifact-*")
	if err != nil {
		return fmt.Errorf("create temporary contract artifact: %w", err)
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err := temp.Chmod(mode); err != nil {
		temp.Close()
		return fmt.Errorf("set contract artifact permissions: %w", err)
	}
	if _, err := temp.Write(data); err != nil {
		temp.Close()
		return fmt.Errorf("write contract artifact: %w", err)
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return fmt.Errorf("sync contract artifact: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close contract artifact: %w", err)
	}
	if err := os.Rename(tempName, filepath.Join(directory, name)); err != nil {
		return fmt.Errorf("publish contract artifact: %w", err)
	}
	return nil
}

type ContractArtifactService struct {
	network      string
	rpc          ContractArtifactRPC
	store        contractArtifactStore
	maxWasmBytes int64
}

func NewContractArtifactService(network string, rpc ContractArtifactRPC, store contractArtifactStore, maxWasmBytes int64) *ContractArtifactService {
	if maxWasmBytes <= 0 {
		maxWasmBytes = defaultMaxContractWASMBytes
	}
	return &ContractArtifactService{network: network, rpc: rpc, store: store, maxWasmBytes: maxWasmBytes}
}

func (s *ContractArtifactService) Resolve(ctx context.Context, contractID string) (*ContractInterfaceResponse, error) {
	if s == nil || s.rpc == nil {
		return nil, errors.New("contract artifact resolver is not configured")
	}
	executable, err := s.rpc.ResolveContractExecutable(ctx, contractID)
	if err != nil {
		return nil, err
	}
	if executable.Type == "stellar_asset" {
		spec := stellarAssetContractSpec()
		return &ContractInterfaceResponse{
			ContractID: contractID, Network: s.network, DetectedType: ContractTypeSEP41,
			Executable: executable, Interface: spec, Metadata: []ContractMetadataEntry{}, Environment: ContractEnvironment{},
			Provenance:        ContractArtifactProvenance{ExecutableSource: "stellar_rpc", CodeSource: "protocol_builtin", ResolvedAtLedger: executable.ResolvedAtLedger},
			ObservedFunctions: []string{},
		}, nil
	}

	cached, codeSource, err := s.resolveCode(ctx, executable.WasmHash)
	if err != nil {
		return nil, err
	}
	executable.WasmSizeBytes = cached.WasmSize
	functionNames := make([]string, 0, len(cached.Interface.Functions))
	for _, function := range cached.Interface.Functions {
		functionNames = append(functionNames, function.Name)
	}
	return &ContractInterfaceResponse{
		ContractID: contractID, Network: s.network, DetectedType: DetectContractType(functionNames),
		Executable: executable, Interface: cached.Interface, Metadata: cached.Metadata, Environment: cached.Environment,
		Provenance:        ContractArtifactProvenance{ExecutableSource: "stellar_rpc", CodeSource: codeSource, CodeLedger: cached.CodeLedger, ResolvedAtLedger: executable.ResolvedAtLedger},
		ObservedFunctions: []string{},
	}, nil
}

func (s *ContractArtifactService) ResolveWASM(ctx context.Context, contractID string) (*ContractWASMArtifact, error) {
	if s == nil || s.rpc == nil {
		return nil, errors.New("contract artifact resolver is not configured")
	}
	executable, err := s.rpc.ResolveContractExecutable(ctx, contractID)
	if err != nil {
		return nil, err
	}
	if executable.Type == "stellar_asset" {
		return nil, ErrContractIsSAC
	}
	cached, codeSource, err := s.resolveCode(ctx, executable.WasmHash)
	if err != nil {
		return nil, err
	}
	executable.WasmSizeBytes = cached.WasmSize
	return &ContractWASMArtifact{
		ContractID: contractID, Network: s.network, WasmHash: cached.WasmHash, WASM: cached.WASM, Executable: executable,
		Provenance: ContractArtifactProvenance{ExecutableSource: "stellar_rpc", CodeSource: codeSource, CodeLedger: cached.CodeLedger, ResolvedAtLedger: executable.ResolvedAtLedger},
	}, nil
}

func (s *ContractArtifactService) resolveCode(ctx context.Context, hash string) (*cachedContractCode, string, error) {
	hash, err := normalizeContractWasmHash(hash)
	if err != nil {
		return nil, "", err
	}
	if s.store != nil {
		cached, err := s.store.Load(hash)
		if err == nil {
			if int64(cached.WasmSize) > s.maxWasmBytes {
				return nil, "", fmt.Errorf("contract wasm exceeds configured %d-byte limit", s.maxWasmBytes)
			}
			return cached, "file_cache", nil
		}
		// Missing or corrupt cache entries are never served. Fetching the code
		// again from the authoritative source repairs the content-addressed entry.
	}

	code, err := s.rpc.FetchContractCode(ctx, hash)
	if err != nil {
		return nil, "", err
	}
	if int64(len(code.WASM)) > s.maxWasmBytes {
		return nil, "", fmt.Errorf("contract wasm exceeds configured %d-byte limit", s.maxWasmBytes)
	}
	if err := validateWasmHash(hash, code.WASM); err != nil {
		return nil, "", err
	}
	parsed, err := ParseContractWASM(code.WASM)
	if err != nil {
		return nil, "", err
	}
	cached := &cachedContractCode{
		WasmHash: hash, WasmSize: len(code.WASM), Interface: parsed.Spec, Metadata: parsed.Metadata,
		Environment: parsed.Environment, CodeLedger: code.CodeLedger, WASM: append([]byte(nil), code.WASM...),
	}
	if s.store != nil {
		if err := s.store.Save(cached); err != nil {
			return nil, "", fmt.Errorf("persist contract artifact: %w", err)
		}
	}
	return cached, "stellar_rpc", nil
}

func normalizeContractWasmHash(hash string) (string, error) {
	hash = strings.ToLower(strings.TrimSpace(hash))
	decoded, err := hex.DecodeString(hash)
	if err != nil || len(decoded) != sha256.Size {
		return "", errors.New("wasm hash must be 64 hexadecimal characters")
	}
	return hash, nil
}

func validateWasmHash(expected string, wasm []byte) error {
	actual := sha256.Sum256(wasm)
	if hex.EncodeToString(actual[:]) != expected {
		return fmt.Errorf("contract wasm sha256 does not match ledger hash %s", expected)
	}
	return nil
}

func stellarAssetContractSpec() ContractSpec {
	field := func(name, valueType string) ContractSpecField { return ContractSpecField{Name: name, Type: valueType} }
	fn := func(name string, inputs []ContractSpecField, outputs ...string) ContractSpecFunction {
		return ContractSpecFunction{Name: name, Inputs: inputs, Outputs: outputs}
	}
	return ContractSpec{
		Functions: []ContractSpecFunction{
			fn("allowance", []ContractSpecField{field("from", "Address"), field("spender", "Address")}, "i128"),
			fn("approve", []ContractSpecField{field("from", "Address"), field("spender", "Address"), field("amount", "i128"), field("expiration_ledger", "u32")}),
			fn("balance", []ContractSpecField{field("id", "Address")}, "i128"),
			fn("transfer", []ContractSpecField{field("from", "Address"), field("to", "MuxedAddress"), field("amount", "i128")}),
			fn("transfer_from", []ContractSpecField{field("spender", "Address"), field("from", "Address"), field("to", "Address"), field("amount", "i128")}),
			fn("burn", []ContractSpecField{field("from", "Address"), field("amount", "i128")}),
			fn("burn_from", []ContractSpecField{field("spender", "Address"), field("from", "Address"), field("amount", "i128")}),
			fn("decimals", nil, "u32"), fn("name", nil, "String"), fn("symbol", nil, "String"),
			fn("set_admin", []ContractSpecField{field("new_admin", "Address")}), fn("admin", nil, "Address"),
			fn("set_authorized", []ContractSpecField{field("id", "Address"), field("authorize", "bool")}),
			fn("authorized", []ContractSpecField{field("id", "Address")}, "bool"),
			fn("mint", []ContractSpecField{field("to", "Address"), field("amount", "i128")}),
			fn("clawback", []ContractSpecField{field("from", "Address"), field("amount", "i128")}),
		},
		Structs: []ContractSpecStruct{}, Unions: []ContractSpecUnion{}, Enums: []ContractSpecEnum{}, Events: []ContractSpecEvent{},
		Errors: []ContractSpecEnum{{Name: "ContractError", Cases: []ContractSpecEnumCase{
			{Name: "InternalError", Value: 1}, {Name: "OperationNotSupportedError", Value: 2}, {Name: "AlreadyInitializedError", Value: 3},
			{Name: "AccountMissingError", Value: 6}, {Name: "NegativeAmountError", Value: 8}, {Name: "AllowanceError", Value: 9},
			{Name: "BalanceError", Value: 10}, {Name: "BalanceDeauthorizedError", Value: 11}, {Name: "OverflowError", Value: 12},
			{Name: "TrustlineMissingError", Value: 13},
		}}},
	}
}
