package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestParseContractWASMDecodesAuthoritativeInterfaceAndMetadata(t *testing.T) {
	wasm := testContractWASM(t)
	parsed, err := ParseContractWASM(wasm)
	if err != nil {
		t.Fatalf("ParseContractWASM: %v", err)
	}
	if got := len(parsed.Spec.Functions); got != 1 {
		t.Fatalf("functions: got %d, want 1", got)
	}
	function := parsed.Spec.Functions[0]
	if function.Name != "init" || len(function.Inputs) != 2 || function.Inputs[1].Type != "Vec<BytesN<32>>" {
		t.Fatalf("unexpected function: %#v", function)
	}
	if len(function.Outputs) != 1 || function.Outputs[0] != "Result<Void, Errors>" {
		t.Fatalf("unexpected outputs: %#v", function.Outputs)
	}
	if len(parsed.Spec.Structs) != 1 || parsed.Spec.Structs[0].Name != "PriceEntry" {
		t.Fatalf("unexpected structs: %#v", parsed.Spec.Structs)
	}
	if len(parsed.Spec.Unions) != 1 || parsed.Spec.Unions[0].Cases[1].Values[0] != "BytesN<8>" {
		t.Fatalf("unexpected unions: %#v", parsed.Spec.Unions)
	}
	if len(parsed.Spec.Errors) != 1 || parsed.Spec.Errors[0].Cases[0].Value != 1 {
		t.Fatalf("unexpected errors: %#v", parsed.Spec.Errors)
	}
	if len(parsed.Metadata) != 1 || parsed.Metadata[0].Key != "Description" {
		t.Fatalf("unexpected metadata: %#v", parsed.Metadata)
	}
	if parsed.Environment.InterfaceVersion == nil || parsed.Environment.InterfaceVersion.Protocol != 23 {
		t.Fatalf("unexpected environment: %#v", parsed.Environment)
	}

	rust := RenderContractSpecRust(parsed.Spec)
	for _, expected := range []string{
		"fn init(admin: Address, publishers: Vec<BytesN<32>>) -> Result<Void, Errors>",
		"struct PriceEntry", "union DataKey", "enum Errors", "AlreadyInitialized = 1",
	} {
		if !strings.Contains(rust, expected) {
			t.Fatalf("rust rendering missing %q:\n%s", expected, rust)
		}
	}
}

func TestParseContractWASMRejectsMissingAndDuplicateSpecs(t *testing.T) {
	withoutSpec := append([]byte("\x00asm\x01\x00\x00\x00"), testWASMCustomSection("other", []byte{1, 2, 3})...)
	if _, err := ParseContractWASM(withoutSpec); err == nil || !strings.Contains(err.Error(), "contractspecv0") {
		t.Fatalf("expected missing spec error, got %v", err)
	}

	spec := marshalXDRStream(t, testSpecEntries(t))
	duplicate := append([]byte("\x00asm\x01\x00\x00\x00"), testWASMCustomSection(contractSpecSectionName, spec)...)
	duplicate = append(duplicate, testWASMCustomSection(contractSpecSectionName, spec)...)
	if _, err := ParseContractWASM(duplicate); err == nil || !strings.Contains(err.Error(), "expected one") {
		t.Fatalf("expected duplicate spec error, got %v", err)
	}
}

func testContractWASM(t *testing.T) []byte {
	t.Helper()
	wasm := []byte("\x00asm\x01\x00\x00\x00")
	wasm = append(wasm, testWASMCustomSection(contractSpecSectionName, marshalXDRStream(t, testSpecEntries(t)))...)
	meta, err := xdr.NewScMetaEntry(xdr.ScMetaKindScMetaV0, xdr.ScMetaV0{Key: "Description", Val: "Oracle contract"})
	if err != nil {
		t.Fatal(err)
	}
	wasm = append(wasm, testWASMCustomSection(contractMetaSectionName, marshalXDRStream(t, []xdr.ScMetaEntry{meta}))...)
	env, err := xdr.NewScEnvMetaEntry(xdr.ScEnvMetaKindScEnvMetaKindInterfaceVersion, xdr.ScEnvMetaEntryInterfaceVersion{Protocol: 23, PreRelease: 0})
	if err != nil {
		t.Fatal(err)
	}
	wasm = append(wasm, testWASMCustomSection(contractEnvMetaSectionName, marshalXDRStream(t, []xdr.ScEnvMetaEntry{env}))...)
	return wasm
}

func testSpecEntries(t *testing.T) []xdr.ScSpecEntry {
	t.Helper()
	address := xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeAddress}
	voidType := xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeVoid}
	bytes32 := xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeBytesN, BytesN: &xdr.ScSpecTypeBytesN{N: 32}}
	bytes8 := xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeBytesN, BytesN: &xdr.ScSpecTypeBytesN{N: 8}}
	vecBytes32 := xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeVec, Vec: &xdr.ScSpecTypeVec{ElementType: bytes32}}
	errorsType := xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeUdt, Udt: &xdr.ScSpecTypeUdt{Name: "Errors"}}
	resultType := xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeResult, Result: &xdr.ScSpecTypeResult{OkType: voidType, ErrorType: errorsType}}

	function := mustSpecEntry(t, xdr.ScSpecEntryKindScSpecEntryFunctionV0, xdr.ScSpecFunctionV0{
		Doc: "One-time setup", Name: xdr.ScSymbol("init"),
		Inputs:  []xdr.ScSpecFunctionInputV0{{Name: "admin", Type: address}, {Name: "publishers", Type: vecBytes32}},
		Outputs: []xdr.ScSpecTypeDef{resultType},
	})
	structure := mustSpecEntry(t, xdr.ScSpecEntryKindScSpecEntryUdtStructV0, xdr.ScSpecUdtStructV0{
		Name: "PriceEntry", Fields: []xdr.ScSpecUdtStructFieldV0{{Name: "price", Type: xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeI128}}},
	})
	union := mustSpecEntry(t, xdr.ScSpecEntryKindScSpecEntryUdtUnionV0, xdr.ScSpecUdtUnionV0{
		Name: "DataKey", Cases: []xdr.ScSpecUdtUnionCaseV0{
			{Kind: xdr.ScSpecUdtUnionCaseV0KindScSpecUdtUnionCaseVoidV0, VoidCase: &xdr.ScSpecUdtUnionCaseVoidV0{Name: "Admin"}},
			{Kind: xdr.ScSpecUdtUnionCaseV0KindScSpecUdtUnionCaseTupleV0, TupleCase: &xdr.ScSpecUdtUnionCaseTupleV0{Name: "Price", Type: []xdr.ScSpecTypeDef{bytes8}}},
		},
	})
	errorEnum := mustSpecEntry(t, xdr.ScSpecEntryKindScSpecEntryUdtErrorEnumV0, xdr.ScSpecUdtErrorEnumV0{
		Name: "Errors", Cases: []xdr.ScSpecUdtErrorEnumCaseV0{{Name: "AlreadyInitialized", Value: 1}},
	})
	return []xdr.ScSpecEntry{function, structure, union, errorEnum}
}

func mustSpecEntry(t *testing.T, kind xdr.ScSpecEntryKind, value any) xdr.ScSpecEntry {
	t.Helper()
	entry, err := xdr.NewScSpecEntry(kind, value)
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

func marshalXDRStream[T any](t *testing.T, entries []T) []byte {
	t.Helper()
	var buffer bytes.Buffer
	for _, entry := range entries {
		if _, err := xdr.Marshal(&buffer, entry); err != nil {
			t.Fatalf("marshal xdr: %v", err)
		}
	}
	return buffer.Bytes()
}

func testWASMCustomSection(name string, payload []byte) []byte {
	nameLength := testEncodeLEB128(uint32(len(name)))
	content := append(append(nameLength, []byte(name)...), payload...)
	section := []byte{0}
	section = append(section, testEncodeLEB128(uint32(len(content)))...)
	return append(section, content...)
}

func testEncodeLEB128(value uint32) []byte {
	var result []byte
	for {
		b := byte(value & 0x7f)
		value >>= 7
		if value != 0 {
			b |= 0x80
		}
		result = append(result, b)
		if value == 0 {
			return result
		}
	}
}
