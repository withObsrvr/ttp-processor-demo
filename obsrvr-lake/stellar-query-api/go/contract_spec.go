package main

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/stellar/go-stellar-sdk/xdr"
)

const (
	contractSpecSectionName    = "contractspecv0"
	contractMetaSectionName    = "contractmetav0"
	contractEnvMetaSectionName = "contractenvmetav0"
	maxContractSpecEntries     = 4096
)

var (
	errInvalidWASM        = errors.New("invalid wasm module")
	errContractSpecAbsent = errors.New("contractspecv0 section not found")
)

type ContractSpec struct {
	Functions []ContractSpecFunction `json:"functions"`
	Structs   []ContractSpecStruct   `json:"structs"`
	Unions    []ContractSpecUnion    `json:"unions"`
	Enums     []ContractSpecEnum     `json:"enums"`
	Errors    []ContractSpecEnum     `json:"errors"`
	Events    []ContractSpecEvent    `json:"events"`
}

type ContractSpecFunction struct {
	Name    string              `json:"name"`
	Doc     string              `json:"doc,omitempty"`
	Inputs  []ContractSpecField `json:"inputs"`
	Outputs []string            `json:"outputs"`
}

type ContractSpecField struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Doc  string `json:"doc,omitempty"`
}

type ContractSpecStruct struct {
	Name   string              `json:"name"`
	Doc    string              `json:"doc,omitempty"`
	Lib    string              `json:"lib,omitempty"`
	Fields []ContractSpecField `json:"fields"`
}

type ContractSpecUnion struct {
	Name  string                  `json:"name"`
	Doc   string                  `json:"doc,omitempty"`
	Lib   string                  `json:"lib,omitempty"`
	Cases []ContractSpecUnionCase `json:"cases"`
}

type ContractSpecUnionCase struct {
	Name   string   `json:"name"`
	Doc    string   `json:"doc,omitempty"`
	Values []string `json:"values"`
}

type ContractSpecEnum struct {
	Name  string                 `json:"name"`
	Doc   string                 `json:"doc,omitempty"`
	Lib   string                 `json:"lib,omitempty"`
	Cases []ContractSpecEnumCase `json:"cases"`
}

type ContractSpecEnumCase struct {
	Name  string `json:"name"`
	Value uint32 `json:"value"`
	Doc   string `json:"doc,omitempty"`
}

type ContractSpecEvent struct {
	Name         string                   `json:"name"`
	Doc          string                   `json:"doc,omitempty"`
	Lib          string                   `json:"lib,omitempty"`
	PrefixTopics []string                 `json:"prefix_topics"`
	Params       []ContractSpecEventParam `json:"params"`
	DataFormat   string                   `json:"data_format"`
}

type ContractSpecEventParam struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Location string `json:"location"`
	Doc      string `json:"doc,omitempty"`
}

type ContractMetadataEntry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type ContractEnvironment struct {
	InterfaceVersion *ContractEnvironmentVersion `json:"interface_version,omitempty"`
}

type ContractEnvironmentVersion struct {
	Protocol   uint32 `json:"protocol"`
	PreRelease uint32 `json:"pre_release"`
}

type ParsedContractWASM struct {
	Spec        ContractSpec            `json:"interface"`
	Metadata    []ContractMetadataEntry `json:"metadata"`
	Environment ContractEnvironment     `json:"environment"`
}

func ParseContractWASM(wasm []byte) (*ParsedContractWASM, error) {
	sections, err := parseWASMCustomSections(wasm)
	if err != nil {
		return nil, err
	}

	specSections := sections[contractSpecSectionName]
	if len(specSections) == 0 {
		return nil, errContractSpecAbsent
	}
	if len(specSections) != 1 {
		return nil, fmt.Errorf("%w: expected one %s section, found %d", errInvalidWASM, contractSpecSectionName, len(specSections))
	}

	entries, err := decodeContractSpecEntries(specSections[0])
	if err != nil {
		return nil, err
	}
	parsed := &ParsedContractWASM{Spec: contractSpecFromXDR(entries)}

	for _, section := range sections[contractMetaSectionName] {
		metadata, err := decodeContractMetadata(section)
		if err != nil {
			return nil, err
		}
		parsed.Metadata = append(parsed.Metadata, metadata...)
	}
	for _, section := range sections[contractEnvMetaSectionName] {
		environment, err := decodeContractEnvironment(section)
		if err != nil {
			return nil, err
		}
		if environment.InterfaceVersion != nil {
			parsed.Environment = environment
		}
	}

	return parsed, nil
}

func parseWASMCustomSections(wasm []byte) (map[string][][]byte, error) {
	if len(wasm) < 8 || !bytes.Equal(wasm[:4], []byte{'\x00', 'a', 's', 'm'}) {
		return nil, fmt.Errorf("%w: missing wasm magic", errInvalidWASM)
	}
	if !bytes.Equal(wasm[4:8], []byte{'\x01', '\x00', '\x00', '\x00'}) {
		return nil, fmt.Errorf("%w: unsupported wasm version", errInvalidWASM)
	}

	sections := make(map[string][][]byte)
	for offset := 8; offset < len(wasm); {
		sectionID := wasm[offset]
		offset++
		sectionSize, n, err := decodeWASMU32(wasm[offset:])
		if err != nil {
			return nil, fmt.Errorf("%w: section size: %v", errInvalidWASM, err)
		}
		offset += n
		if uint64(sectionSize) > uint64(len(wasm)-offset) {
			return nil, fmt.Errorf("%w: section exceeds module length", errInvalidWASM)
		}
		sectionEnd := offset + int(sectionSize)
		if sectionID == 0 {
			nameLen, nameBytes, err := decodeWASMU32(wasm[offset:sectionEnd])
			if err != nil {
				return nil, fmt.Errorf("%w: custom section name: %v", errInvalidWASM, err)
			}
			nameStart := offset + nameBytes
			if uint64(nameLen) > uint64(sectionEnd-nameStart) {
				return nil, fmt.Errorf("%w: custom section name exceeds section length", errInvalidWASM)
			}
			nameEnd := nameStart + int(nameLen)
			name := string(wasm[nameStart:nameEnd])
			payload := append([]byte(nil), wasm[nameEnd:sectionEnd]...)
			sections[name] = append(sections[name], payload)
		}
		offset = sectionEnd
	}
	return sections, nil
}

func decodeWASMU32(data []byte) (uint32, int, error) {
	var value uint32
	for i := 0; i < len(data) && i < 5; i++ {
		b := data[i]
		if i == 4 && b&0xf0 != 0 {
			return 0, 0, errors.New("u32 LEB128 overflow")
		}
		value |= uint32(b&0x7f) << (7 * i)
		if b&0x80 == 0 {
			return value, i + 1, nil
		}
	}
	return 0, 0, errors.New("truncated u32 LEB128")
}

func decodeContractSpecEntries(data []byte) ([]xdr.ScSpecEntry, error) {
	reader := bytes.NewReader(data)
	entries := make([]xdr.ScSpecEntry, 0)
	for reader.Len() > 0 {
		if len(entries) >= maxContractSpecEntries {
			return nil, fmt.Errorf("contract spec exceeds %d entries", maxContractSpecEntries)
		}
		before := reader.Len()
		var entry xdr.ScSpecEntry
		n, err := xdr.Unmarshal(reader, &entry)
		if err != nil {
			return nil, fmt.Errorf("decode contract spec entry %d: %w", len(entries), err)
		}
		if n <= 0 || reader.Len() >= before {
			return nil, errors.New("contract spec decoder made no progress")
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func decodeContractMetadata(data []byte) ([]ContractMetadataEntry, error) {
	reader := bytes.NewReader(data)
	entries := make([]ContractMetadataEntry, 0)
	for reader.Len() > 0 {
		var entry xdr.ScMetaEntry
		if _, err := xdr.Unmarshal(reader, &entry); err != nil {
			return nil, fmt.Errorf("decode contract metadata: %w", err)
		}
		if value, ok := entry.GetV0(); ok {
			entries = append(entries, ContractMetadataEntry{Key: value.Key, Value: value.Val})
		}
	}
	return entries, nil
}

func decodeContractEnvironment(data []byte) (ContractEnvironment, error) {
	reader := bytes.NewReader(data)
	var environment ContractEnvironment
	for reader.Len() > 0 {
		var entry xdr.ScEnvMetaEntry
		if _, err := xdr.Unmarshal(reader, &entry); err != nil {
			return environment, fmt.Errorf("decode contract environment metadata: %w", err)
		}
		if value, ok := entry.GetInterfaceVersion(); ok {
			environment.InterfaceVersion = &ContractEnvironmentVersion{
				Protocol:   uint32(value.Protocol),
				PreRelease: uint32(value.PreRelease),
			}
		}
	}
	return environment, nil
}

func contractSpecFromXDR(entries []xdr.ScSpecEntry) ContractSpec {
	spec := ContractSpec{
		Functions: []ContractSpecFunction{},
		Structs:   []ContractSpecStruct{},
		Unions:    []ContractSpecUnion{},
		Enums:     []ContractSpecEnum{},
		Errors:    []ContractSpecEnum{},
		Events:    []ContractSpecEvent{},
	}
	for _, entry := range entries {
		switch entry.Kind {
		case xdr.ScSpecEntryKindScSpecEntryFunctionV0:
			if value, ok := entry.GetFunctionV0(); ok {
				spec.Functions = append(spec.Functions, contractFunctionFromXDR(value))
			}
		case xdr.ScSpecEntryKindScSpecEntryUdtStructV0:
			if value, ok := entry.GetUdtStructV0(); ok {
				spec.Structs = append(spec.Structs, contractStructFromXDR(value))
			}
		case xdr.ScSpecEntryKindScSpecEntryUdtUnionV0:
			if value, ok := entry.GetUdtUnionV0(); ok {
				spec.Unions = append(spec.Unions, contractUnionFromXDR(value))
			}
		case xdr.ScSpecEntryKindScSpecEntryUdtEnumV0:
			if value, ok := entry.GetUdtEnumV0(); ok {
				spec.Enums = append(spec.Enums, contractEnumFromXDR(value.Name, value.Doc, value.Lib, value.Cases))
			}
		case xdr.ScSpecEntryKindScSpecEntryUdtErrorEnumV0:
			if value, ok := entry.GetUdtErrorEnumV0(); ok {
				cases := make([]ContractSpecEnumCase, 0, len(value.Cases))
				for _, c := range value.Cases {
					cases = append(cases, ContractSpecEnumCase{Name: c.Name, Value: uint32(c.Value), Doc: c.Doc})
				}
				spec.Errors = append(spec.Errors, ContractSpecEnum{Name: value.Name, Doc: value.Doc, Lib: value.Lib, Cases: cases})
			}
		case xdr.ScSpecEntryKindScSpecEntryEventV0:
			if value, ok := entry.GetEventV0(); ok {
				spec.Events = append(spec.Events, contractEventFromXDR(value))
			}
		}
	}
	return spec
}

func contractFunctionFromXDR(value xdr.ScSpecFunctionV0) ContractSpecFunction {
	function := ContractSpecFunction{Name: string(value.Name), Doc: value.Doc, Inputs: []ContractSpecField{}, Outputs: []string{}}
	for _, input := range value.Inputs {
		function.Inputs = append(function.Inputs, ContractSpecField{Name: input.Name, Type: contractTypeString(input.Type), Doc: input.Doc})
	}
	for _, output := range value.Outputs {
		function.Outputs = append(function.Outputs, contractTypeString(output))
	}
	return function
}

func contractStructFromXDR(value xdr.ScSpecUdtStructV0) ContractSpecStruct {
	result := ContractSpecStruct{Name: value.Name, Doc: value.Doc, Lib: value.Lib, Fields: []ContractSpecField{}}
	for _, field := range value.Fields {
		result.Fields = append(result.Fields, ContractSpecField{Name: field.Name, Type: contractTypeString(field.Type), Doc: field.Doc})
	}
	return result
}

func contractUnionFromXDR(value xdr.ScSpecUdtUnionV0) ContractSpecUnion {
	result := ContractSpecUnion{Name: value.Name, Doc: value.Doc, Lib: value.Lib, Cases: []ContractSpecUnionCase{}}
	for _, c := range value.Cases {
		item := ContractSpecUnionCase{Values: []string{}}
		switch c.Kind {
		case xdr.ScSpecUdtUnionCaseV0KindScSpecUdtUnionCaseVoidV0:
			if c.VoidCase != nil {
				item.Name, item.Doc = c.VoidCase.Name, c.VoidCase.Doc
			}
		case xdr.ScSpecUdtUnionCaseV0KindScSpecUdtUnionCaseTupleV0:
			if c.TupleCase != nil {
				item.Name, item.Doc = c.TupleCase.Name, c.TupleCase.Doc
				for _, valueType := range c.TupleCase.Type {
					item.Values = append(item.Values, contractTypeString(valueType))
				}
			}
		}
		result.Cases = append(result.Cases, item)
	}
	return result
}

func contractEnumFromXDR(name, doc, lib string, source []xdr.ScSpecUdtEnumCaseV0) ContractSpecEnum {
	result := ContractSpecEnum{Name: name, Doc: doc, Lib: lib, Cases: []ContractSpecEnumCase{}}
	for _, c := range source {
		result.Cases = append(result.Cases, ContractSpecEnumCase{Name: c.Name, Value: uint32(c.Value), Doc: c.Doc})
	}
	return result
}

func contractEventFromXDR(value xdr.ScSpecEventV0) ContractSpecEvent {
	result := ContractSpecEvent{Name: string(value.Name), Doc: value.Doc, Lib: value.Lib, PrefixTopics: []string{}, Params: []ContractSpecEventParam{}}
	for _, topic := range value.PrefixTopics {
		result.PrefixTopics = append(result.PrefixTopics, string(topic))
	}
	for _, param := range value.Params {
		location := "data"
		if param.Location == xdr.ScSpecEventParamLocationV0ScSpecEventParamLocationTopicList {
			location = "topic"
		}
		result.Params = append(result.Params, ContractSpecEventParam{Name: param.Name, Type: contractTypeString(param.Type), Location: location, Doc: param.Doc})
	}
	switch value.DataFormat {
	case xdr.ScSpecEventDataFormatScSpecEventDataFormatVec:
		result.DataFormat = "vec"
	case xdr.ScSpecEventDataFormatScSpecEventDataFormatMap:
		result.DataFormat = "map"
	default:
		result.DataFormat = "single_value"
	}
	return result
}

func contractTypeString(value xdr.ScSpecTypeDef) string {
	primitive := map[xdr.ScSpecType]string{
		xdr.ScSpecTypeScSpecTypeVal: "Val", xdr.ScSpecTypeScSpecTypeBool: "bool", xdr.ScSpecTypeScSpecTypeVoid: "Void",
		xdr.ScSpecTypeScSpecTypeError: "Error", xdr.ScSpecTypeScSpecTypeU32: "u32", xdr.ScSpecTypeScSpecTypeI32: "i32",
		xdr.ScSpecTypeScSpecTypeU64: "u64", xdr.ScSpecTypeScSpecTypeI64: "i64", xdr.ScSpecTypeScSpecTypeTimepoint: "Timepoint",
		xdr.ScSpecTypeScSpecTypeDuration: "Duration", xdr.ScSpecTypeScSpecTypeU128: "u128", xdr.ScSpecTypeScSpecTypeI128: "i128",
		xdr.ScSpecTypeScSpecTypeU256: "u256", xdr.ScSpecTypeScSpecTypeI256: "i256", xdr.ScSpecTypeScSpecTypeBytes: "Bytes",
		xdr.ScSpecTypeScSpecTypeString: "String", xdr.ScSpecTypeScSpecTypeSymbol: "Symbol", xdr.ScSpecTypeScSpecTypeAddress: "Address",
		xdr.ScSpecTypeScSpecTypeMuxedAddress: "MuxedAddress",
	}
	if result, ok := primitive[value.Type]; ok {
		return result
	}
	switch value.Type {
	case xdr.ScSpecTypeScSpecTypeOption:
		if value.Option != nil {
			return "Option<" + contractTypeString(value.Option.ValueType) + ">"
		}
	case xdr.ScSpecTypeScSpecTypeResult:
		if value.Result != nil {
			return "Result<" + contractTypeString(value.Result.OkType) + ", " + contractTypeString(value.Result.ErrorType) + ">"
		}
	case xdr.ScSpecTypeScSpecTypeVec:
		if value.Vec != nil {
			return "Vec<" + contractTypeString(value.Vec.ElementType) + ">"
		}
	case xdr.ScSpecTypeScSpecTypeMap:
		if value.Map != nil {
			return "Map<" + contractTypeString(value.Map.KeyType) + ", " + contractTypeString(value.Map.ValueType) + ">"
		}
	case xdr.ScSpecTypeScSpecTypeTuple:
		if value.Tuple != nil {
			items := make([]string, 0, len(value.Tuple.ValueTypes))
			for _, item := range value.Tuple.ValueTypes {
				items = append(items, contractTypeString(item))
			}
			return "(" + strings.Join(items, ", ") + ")"
		}
	case xdr.ScSpecTypeScSpecTypeBytesN:
		if value.BytesN != nil {
			return fmt.Sprintf("BytesN<%d>", uint32(value.BytesN.N))
		}
	case xdr.ScSpecTypeScSpecTypeUdt:
		if value.Udt != nil {
			return value.Udt.Name
		}
	}
	return "Unknown"
}

func RenderContractSpecRust(spec ContractSpec) string {
	var out strings.Builder
	for _, function := range spec.Functions {
		writeRustDoc(&out, function.Doc)
		inputs := make([]string, 0, len(function.Inputs))
		for _, input := range function.Inputs {
			inputs = append(inputs, input.Name+": "+input.Type)
		}
		fmt.Fprintf(&out, "fn %s(%s)", function.Name, strings.Join(inputs, ", "))
		switch len(function.Outputs) {
		case 0:
		case 1:
			fmt.Fprintf(&out, " -> %s", function.Outputs[0])
		default:
			fmt.Fprintf(&out, " -> (%s)", strings.Join(function.Outputs, ", "))
		}
		out.WriteString("\n\n")
	}
	for _, item := range spec.Structs {
		writeRustDoc(&out, item.Doc)
		fmt.Fprintf(&out, "#[contracttype]\nstruct %s {\n", item.Name)
		for _, field := range item.Fields {
			fmt.Fprintf(&out, "  %s: %s,\n", field.Name, field.Type)
		}
		out.WriteString("}\n\n")
	}
	for _, item := range spec.Unions {
		writeRustDoc(&out, item.Doc)
		fmt.Fprintf(&out, "#[contracttype]\nenum %s {\n", item.Name)
		for _, c := range item.Cases {
			writeRustDocIndented(&out, c.Doc, "  ")
			if len(c.Values) == 0 {
				fmt.Fprintf(&out, "  %s,\n", c.Name)
				continue
			}
			fmt.Fprintf(&out, "  %s(%s),\n", c.Name, strings.Join(c.Values, ", "))
		}
		out.WriteString("}\n\n")
	}
	for _, item := range spec.Enums {
		renderRustEnum(&out, item, "contracttype")
	}
	for _, item := range spec.Errors {
		renderRustEnum(&out, item, "contracterror")
	}
	for _, event := range spec.Events {
		renderRustEvent(&out, event)
	}
	return strings.TrimSpace(out.String()) + "\n"
}

func renderRustEvent(out *strings.Builder, event ContractSpecEvent) {
	writeRustDoc(out, event.Doc)
	attributes := make([]string, 0, 2)
	if len(event.PrefixTopics) > 0 {
		topics := make([]string, 0, len(event.PrefixTopics))
		for _, topic := range event.PrefixTopics {
			topics = append(topics, strconv.Quote(topic))
		}
		attributes = append(attributes, "topics = ["+strings.Join(topics, ", ")+"]")
	}
	if event.DataFormat != "" {
		dataFormat := strings.ReplaceAll(event.DataFormat, "_", "-")
		attributes = append(attributes, "data_format = "+strconv.Quote(dataFormat))
	}
	if len(attributes) == 0 {
		out.WriteString("#[contractevent]\n")
	} else {
		fmt.Fprintf(out, "#[contractevent(%s)]\n", strings.Join(attributes, ", "))
	}
	fmt.Fprintf(out, "struct %s {\n", event.Name)
	for _, param := range event.Params {
		writeRustDocIndented(out, param.Doc, "  ")
		if param.Location == "topic" {
			out.WriteString("  #[topic]\n")
		}
		fmt.Fprintf(out, "  %s: %s,\n", param.Name, param.Type)
	}
	out.WriteString("}\n\n")
}

func renderRustEnum(out *strings.Builder, item ContractSpecEnum, attribute string) {
	writeRustDoc(out, item.Doc)
	fmt.Fprintf(out, "#[%s]\nenum %s {\n", attribute, item.Name)
	for _, c := range item.Cases {
		fmt.Fprintf(out, "  %s = %d,\n", c.Name, c.Value)
	}
	out.WriteString("}\n\n")
}

func writeRustDoc(out *strings.Builder, doc string) {
	writeRustDocIndented(out, doc, "")
}

func writeRustDocIndented(out *strings.Builder, doc, indent string) {
	for _, line := range strings.Split(strings.TrimSpace(doc), "\n") {
		if line != "" {
			fmt.Fprintf(out, "%s/// %s\n", indent, line)
		}
	}
}
