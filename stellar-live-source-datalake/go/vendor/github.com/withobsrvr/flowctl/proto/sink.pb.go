// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.6
// 	protoc        v3.12.4
// source: proto/sink.proto

package flowctlpb

import (
	empty "github.com/golang/protobuf/ptypes/empty"
	_ "github.com/golang/protobuf/ptypes/timestamp"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// Sink acknowledgment
type SinkAck struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Success       bool                   `protobuf:"varint,1,opt,name=success,proto3" json:"success,omitempty"`
	Error         string                 `protobuf:"bytes,2,opt,name=error,proto3" json:"error,omitempty"`                                                                                 // Error message if success is false
	Metadata      map[string]string      `protobuf:"bytes,3,rep,name=metadata,proto3" json:"metadata,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"` // Additional acknowledgment metadata
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *SinkAck) Reset() {
	*x = SinkAck{}
	mi := &file_proto_sink_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SinkAck) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SinkAck) ProtoMessage() {}

func (x *SinkAck) ProtoReflect() protoreflect.Message {
	mi := &file_proto_sink_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SinkAck.ProtoReflect.Descriptor instead.
func (*SinkAck) Descriptor() ([]byte, []int) {
	return file_proto_sink_proto_rawDescGZIP(), []int{0}
}

func (x *SinkAck) GetSuccess() bool {
	if x != nil {
		return x.Success
	}
	return false
}

func (x *SinkAck) GetError() string {
	if x != nil {
		return x.Error
	}
	return ""
}

func (x *SinkAck) GetMetadata() map[string]string {
	if x != nil {
		return x.Metadata
	}
	return nil
}

var File_proto_sink_proto protoreflect.FileDescriptor

const file_proto_sink_proto_rawDesc = "" +
	"\n" +
	"\x10proto/sink.proto\x12\aflowctl\x1a\x1bgoogle/protobuf/empty.proto\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x12proto/common.proto\"\xb2\x01\n" +
	"\aSinkAck\x12\x18\n" +
	"\asuccess\x18\x01 \x01(\bR\asuccess\x12\x14\n" +
	"\x05error\x18\x02 \x01(\tR\x05error\x12:\n" +
	"\bmetadata\x18\x03 \x03(\v2\x1e.flowctl.SinkAck.MetadataEntryR\bmetadata\x1a;\n" +
	"\rMetadataEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\tR\x05value:\x028\x012\xa6\x01\n" +
	"\tEventSink\x12,\n" +
	"\x06Ingest\x12\x0e.flowctl.Event\x1a\x10.flowctl.SinkAck(\x01\x12-\n" +
	"\x04Ping\x12\x16.google.protobuf.Empty\x1a\r.flowctl.Pong\x12<\n" +
	"\bShutdown\x12\x18.flowctl.ShutdownRequest\x1a\x16.google.protobuf.EmptyB/Z-github.com/withobsrvr/flowctl/proto;flowctlpbb\x06proto3"

var (
	file_proto_sink_proto_rawDescOnce sync.Once
	file_proto_sink_proto_rawDescData []byte
)

func file_proto_sink_proto_rawDescGZIP() []byte {
	file_proto_sink_proto_rawDescOnce.Do(func() {
		file_proto_sink_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_proto_sink_proto_rawDesc), len(file_proto_sink_proto_rawDesc)))
	})
	return file_proto_sink_proto_rawDescData
}

var file_proto_sink_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_proto_sink_proto_goTypes = []any{
	(*SinkAck)(nil),         // 0: flowctl.SinkAck
	nil,                     // 1: flowctl.SinkAck.MetadataEntry
	(*Event)(nil),           // 2: flowctl.Event
	(*empty.Empty)(nil),     // 3: google.protobuf.Empty
	(*ShutdownRequest)(nil), // 4: flowctl.ShutdownRequest
	(*Pong)(nil),            // 5: flowctl.Pong
}
var file_proto_sink_proto_depIdxs = []int32{
	1, // 0: flowctl.SinkAck.metadata:type_name -> flowctl.SinkAck.MetadataEntry
	2, // 1: flowctl.EventSink.Ingest:input_type -> flowctl.Event
	3, // 2: flowctl.EventSink.Ping:input_type -> google.protobuf.Empty
	4, // 3: flowctl.EventSink.Shutdown:input_type -> flowctl.ShutdownRequest
	0, // 4: flowctl.EventSink.Ingest:output_type -> flowctl.SinkAck
	5, // 5: flowctl.EventSink.Ping:output_type -> flowctl.Pong
	3, // 6: flowctl.EventSink.Shutdown:output_type -> google.protobuf.Empty
	4, // [4:7] is the sub-list for method output_type
	1, // [1:4] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_proto_sink_proto_init() }
func file_proto_sink_proto_init() {
	if File_proto_sink_proto != nil {
		return
	}
	file_proto_common_proto_init()
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_proto_sink_proto_rawDesc), len(file_proto_sink_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_proto_sink_proto_goTypes,
		DependencyIndexes: file_proto_sink_proto_depIdxs,
		MessageInfos:      file_proto_sink_proto_msgTypes,
	}.Build()
	File_proto_sink_proto = out.File
	file_proto_sink_proto_goTypes = nil
	file_proto_sink_proto_depIdxs = nil
}
