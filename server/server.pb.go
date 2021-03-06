// Code generated by protoc-gen-go.
// source: server.proto
// DO NOT EDIT!

/*
Package server is a generated protocol buffer package.

It is generated from these files:
	server.proto

It has these top-level messages:
	Request
	Response
	Query
	Result
*/
package server

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

import (
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
const _ = proto.ProtoPackageIsVersion1

type Request struct {
	Dsn      string `protobuf:"bytes,1,opt,name=dsn" json:"dsn,omitempty"`
	Host     string `protobuf:"bytes,2,opt,name=host" json:"host,omitempty"`
	Port     uint32 `protobuf:"varint,3,opt,name=port" json:"port,omitempty"`
	Database string `protobuf:"bytes,4,opt,name=database" json:"database,omitempty"`
	User     string `protobuf:"bytes,5,opt,name=user" json:"user,omitempty"`
	Password string `protobuf:"bytes,6,opt,name=password" json:"password,omitempty"`
}

func (m *Request) Reset()                    { *m = Request{} }
func (m *Request) String() string            { return proto.CompactTextString(m) }
func (*Request) ProtoMessage()               {}
func (*Request) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

type Response struct {
	Code uint32 `protobuf:"varint,1,opt,name=code" json:"code,omitempty"`
	Msg  string `protobuf:"bytes,2,opt,name=msg" json:"msg,omitempty"`
}

func (m *Response) Reset()                    { *m = Response{} }
func (m *Response) String() string            { return proto.CompactTextString(m) }
func (*Response) ProtoMessage()               {}
func (*Response) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

type Query struct {
	Query string `protobuf:"bytes,1,opt,name=query" json:"query,omitempty"`
}

func (m *Query) Reset()                    { *m = Query{} }
func (m *Query) String() string            { return proto.CompactTextString(m) }
func (*Query) ProtoMessage()               {}
func (*Query) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{2} }

type Result struct {
	Result uint32 `protobuf:"varint,1,opt,name=result" json:"result,omitempty"`
}

func (m *Result) Reset()                    { *m = Result{} }
func (m *Result) String() string            { return proto.CompactTextString(m) }
func (*Result) ProtoMessage()               {}
func (*Result) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{3} }

func init() {
	proto.RegisterType((*Request)(nil), "server.Request")
	proto.RegisterType((*Response)(nil), "server.Response")
	proto.RegisterType((*Query)(nil), "server.Query")
	proto.RegisterType((*Result)(nil), "server.Result")
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// Client API for Neo5G service

type Neo5GClient interface {
	Connect(ctx context.Context, opts ...grpc.CallOption) (Neo5G_ConnectClient, error)
	Execute(ctx context.Context, opts ...grpc.CallOption) (Neo5G_ExecuteClient, error)
}

type neo5GClient struct {
	cc *grpc.ClientConn
}

func NewNeo5GClient(cc *grpc.ClientConn) Neo5GClient {
	return &neo5GClient{cc}
}

func (c *neo5GClient) Connect(ctx context.Context, opts ...grpc.CallOption) (Neo5G_ConnectClient, error) {
	stream, err := grpc.NewClientStream(ctx, &_Neo5G_serviceDesc.Streams[0], c.cc, "/server.neo5g/Connect", opts...)
	if err != nil {
		return nil, err
	}
	x := &neo5GConnectClient{stream}
	return x, nil
}

type Neo5G_ConnectClient interface {
	Send(*Request) error
	Recv() (*Response, error)
	grpc.ClientStream
}

type neo5GConnectClient struct {
	grpc.ClientStream
}

func (x *neo5GConnectClient) Send(m *Request) error {
	return x.ClientStream.SendMsg(m)
}

func (x *neo5GConnectClient) Recv() (*Response, error) {
	m := new(Response)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *neo5GClient) Execute(ctx context.Context, opts ...grpc.CallOption) (Neo5G_ExecuteClient, error) {
	stream, err := grpc.NewClientStream(ctx, &_Neo5G_serviceDesc.Streams[1], c.cc, "/server.neo5g/Execute", opts...)
	if err != nil {
		return nil, err
	}
	x := &neo5GExecuteClient{stream}
	return x, nil
}

type Neo5G_ExecuteClient interface {
	Send(*Query) error
	Recv() (*Result, error)
	grpc.ClientStream
}

type neo5GExecuteClient struct {
	grpc.ClientStream
}

func (x *neo5GExecuteClient) Send(m *Query) error {
	return x.ClientStream.SendMsg(m)
}

func (x *neo5GExecuteClient) Recv() (*Result, error) {
	m := new(Result)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// Server API for Neo5G service

type Neo5GServer interface {
	Connect(Neo5G_ConnectServer) error
	Execute(Neo5G_ExecuteServer) error
}

func RegisterNeo5GServer(s *grpc.Server, srv Neo5GServer) {
	s.RegisterService(&_Neo5G_serviceDesc, srv)
}

func _Neo5G_Connect_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(Neo5GServer).Connect(&neo5GConnectServer{stream})
}

type Neo5G_ConnectServer interface {
	Send(*Response) error
	Recv() (*Request, error)
	grpc.ServerStream
}

type neo5GConnectServer struct {
	grpc.ServerStream
}

func (x *neo5GConnectServer) Send(m *Response) error {
	return x.ServerStream.SendMsg(m)
}

func (x *neo5GConnectServer) Recv() (*Request, error) {
	m := new(Request)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _Neo5G_Execute_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(Neo5GServer).Execute(&neo5GExecuteServer{stream})
}

type Neo5G_ExecuteServer interface {
	Send(*Result) error
	Recv() (*Query, error)
	grpc.ServerStream
}

type neo5GExecuteServer struct {
	grpc.ServerStream
}

func (x *neo5GExecuteServer) Send(m *Result) error {
	return x.ServerStream.SendMsg(m)
}

func (x *neo5GExecuteServer) Recv() (*Query, error) {
	m := new(Query)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

var _Neo5G_serviceDesc = grpc.ServiceDesc{
	ServiceName: "server.neo5g",
	HandlerType: (*Neo5GServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Connect",
			Handler:       _Neo5G_Connect_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
		{
			StreamName:    "Execute",
			Handler:       _Neo5G_Execute_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
}

var fileDescriptor0 = []byte{
	// 272 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0x4c, 0x91, 0xdd, 0x4a, 0xfb, 0x40,
	0x10, 0xc5, 0xff, 0xf9, 0xb7, 0xd9, 0xc4, 0xc1, 0x6a, 0x19, 0x44, 0x96, 0x80, 0x50, 0xf6, 0xaa,
	0x57, 0xa1, 0x54, 0x7c, 0x01, 0xc5, 0x07, 0x30, 0x97, 0xde, 0xa5, 0xc9, 0x50, 0x41, 0xdd, 0x4d,
	0xf7, 0xc3, 0x8f, 0xa7, 0xf0, 0x95, 0xdd, 0xaf, 0xaa, 0x77, 0xbf, 0x73, 0x38, 0x3b, 0x73, 0x32,
	0x81, 0x53, 0x43, 0xfa, 0x8d, 0x74, 0x3b, 0x69, 0x65, 0x15, 0xb2, 0xa4, 0xc4, 0x57, 0x01, 0x55,
	0x47, 0x07, 0x47, 0xc6, 0xe2, 0x12, 0x66, 0xa3, 0x91, 0xbc, 0x58, 0x15, 0xeb, 0x93, 0x2e, 0x20,
	0x22, 0xcc, 0x9f, 0x94, 0xb1, 0xfc, 0x7f, 0xb4, 0x22, 0x07, 0x6f, 0x52, 0xda, 0xf2, 0x99, 0xf7,
	0x16, 0x5d, 0x64, 0x6c, 0xa0, 0x1e, 0x7b, 0xdb, 0xef, 0x7a, 0x43, 0x7c, 0x1e, 0xb3, 0x3f, 0x3a,
	0xe4, 0x9d, 0x5f, 0xc6, 0xcb, 0x34, 0x23, 0x70, 0xc8, 0x4f, 0xbd, 0x31, 0xef, 0x4a, 0x8f, 0x9c,
	0xa5, 0xfc, 0x51, 0x8b, 0x0d, 0xd4, 0x1d, 0x99, 0x49, 0xc9, 0xf4, 0x76, 0x50, 0x23, 0xc5, 0x4a,
	0x7e, 0x57, 0xe0, 0xd0, 0xf2, 0xd5, 0xec, 0x73, 0xa5, 0x80, 0xe2, 0x0a, 0xca, 0x07, 0x47, 0xfa,
	0x13, 0x2f, 0xa0, 0x3c, 0x04, 0xc8, 0x9f, 0x90, 0x84, 0x58, 0x01, 0xf3, 0x03, 0xdd, 0x8b, 0xc5,
	0x4b, 0x60, 0x3a, 0x52, 0x1e, 0x98, 0xd5, 0xf6, 0x19, 0x4a, 0x49, 0xea, 0x66, 0x8f, 0x5b, 0xa8,
	0xee, 0x94, 0x94, 0x34, 0x58, 0x3c, 0x6f, 0xf3, 0xbd, 0xf2, 0x75, 0x9a, 0xe5, 0xaf, 0x91, 0xda,
	0x89, 0x7f, 0xeb, 0x62, 0x53, 0x60, 0x0b, 0xd5, 0xfd, 0x07, 0x0d, 0xce, 0x12, 0x2e, 0x8e, 0x91,
	0x58, 0xa7, 0x39, 0xfb, 0xf3, 0xc2, 0xaf, 0x49, 0xf9, 0xdb, 0xfa, 0x31, 0xdf, 0x7e, 0xc7, 0xe2,
	0xaf, 0xb8, 0xfe, 0x0e, 0x00, 0x00, 0xff, 0xff, 0xcb, 0x10, 0x7b, 0x63, 0x9a, 0x01, 0x00, 0x00,
}
