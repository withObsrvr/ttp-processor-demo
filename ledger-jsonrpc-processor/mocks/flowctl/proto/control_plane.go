package proto

import (
	"context"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// ServiceType represents the type of service being registered
type ServiceType int32

const (
	ServiceType_SERVICE_TYPE_UNSPECIFIED ServiceType = 0
	ServiceType_SERVICE_TYPE_SOURCE      ServiceType = 1
	ServiceType_SERVICE_TYPE_PROCESSOR   ServiceType = 2
	ServiceType_SERVICE_TYPE_SINK        ServiceType = 3
)

// ServiceInfo contains information about a service registering with flowctl
type ServiceInfo struct {
	ServiceType      ServiceType
	InputEventTypes  []string
	OutputEventTypes []string
	HealthEndpoint   string
	MaxInflight      int32
	Metadata         map[string]string
}

// ServiceAck is the acknowledgment from a successful registration
type ServiceAck struct {
	ServiceId      string
	TopicNames     []string
	ConnectionInfo map[string]string
}

// ServiceHeartbeat is sent periodically to report service health
type ServiceHeartbeat struct {
	ServiceId string
	Timestamp *timestamppb.Timestamp
	Metrics   map[string]float64
}

// ControlPlaneClient is the client for the flowctl control plane
type ControlPlaneClient interface {
	Register(ctx context.Context, in *ServiceInfo, opts ...interface{}) (*ServiceAck, error)
	Heartbeat(ctx context.Context, in *ServiceHeartbeat, opts ...interface{}) (interface{}, error)
}

// Mock implementation of ControlPlaneClient for development
type mockControlPlaneClient struct{}

func NewControlPlaneClient(conn interface{}) ControlPlaneClient {
	return &mockControlPlaneClient{}
}

func (m *mockControlPlaneClient) Register(ctx context.Context, in *ServiceInfo, opts ...interface{}) (*ServiceAck, error) {
	return &ServiceAck{
		ServiceId:      "mock-service-id",
		TopicNames:     []string{"mock-topic"},
		ConnectionInfo: map[string]string{"broker": "mock-broker:9092"},
	}, nil
}

func (m *mockControlPlaneClient) Heartbeat(ctx context.Context, in *ServiceHeartbeat, opts ...interface{}) (interface{}, error) {
	return nil, nil
}