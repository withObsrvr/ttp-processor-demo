# Flow Proto Repository Update Plan

## Overview

The flow-proto repository should be updated to include all the necessary proto definitions required by the newly-created flowctl-sdk. This document outlines the recommended updates and the integration strategy.

## Updates Needed

1. **Control Plane Definitions**

   Create a new `controlplane` directory with these proto files:
   
   ```protobuf
   // controlplane/controlplane.proto
   syntax = "proto3";

   package obsrvr.flow.controlplane;
   
   import "google/protobuf/timestamp.proto";
   
   option go_package = "github.com/withObsrvr/flow-proto/proto/controlplane";
   
   service ControlPlane {
     // Processor registration and heartbeat
     rpc RegisterProcessor(RegisterProcessorRequest) returns (RegisterProcessorResponse);
     rpc ProcessorHeartbeat(ProcessorHeartbeatRequest) returns (ProcessorHeartbeatResponse);
     
     // Source registration and heartbeat
     rpc RegisterSource(RegisterSourceRequest) returns (RegisterSourceResponse);
     rpc SourceHeartbeat(SourceHeartbeatRequest) returns (SourceHeartbeatResponse);
     
     // Consumer registration and heartbeat
     rpc RegisterConsumer(RegisterConsumerRequest) returns (RegisterConsumerResponse);
     rpc ConsumerHeartbeat(ConsumerHeartbeatRequest) returns (ConsumerHeartbeatResponse);
   }
   
   // Processor messages
   message RegisterProcessorRequest {
     string processor_id = 1;
     string endpoint = 2;
     map<string, string> metadata = 3;
   }
   
   message RegisterProcessorResponse {
     string processor_id = 1;
     bool success = 2;
     string message = 3;
   }
   
   message ProcessorHeartbeatRequest {
     string processor_id = 1;
     google.protobuf.Timestamp timestamp = 2;
     map<string, double> metrics = 3;
     string status = 4;
   }
   
   message ProcessorHeartbeatResponse {
     bool success = 1;
     string message = 2;
   }
   
   // Source messages
   message RegisterSourceRequest {
     string source_id = 1;
     string endpoint = 2;
     map<string, string> metadata = 3;
   }
   
   message RegisterSourceResponse {
     string source_id = 1;
     bool success = 2;
     string message = 3;
   }
   
   message SourceHeartbeatRequest {
     string source_id = 1;
     google.protobuf.Timestamp timestamp = 2;
     map<string, double> metrics = 3;
     string status = 4;
   }
   
   message SourceHeartbeatResponse {
     bool success = 1;
     string message = 2;
   }
   
   // Consumer messages
   message RegisterConsumerRequest {
     string consumer_id = 1;
     string endpoint = 2;
     map<string, string> metadata = 3;
   }
   
   message RegisterConsumerResponse {
     string consumer_id = 1;
     bool success = 2;
     string message = 3;
   }
   
   message ConsumerHeartbeatRequest {
     string consumer_id = 1;
     google.protobuf.Timestamp timestamp = 2;
     map<string, double> metrics = 3;
     string status = 4;
   }
   
   message ConsumerHeartbeatResponse {
     bool success = 1;
     string message = 2;
   }
   ```

2. **Updated Processor Definitions**

   Update the processor definitions to ensure they match the implementation in ttp-processor and other processors:
   
   ```protobuf
   // processor/processor.proto
   syntax = "proto3";

   package obsrvr.flow.processor;
   
   option go_package = "github.com/withObsrvr/flow-proto/proto/processor";
   
   service ProcessorService {
     // Process data in a streaming fashion
     rpc Process(stream DataMessage) returns (stream DataMessage);
     
     // Get processor capabilities
     rpc GetCapabilities(CapabilitiesRequest) returns (CapabilitiesResponse);
   }
   
   message DataMessage {
     bytes payload = 1;
     map<string, string> metadata = 2;
   }
   
   message CapabilitiesRequest {
     // Empty request
   }
   
   message CapabilitiesResponse {
     string processor_id = 1;
     string name = 2;
     string description = 3;
     string version = 4;
     repeated string input_event_types = 5;
     repeated string output_event_types = 6;
     int32 max_concurrent = 7;
   }
   ```

3. **Common Definitions**

   Create common message types that can be shared across services:
   
   ```protobuf
   // common/common.proto
   syntax = "proto3";

   package obsrvr.flow.common;
   
   option go_package = "github.com/withObsrvr/flow-proto/proto/common";
   
   message Metrics {
     map<string, double> values = 1;
   }
   
   enum HealthStatus {
     UNKNOWN = 0;
     STARTING = 1;
     HEALTHY = 2;
     UNHEALTHY = 3;
     STOPPING = 4;
   }
   ```

## Integration Strategy

1. **Update Repository**

   - Create pull request with the updated proto files
   - Ensure backward compatibility with existing code
   - Update Go code generation scripts
   - Update documentation

2. **SDK Integration**

   - Update flowctl-sdk to use the official flow-proto repository
   - Replace the mocked protobuf implementations with the actual imports
   - Ensure compatibility between the SDK and the proto definitions

3. **Testing**

   - Test the integration between flowctl, flowctl-sdk, and flow-proto
   - Ensure all services can communicate properly
   - Validate metrics reporting and control plane integration

## Versioning and Releases

1. **Flow Proto Package**
   
   - Update to v0.2.0 to reflect the significant additions
   - Tag a new release after changes are merged
   - Document changes in release notes

2. **Flowctl SDK Package**

   - Release v0.1.0 as the initial SDK version
   - Ensure it depends on the latest flow-proto package

## Migration Plan for Existing Services

1. **Stellar Live Source and Datalake**

   - Update imports to use the official flow-proto package
   - Replace custom implementations with SDK components
   - Maintain backward compatibility with existing systems

2. **TTP Processor**

   - Migrate to using the SDK for flowctl integration
   - Keep the core processing logic unchanged
   - Leverage the standardized metrics and health checks

3. **Consumer App**

   - Update the TypeScript components to match the new proto definitions
   - Generate updated client code using the new protos
   - Migrate to a consistent approach for metrics and health reporting

## Future Enhancements

1. **Configuration Updates**
   
   - Add support for dynamic configuration updates from flowctl
   - Implement config validation and versioning

2. **Service Discovery**

   - Add service discovery capabilities to the control plane
   - Enable processors to discover sources and sinks

3. **Pipeline Management**

   - Add pipeline definitions to manage flow between components
   - Enable dynamic routing of events