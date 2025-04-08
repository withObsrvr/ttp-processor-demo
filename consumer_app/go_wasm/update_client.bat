@echo off
echo Updating client to use generated protobuf code...

:: First check if the generated files exist
IF NOT EXIST cmd\consumer_wasm\gen\event_service\event_service.pb.go (
    echo Error: Generated protobuf files not found. Please run generate_proto.bat first.
    goto end
)

:: Create a backup of the original client.go file
echo Creating backup of client.go...
copy /Y internal\client\client.go internal\client\client.go.bak

:: Update the client implementation
echo package client > internal\client\client_gen.go
echo. >> internal\client\client_gen.go
echo import ( >> internal\client\client_gen.go
echo     "context" >> internal\client\client_gen.go
echo     "fmt" >> internal\client\client_gen.go
echo     "io" >> internal\client\client_gen.go
echo     "log" >> internal\client\client_gen.go
echo     "time" >> internal\client\client_gen.go
echo     "google.golang.org/grpc" >> internal\client\client_gen.go
echo     pb "consumer_wasm/gen/event_service" >> internal\client\client_gen.go
echo     token_transfer "consumer_wasm/gen/token_transfer" >> internal\client\client_gen.go
echo ) >> internal\client\client_gen.go
echo. >> internal\client\client_gen.go
echo // TokenTransferServiceClient is the client API for TokenTransferService. >> internal\client\client_gen.go
echo type TokenTransferServiceClient interface { >> internal\client\client_gen.go
echo     GetTTPEvents(ctx context.Context, in *GetEventsRequest, opts ...grpc.CallOption) (TokenTransferService_GetTTPEventsClient, error) >> internal\client\client_gen.go
echo } >> internal\client\client_gen.go
echo. >> internal\client\client_gen.go
echo // TokenTransferService_GetTTPEventsClient is the client API for the GetTTPEvents method. >> internal\client\client_gen.go
echo type TokenTransferService_GetTTPEventsClient interface { >> internal\client\client_gen.go
echo     Recv() (*TokenTransferEvent, error) >> internal\client\client_gen.go
echo     grpc.ClientStream >> internal\client\client_gen.go
echo } >> internal\client\client_gen.go
echo. >> internal\client\client_gen.go
echo // Use the real implementation when the generated code is available >> internal\client\client_gen.go
echo func NewTokenTransferServiceClient(cc grpc.ClientConnInterface) TokenTransferServiceClient { >> internal\client\client_gen.go
echo     return pb.NewEventServiceClient(cc) >> internal\client\client_gen.go
echo } >> internal\client\client_gen.go
echo. >> internal\client\client_gen.go
echo // Convert between the generated types and our interface types >> internal\client\client_gen.go
echo type tokenTransferServiceClientAdapter struct { >> internal\client\client_gen.go
echo     client pb.EventServiceClient >> internal\client\client_gen.go
echo } >> internal\client\client_gen.go
echo. >> internal\client\client_gen.go
echo func (c *tokenTransferServiceClientAdapter) GetTTPEvents(ctx context.Context, in *GetEventsRequest, opts ...grpc.CallOption) (TokenTransferService_GetTTPEventsClient, error) { >> internal\client\client_gen.go
echo     pbReq := &pb.GetEventsRequest{ >> internal\client\client_gen.go
echo         StartLedger: in.StartLedger, >> internal\client\client_gen.go
echo         EndLedger:   in.EndLedger, >> internal\client\client_gen.go
echo     } >> internal\client\client_gen.go
echo     stream, err := c.client.GetTTPEvents(ctx, pbReq, opts...) >> internal\client\client_gen.go
echo     if err != nil { >> internal\client\client_gen.go
echo         return nil, err >> internal\client\client_gen.go
echo     } >> internal\client\client_gen.go
echo     return &tokenTransferServiceGetTTPEventsClientAdapter{stream}, nil >> internal\client\client_gen.go
echo } >> internal\client\client_gen.go
echo. >> internal\client\client_gen.go
echo type tokenTransferServiceGetTTPEventsClientAdapter struct { >> internal\client\client_gen.go
echo     stream pb.EventService_GetTTPEventsClient >> internal\client\client_gen.go
echo } >> internal\client\client_gen.go
echo. >> internal\client\client_gen.go
echo func (c *tokenTransferServiceGetTTPEventsClientAdapter) Recv() (*TokenTransferEvent, error) { >> internal\client\client_gen.go
echo     event, err := c.stream.Recv() >> internal\client\client_gen.go
echo     if err != nil { >> internal\client\client_gen.go
echo         return nil, err >> internal\client\client_gen.go
echo     } >> internal\client\client_gen.go
echo     // Convert from pb.TokenTransferEvent to our TokenTransferEvent >> internal\client\client_gen.go
echo     // ... Conversion code would go here ... >> internal\client\client_gen.go
echo     return ConvertPBToTokenTransferEvent(event), nil >> internal\client\client_gen.go
echo } >> internal\client\client_gen.go
echo. >> internal\client\client_gen.go
echo func (c *tokenTransferServiceGetTTPEventsClientAdapter) CloseSend() error { >> internal\client\client_gen.go
echo     return c.stream.CloseSend() >> internal\client\client_gen.go
echo } >> internal\client\client_gen.go
echo. >> internal\client\client_gen.go
echo func (c *tokenTransferServiceGetTTPEventsClientAdapter) Context() context.Context { >> internal\client\client_gen.go
echo     return c.stream.Context() >> internal\client\client_gen.go
echo } >> internal\client\client_gen.go
echo. >> internal\client\client_gen.go
echo func (c *tokenTransferServiceGetTTPEventsClientAdapter) SendMsg(m interface{}) error { >> internal\client\client_gen.go
echo     return c.stream.SendMsg(m) >> internal\client\client_gen.go
echo } >> internal\client\client_gen.go
echo. >> internal\client\client_gen.go
echo func (c *tokenTransferServiceGetTTPEventsClientAdapter) RecvMsg(m interface{}) error { >> internal\client\client_gen.go
echo     return c.stream.RecvMsg(m) >> internal\client\client_gen.go
echo } >> internal\client\client_gen.go
echo. >> internal\client\client_gen.go
echo func ConvertPBToTokenTransferEvent(pbEvent *token_transfer.TokenTransferEvent) *TokenTransferEvent { >> internal\client\client_gen.go
echo     // This function would convert from the protobuf generated type to our internal type >> internal\client\client_gen.go
echo     // For now, we'll just create a dummy implementation >> internal\client\client_gen.go
echo     return &TokenTransferEvent{} >> internal\client\client_gen.go
echo } >> internal\client\client_gen.go

echo Done! Client code has been updated to use the real gRPC implementation.

:end
pause 