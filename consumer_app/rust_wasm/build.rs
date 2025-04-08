fn main() {
    // Temporarily disabled protobuf compilation until we resolve dependencies
    println!("cargo:warning=Proto compilation disabled for test build");
    
    // Uncomment when ready to use protobuf again
    /*
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .out_dir("src/proto")
        .compile(
            &[
                "proto/event_service/event_service.proto",
                "proto/ingest/processors/token_transfer/token_transfer_event.proto",
                "proto/ingest/asset/asset.proto",
            ],
            &["proto"],
        ).expect("Failed to compile protos");
    
    println!("cargo:rerun-if-changed=proto/");
    */
} 