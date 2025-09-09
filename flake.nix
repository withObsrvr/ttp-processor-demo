{
  description = "TTP Processor Demo - Microservices architecture for processing Token Transfer Protocol events from Stellar blockchain";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        
        # Node.js version for consumer apps
        nodejs = pkgs.nodejs_22;
      in
      {
        # Development shell for the entire workspace
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            # Go development
            go_1_23
            gopls
            delve
            gotools
            go-tools
            
            # Protocol buffers
            protobuf
            protoc-gen-go
            protoc-gen-go-grpc
            
            # Node.js for consumer apps
            nodejs
            nodejs.pkgs.typescript
            nodejs.pkgs.npm
            
            # Build tools
            gnumake
            git
            jq
            yq
            
            # Container tools
            docker
            docker-compose
            
            # Apache Arrow for data processing
            arrow-cpp
            
            # Development utilities
            ripgrep
            fd
            tree
            watch
            curl
            wget
            
            # Database tools (for testing/development)
            postgresql_16
            
            # Monitoring tools
            prometheus
            grafana
          ];
          
          # Environment variables from .envrc
          shellHook = ''
            # Set custom prompt to indicate Nix environment
            export PS1="\[\033[1;35m\][nix:ttp-demo]\[\033[0m\] \[\033[1;34m\]\w\[\033[0m\] \[\033[1;36m\]\$\[\033[0m\] "
            
            echo "🚀 TTP Processor Demo Development Environment"
            echo "================================================"
            echo "Go version: $(go version)"
            echo "Node.js version: $(node --version)"
            echo "Protoc version: $(protoc --version)"
            echo ""
            
            # Set up Go workspace
            export GOWORK=auto
            export GO111MODULE=on
            
            # Load environment variables as specified in .envrc
            export ENABLE_FLOWCTL=true
            export ENABLE_UNIFIED_EVENTS=true
            export HEALTH_PORT=8088
            export NETWORK_PASSPHRASE="Test SDF Network ; September 2015"
            export SOURCE_SERVICE_ADDRESS=localhost:50052
            
            # Load .env file if it exists
            if [ -f .env ]; then
              echo "Loading .env file..."
              set -a
              source .env
              set +a
            fi
            
            # Load .secrets file if it exists
            if [ -f .secrets ]; then
              echo "Loading .secrets file..."
              set -a
              source .secrets
              set +a
            fi
            
            echo ""
            echo "Available services:"
            echo "  - stellar-live-source: RPC-based ledger data source"
            echo "  - stellar-live-source-datalake: Storage-based ledger data source"
            echo "  - stellar-arrow-source: Arrow-based data source"
            echo "  - ttp-processor: Processes raw ledgers into TTP events"
            echo "  - contract-invocation-processor: Processes contract invocations"
            echo "  - contract-data-processor: Hybrid Arrow Flight/gRPC service"
            echo "  - arrow-consumer-demo: Demo consumer for Arrow data"
            echo ""
            echo "Common commands:"
            echo "  make build     - Build services (run in service directory)"
            echo "  make gen-proto - Generate protobuf code"
            echo "  make run       - Run the service"
            echo "  go test ./...  - Run tests"
            echo ""
            echo "Service directories:"
            for service in stellar-live-source stellar-live-source-datalake stellar-arrow-source ttp-processor contract-invocation-processor contract-data-processor arrow-consumer-demo; do
              if [ -d "$service" ]; then
                echo "  - $service/"
              fi
            done
            echo ""
            echo "Consumer app directories:"
            echo "  - consumer_app/node/"
            echo "  - consumer_app/go_wasm/"
            echo "  - consumer_app/rust_wasm/"
            echo ""
            
            # Helper functions
            build-all() {
              echo "Building all Go services..."
              for dir in stellar-live-source stellar-live-source-datalake stellar-arrow-source ttp-processor contract-invocation-processor contract-data-processor arrow-consumer-demo; do
                if [ -d "$dir/go" ] || [ -d "$dir" ]; then
                  echo "Building $dir..."
                  (cd $dir && make build) || echo "Failed to build $dir"
                fi
              done
            }
            
            gen-all-protos() {
              echo "Generating all protobuf files..."
              for dir in stellar-live-source stellar-live-source-datalake stellar-arrow-source ttp-processor contract-invocation-processor contract-data-processor; do
                if [ -f "$dir/Makefile" ] && grep -q "gen-proto" "$dir/Makefile"; then
                  echo "Generating protos for $dir..."
                  (cd $dir && make gen-proto) || echo "Failed to generate protos for $dir"
                fi
              done
            }
            
            echo "Helper functions available:"
            echo "  build-all       - Build all Go services"
            echo "  gen-all-protos  - Generate all protobuf files"
            echo ""
            echo "Environment ready! Happy coding! 🎉"
          '';
          
          # Additional environment variables
          PROTOC = "${pkgs.protobuf}/bin/protoc";
          PROTOC_GEN_GO = "${pkgs.protoc-gen-go}/bin/protoc-gen-go";
          PROTOC_GEN_GO_GRPC = "${pkgs.protoc-gen-go-grpc}/bin/protoc-gen-go-grpc";
        };
        
        # Alternative minimal shell for CI/automated builds
        devShells.ci = pkgs.mkShell {
          buildInputs = with pkgs; [
            go_1_23
            protobuf
            protoc-gen-go
            protoc-gen-go-grpc
            gnumake
            git
          ];
          
          shellHook = ''
            export GOWORK=auto
            export GO111MODULE=on
          '';
        };
      }
    );
}