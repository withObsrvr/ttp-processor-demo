{
  description = "Contract Data Processor - Hybrid service for Stellar smart contract data with gRPC control plane and Arrow Flight data plane";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        
        # Go module for the contract data processor
        contractDataProcessor = pkgs.buildGoModule {
          pname = "contract-data-processor";
          version = "1.0.0";
          
          src = ./.;
          
          # Use vendored dependencies
          vendorHash = null;
          modVendorDir = "./go/vendor";
          
          # Disable Go workspace mode for reproducible builds
          GOWORK = "off";
          
          buildFlags = [ "-mod=vendor" "-modcacherw" ];
          
          subPackages = [ "go" ];
          
          # Native build inputs
          nativeBuildInputs = with pkgs; [
            protobuf
            protoc-gen-go
            protoc-gen-go-grpc
          ];
          
          # Build inputs (Apache Arrow for CGO)
          buildInputs = with pkgs; [
            arrow-cpp
          ];
          
          # Enable CGO for Apache Arrow
          CGO_ENABLED = "1";
          
          # Proto generation and build preparation
          preBuild = ''
            echo "Generating protobuf files..."
            
            # Create output directory for generated files
            mkdir -p go/proto
            
            # Generate Go code from proto files
            protoc \
              --go_out=go \
              --go_opt=paths=source_relative \
              --go-grpc_out=go \
              --go-grpc_opt=paths=source_relative \
              proto/*.proto
            
            echo "Protobuf generation complete"
            
            # Add replace directive for local proto package
            cd go
            echo "" >> go.mod
            echo "replace github.com/withObsrvr/ttp-processor-demo/contract-data-processor/proto => ./proto" >> go.mod
            cd ..
          '';
          
          # Post install - ensure binary has correct name
          postInstall = ''
            mv $out/bin/go $out/bin/contract-data-processor
          '';
          
          meta = with pkgs.lib; {
            description = "Hybrid service processing Stellar contract data with Arrow Flight";
            license = licenses.mit;
            maintainers = [ ];
          };
        };
        
        # Docker image
        dockerImage = pkgs.dockerTools.buildImage {
          name = "contract-data-processor";
          tag = "latest";
          
          copyToRoot = pkgs.buildEnv {
            name = "image-root";
            paths = with pkgs; [
              contractDataProcessor
              bashInteractive
              coreutils
              tzdata
              cacert
              # Include Arrow runtime libraries
              arrow-cpp
            ];
            pathsToLink = [ "/bin" "/etc" "/lib" ];
          };
          
          config = {
            Entrypoint = [ "/bin/contract-data-processor" ];
            ExposedPorts = {
              "50054/tcp" = {};  # gRPC control plane
              "8816/tcp" = {};   # Arrow Flight data plane
              "8089/tcp" = {};   # HTTP health/metrics
            };
            Env = [
              "NETWORK_PASSPHRASE=Test SDF Network ; September 2015"
              "SOURCE_ENDPOINT=stellar-live-source-datalake:50053"
              "GRPC_ADDRESS=:50054"
              "FLIGHT_ADDRESS=:8816"
              "HEALTH_PORT=8089"
              "BATCH_SIZE=1000"
              "WORKER_COUNT=4"
            ];
            User = "1000:1000";
            WorkingDir = "/";
          };
        };
        
      in
      {
        # Packages
        packages = {
          default = contractDataProcessor;
          contract-data-processor = contractDataProcessor;
          docker = dockerImage;
        };
        
        # Apps (for nix run)
        apps = {
          default = {
            type = "app";
            program = "${contractDataProcessor}/bin/contract-data-processor";
          };
        };
        
        # Development shell
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            # Go development
            go_1_21
            gopls
            delve
            golangci-lint
            gotools
            
            # Protobuf
            protobuf
            protoc-gen-go
            protoc-gen-go-grpc
            
            # Apache Arrow
            arrow-cpp
            pkg-config
            
            # Build tools
            gnumake
            git
            
            # Development tools
            jq
            yq
            grpcurl
            grpcui
            
            # Container tools
            docker
            docker-compose
          ];
          
          # Environment variables
          GOWORK = "off";
          CGO_ENABLED = "1";
          PKG_CONFIG_PATH = "${pkgs.arrow-cpp}/lib/pkgconfig";
          
          # Shell hook
          shellHook = ''
            echo "Contract Data Processor Development Environment"
            echo "=============================================="
            echo ""
            echo "Available commands:"
            echo "  make gen-proto    - Generate protobuf files"
            echo "  make build        - Build the processor"
            echo "  make test         - Run tests"
            echo "  make run          - Run the processor"
            echo ""
            echo "Configuration:"
            echo "  GRPC_ADDRESS      - gRPC control plane address (default: :50054)"
            echo "  FLIGHT_ADDRESS    - Arrow Flight data plane address (default: :8816)"
            echo "  SOURCE_ENDPOINT   - Upstream data source (default: localhost:50053)"
            echo ""
            echo "Development tools:"
            echo "  grpcurl  - Test gRPC endpoints"
            echo "  grpcui   - Web UI for gRPC testing"
            echo "  delve    - Go debugger"
            echo ""
            
            # Set custom prompt
            export PS1="\[\033[1;34m\][contract-data-processor]\[\033[0m\] \w $ "
            
            # Ensure vendor directory exists
            if [ ! -d "go/vendor" ] && [ -f "go/go.mod" ]; then
              echo "Vendoring dependencies..."
              cd go && go mod vendor && cd ..
            fi
          '';
        };
      }
    );
}