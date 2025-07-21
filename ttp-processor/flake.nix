{
  description = "TTP Processor - Service for processing Token Transfer Protocol events from Stellar blockchain";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        packages = {
          default = pkgs.buildGoModule {
            pname = "ttp-processor";
            version = "0.1.0";
            src = ./.;
            
            # Use vendored dependencies for improved build reliability
            vendorHash = null;
            modVendorDir = "./go/vendor";

            # Skip the go module verification
            allowVendorCheck = false;

            # Add more Go flags to bypass module checks
            buildFlags = ["-mod=vendor" "-modcacherw"];

            # Set environment variables for go builds
            env = {
              GOPROXY = "off";
            };
            
            # Customize Go build to work with our project structure
            preBuild = ''
              # Make sure vendor directory is properly set up before build
              if [ -d "go/vendor" ]; then
                echo "Using existing vendor directory"
              else
                echo "No vendor directory found, this will likely fail"
              fi
              
              # Generate protobuf files
              echo "Generating protobuf files..."
              
              # First create the output directories
              mkdir -p go/gen/event_service
              mkdir -p go/gen/raw_ledger_service
              mkdir -p go/gen/ingest/asset
              mkdir -p go/gen/ingest/processors/token_transfer
              
              # Run protoc to generate Go code
              protoc \
                --proto_path=./protos \
                --go_out=./go/gen \
                --go_opt=paths=source_relative \
                --go-grpc_out=./go/gen \
                --go-grpc_opt=paths=source_relative \
                --experimental_allow_proto3_optional \
                event_service/event_service.proto \
                ingest/processors/token_transfer/token_transfer_event.proto \
                ingest/asset/asset.proto \
                raw_ledger_service/raw_ledger_service.proto
                
              echo "Updating go.mod with replace directives..."
              cd go
              echo 'replace github.com/withObsrvr/ttp-processor/gen/event_service => ./gen/event_service' >> go.mod
              echo 'replace github.com/withObsrvr/ttp-processor/server => ./server' >> go.mod
              GOWORK=off go mod tidy
            '';
            
            buildPhase = ''
              runHook preBuild
              # Disable go workspace mode
              export GOWORK=off
              
              cd go
              # Build using vendored deps
              if [ -d "vendor" ]; then
                go build -mod=vendor -o ../ttp_processor_server main.go
              else
                go build -mod=vendor -o ../ttp_processor_server main.go
              fi
              runHook postBuild
            '';

            installPhase = ''
              runHook preInstall
              mkdir -p $out/bin
              cp ttp_processor_server $out/bin/
              chmod +x $out/bin/ttp_processor_server
              runHook postInstall
            '';
            
            # Add any native build dependencies
            nativeBuildInputs = [ 
              pkgs.go 
              pkgs.protobuf
              pkgs.protoc-gen-go
              pkgs.protoc-gen-go-grpc
              pkgs.gnumake
            ];
          };
          
          # Docker image
          docker = pkgs.dockerTools.buildImage {
            name = "ttp-processor";
            tag = "latest";
            
            # Use the binary from the default package
            copyToRoot = pkgs.buildEnv {
              name = "image-root";
              paths = [
                self.packages.${system}.default
                pkgs.bash
                pkgs.coreutils
                pkgs.tzdata
                pkgs.cacert
              ];
              pathsToLink = [ "/bin" "/etc" "/share" ];
            };
            
            # Configuration
            config = {
              Entrypoint = [ "/bin/ttp_processor_server" ];
              ExposedPorts = {
                "50051/tcp" = {};
                "8088/tcp" = {};
              };
              Env = [
                "LIVE_SOURCE_ENDPOINT=localhost:50052"
                "NETWORK_PASSPHRASE=Test SDF Network ; September 2015"
                "PATH=/bin"
              ];
              WorkingDir = "/";
              User = "1000:1000";
            };
          };
        };

        # Development shell for working on the project
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [ 
            go
            gopls
            delve
            protobuf
            protoc-gen-go
            protoc-gen-go-grpc
            git
            gnumake
            docker
          ];
          
          # Shell setup for development environment
          shellHook = ''
            # Set custom prompt
            export PS1="\[\033[1;32m\][nix:ttp-processor]\[\033[0m\] \[\033[1;34m\]\w\[\033[0m\] \[\033[1;36m\]\$\[\033[0m\] "
            echo "🚀 TTP Processor Development Environment"
            echo "Go version: $(go version)"
            
            # Disable Go workspace mode
            export GOWORK=off
            export GO111MODULE="on"
            
            # Helper to vendor dependencies - improves build reliability
            if [ ! -d go/vendor ]; then
              echo "Vendoring dependencies..."
              cd go
              GOWORK=off go mod tidy
              GOWORK=off go mod vendor
              cd ..
            fi
            
            echo "Development environment ready!"
            echo ""
            echo "Available commands:"
            echo "  make build-processor   - Build the binary"
            echo "  make gen-proto         - Generate protobuf code"
            echo "  make run               - Run the processor"
            echo "  make clean             - Clean build artifacts"
            echo ""
            echo "Nix commands:"
            echo "  nix build              - Build with Nix"
            echo "  nix run                - Run the binary"
            echo "  nix develop            - Enter development shell"
          '';
        };
        
        # App for 'nix run'
        apps.default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/ttp_processor_server";
        };
      }
    );
}