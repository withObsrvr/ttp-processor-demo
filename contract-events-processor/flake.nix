{
  description = "Contract Events Processor - Extracts and filters Soroban contract events from Stellar ledgers";

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
          default = pkgs.buildGoModule rec {
            pname = "contract-events-processor";
            version = "0.1.0";
            src = ./.;

            # Vendor hash for reproducible builds
            # Update this after first build attempt
            vendorHash = "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

            # Set the Go module directory
            modRoot = "./go";

            # Disable go workspace mode
            env = {
              GOWORK = "off";
            };

            preBuild = ''
              echo "Setting up build environment for contract-events-processor..."

              # Generate proto files
              echo "Generating protobuf code..."
              mkdir -p go/gen/contract_event_service
              mkdir -p go/gen/raw_ledger_service

              # Copy raw_ledger_service proto from stellar-live-source
              if [ -f "../stellar-live-source/protos/raw_ledger_service/raw_ledger_service.proto" ]; then
                cp ../stellar-live-source/protos/raw_ledger_service/raw_ledger_service.proto protos/raw_ledger_service.proto
              elif [ -f "protos/raw_ledger_service.proto" ]; then
                echo "Using existing raw_ledger_service.proto"
              else
                echo "ERROR: raw_ledger_service.proto not found"
                exit 1
              fi

              # Generate contract_event_service protos
              ${pkgs.protobuf}/bin/protoc \
                --proto_path=protos \
                --plugin=protoc-gen-go=${pkgs.protoc-gen-go}/bin/protoc-gen-go \
                --go_out=go/gen/contract_event_service \
                --go_opt=paths=source_relative \
                --plugin=protoc-gen-go-grpc=${pkgs.protoc-gen-go-grpc}/bin/protoc-gen-go-grpc \
                --go-grpc_out=go/gen/contract_event_service \
                --go-grpc_opt=paths=source_relative \
                --experimental_allow_proto3_optional \
                contract_event_service.proto

              # Generate raw_ledger_service protos
              ${pkgs.protobuf}/bin/protoc \
                --proto_path=protos \
                --plugin=protoc-gen-go=${pkgs.protoc-gen-go}/bin/protoc-gen-go \
                --go_out=go/gen/raw_ledger_service \
                --go_opt=paths=source_relative \
                --plugin=protoc-gen-go-grpc=${pkgs.protoc-gen-go-grpc}/bin/protoc-gen-go-grpc \
                --go-grpc_out=go/gen/raw_ledger_service \
                --go-grpc_opt=paths=source_relative \
                raw_ledger_service.proto

              # Create go.mod files for generated packages
              cat > go/gen/contract_event_service/go.mod <<EOF
              module github.com/withObsrvr/contract-events-processor/gen/contract_event_service
              go 1.24
              require (
                google.golang.org/grpc v1.72.0
                google.golang.org/protobuf v1.36.6
              )
              EOF

              cat > go/gen/raw_ledger_service/go.mod <<EOF
              module github.com/withObsrvr/contract-events-processor/gen/raw_ledger_service
              go 1.24
              require (
                google.golang.org/grpc v1.72.0
                google.golang.org/protobuf v1.36.6
              )
              EOF

              echo "Proto generation completed"
            '';

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
            name = "withobsrvr/contract-events-processor";
            tag = "latest";

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

            config = {
              Entrypoint = [ "/bin/contract-events-processor" ];
              ExposedPorts = {
                "50053/tcp" = {};  # gRPC port
                "8089/tcp" = {};   # Health check port
              };
              Env = [
                "PATH=/bin"
                "PORT=:50053"
                "HEALTH_PORT=8089"
                "LIVE_SOURCE_ADDRESS=localhost:50052"
                "NETWORK_PASSPHRASE=Test SDF Network ; September 2015"
              ];
              WorkingDir = "/";
              User = "1000:1000";
            };
          };
        };

        # Development shell
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

          shellHook = ''
            export PS1="\[\033[1;32m\][nix:contract-events]\[\033[0m\] \[\033[1;34m\]\w\[\033[0m\] \[\033[1;36m\]\$\[\033[0m\] "
            echo "🔄 Contract Events Processor Development Environment"
            echo "Go version: $(go version)"

            # Disable Go workspace mode
            export GOWORK=off
            export GO111MODULE="on"

            # Vendor dependencies if needed
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
            echo "  make build              - Build the binary"
            echo "  nix build               - Build with Nix"
            echo "  nix build .#docker      - Build Docker image"
            echo "  nix run                 - Run the binary"
          '';
        };

        # App for 'nix run'
        apps.default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/contract-events-processor";
        };
      }
    );
}
