{
  description = "Stellar Live Source Datalake - Service for processing data from the Stellar blockchain";

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
            pname = "stellar-live-source-datalake";
            version = "0.1.0";
            src = ./.;
            
            # Use vendored dependencies for improved build reliability
            vendorHash = null;
            modVendorDir = "./go/vendor"; # Point explicitly to the vendor dir

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
              
              # First create the output directory
              mkdir -p go/gen/raw_ledger_service
              
              # Run protoc directly
              protoc \
                --proto_path=./protos \
                --go_out=./go/gen \
                --go_opt=paths=source_relative \
                --go_opt=Mraw_ledger_service/raw_ledger_service.proto=github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/gen/raw_ledger_service \
                --go-grpc_out=./go/gen \
                --go-grpc_opt=paths=source_relative \
                --go-grpc_opt=Mraw_ledger_service/raw_ledger_service.proto=github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/gen/raw_ledger_service \
                ./protos/raw_ledger_service/raw_ledger_service.proto
                
              echo "Updating go.mod with replace directives..."
              cd go
              echo 'replace github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/gen/raw_ledger_service => ./gen/raw_ledger_service' >> go.mod
              echo 'replace github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/server => ./server' >> go.mod
              GOWORK=off go mod tidy
            '';
            
            buildPhase = ''
              runHook preBuild
              # Disable go workspace mode
              export GOWORK=off
              
              
              # Build using vendored deps if available
              if [ -d "vendor" ]; then
                go build -mod=vendor -o ../stellar_live_source_datalake main.go
              else
                go build -mod=vendor -o ../stellar_live_source_datalake main.go
              fi
              runHook postBuild
            '';

            installPhase = ''
              runHook preInstall
              mkdir -p $out/bin
              cp ../stellar_live_source_datalake $out/bin/
              chmod +x $out/bin/stellar_live_source_datalake
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
            name = "stellar-live-source-datalake";
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
              Entrypoint = [ "/bin/stellar_live_source_datalake" ];
              ExposedPorts = {
                "50052/tcp" = {};
                "8088/tcp" = {};
              };
              Env = [
                "STORAGE_TYPE=FS"
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
            export PS1="\[\033[1;32m\][nix:stellar-live-datalake]\[\033[0m\] \[\033[1;34m\]\w\[\033[0m\] \[\033[1;36m\]\$\[\033[0m\] "
            echo "🚀 Stellar Live Source Datalake Development Environment"
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
            echo "  make build-server      - Build the binary"
            echo "  make nix-build         - Build with Nix"
            echo "  make docker-build      - Build Docker image"
            echo "  make nix-run           - Run the binary"
            echo "  make vendor            - Vendor dependencies (with GOWORK=off)"
          '';
        };
        
        # App for 'nix run'
        apps.default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/stellar_live_source_datalake";
        };
      }
    );
}