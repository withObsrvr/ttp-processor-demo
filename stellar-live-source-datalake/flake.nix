{
  description = "Stellar Live Source Datalake - A service for processing data from the Stellar blockchain";

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
          # Binary application
          stellar-live-source-datalake = pkgs.buildGoModule {
            pname = "stellar-live-source-datalake";
            version = "0.1.0";
            src = ./.;
            
            # Since we're using the vendor directory directly
            vendorHash = null;
            
            # Build the main binary
            buildPhase = ''
              echo "Building binary from $(pwd)"
              cd go
              go build -o stellar_live_source_datalake main.go
              cd ..
              echo "Build complete, file size: $(ls -la go/stellar_live_source_datalake)"
            '';

            # Install the binary
            installPhase = ''
              mkdir -p $out/bin
              cp go/stellar_live_source_datalake $out/bin/
              chmod +x $out/bin/stellar_live_source_datalake
              echo "Installed binary to $out/bin/stellar_live_source_datalake"
            '';
          };

          # Docker image
          docker = pkgs.dockerTools.buildImage {
            name = "stellar-live-source-datalake";
            tag = "latest";
            
            # Copy the built binary and required tools
            copyToRoot = pkgs.buildEnv {
              name = "image-root";
              paths = [
                self.packages.${system}.stellar-live-source-datalake
                pkgs.bash
                pkgs.coreutils
                pkgs.cacert
                pkgs.tzdata
              ];
              pathsToLink = [ "/bin" "/etc" ];
            };
            
            # Docker configuration
            config = {
              Cmd = [ "/bin/stellar_live_source_datalake" ];
              ExposedPorts = {
                "50052/tcp" = {}; # Main gRPC port
                "8088/tcp" = {};  # Health check port
              };
              Env = [
                "STORAGE_TYPE=FS"
                "PATH=/bin"
                "SSL_CERT_FILE=/etc/ssl/certs/ca-bundle.crt"
              ];
              WorkingDir = "/";
            };
          };

          # Default package
          default = self.packages.${system}.stellar-live-source-datalake;
        };
        
        # Apps for 'nix run'
        apps = {
          default = {
            type = "app";
            program = "${self.packages.${system}.stellar-live-source-datalake}/bin/stellar_live_source_datalake";
          };
        };
        
        # Development environment
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            go_1_22
            gopls          # Go language server
            gotools        # Go tools like godoc
            go-outline
            delve          # Go debugger
            golangci-lint  # Go linter
            protobuf       # Protocol Buffers
            protoc-gen-go  # Go code generator for Protocol Buffers
            protoc-gen-go-grpc # gRPC code generator for Go
            docker         # Docker for container work
            gnumake        # Make for build scripts
            jq             # JSON processing
            git            # Version control
          ];
          
          # Environment variables
          shellHook = ''
            echo "🚀 Welcome to Stellar Live Source Datalake Development Environment"
            echo "Go version: $(go version)"
            echo ""
            echo "Available commands:"
            echo "  make build       - Build the binary"
            echo "  make test        - Run tests"
            echo "  nix build        - Build with Nix"
            echo "  nix build .#docker - Build Docker image"
            echo ""
            
            # Helper to vendor dependencies
            if [ ! -d go/vendor ]; then
              echo "Vendoring dependencies..."
              pushd go > /dev/null
              go mod tidy
              go mod vendor
              popd > /dev/null
            fi
            
            # Set up GOPATH and other environment variables
            export GOPATH="$PWD/.go"
            export PATH="$GOPATH/bin:$PATH"
            export GO111MODULE=on
            
            # Create GOPATH directories if they don't exist
            mkdir -p "$GOPATH/src" "$GOPATH/bin" "$GOPATH/pkg"
          '';
        };
      }
    );
}