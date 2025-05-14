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
          # Binary application - use a plain derivation for more direct control
          stellar-live-source-datalake = pkgs.stdenv.mkDerivation {
            pname = "stellar-live-source-datalake";
            version = "0.1.0";
            src = ./.;
            
            # Use Go for building (use the latest available version)
            nativeBuildInputs = [ pkgs.go ];
            
            # Build the main binary directly
            buildPhase = ''
              echo "Building binary from $(pwd)"
              cd go
              # Ensure clean environment and module setup
              export GOCACHE=$TMPDIR/go-cache
              export GOPATH=$TMPDIR/go
              
              # Update dependencies
              go mod tidy
              go mod vendor
              
              # Build with vendor directory
              go build -mod=vendor -o stellar_live_source_datalake main.go
              
              echo "Build complete, file size: $(ls -la stellar_live_source_datalake)"
            '';

            # Install the binary
            installPhase = ''
              mkdir -p $out/bin
              cp go/stellar_live_source_datalake $out/bin/
              chmod +x $out/bin/stellar_live_source_datalake
              echo "Installed binary to $out/bin/stellar_live_source_datalake"
            '';
          };

          # Just alias the default package for now
          docker = self.packages.${system}.stellar-live-source-datalake;

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
            go            # Latest Go version
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