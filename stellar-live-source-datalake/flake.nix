{
  description = "Stellar Live Source Datalake - A service for processing data from the Stellar blockchain";

  # Declare inputs - sources for packages, libraries, etc.
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        
        # Define Go version to use
        go = pkgs.go_1_23;
        
        # Define the package
        stellar-live-source-datalake = pkgs.buildGoModule {
          pname = "stellar-live-source-datalake";
          version = "0.1.0";
          
          src = ./.;
          
          # Replace with the actual hash after the first build attempt
          # or use "lib.fakeSha256" for the first build to get the right hash
          vendorSha256 = null;
          
          # Build the main binary
          buildPhase = ''
            export GOFLAGS="-mod=vendor"
            go build -o $out/bin/stellar_live_source_datalake ./go/main.go
          '';
          
          # Skip install phase as we do everything in the build phase
          installPhase = "echo 'Skipping install phase'";
          
          meta = with pkgs.lib; {
            description = "Stellar Live Source Datalake Service";
            homepage = "https://github.com/withObsrvr/ttp-processor-demo";
            license = licenses.mit;
            maintainers = [];
          };
        };
        
        # Docker image creation
        dockerImage = pkgs.dockerTools.buildImage {
          name = "stellar-live-source-datalake";
          tag = "latest";
          
          # Copy the built binary
          copyToRoot = pkgs.buildEnv {
            name = "image-root";
            paths = [
              stellar-live-source-datalake
              pkgs.bash
              pkgs.coreutils
            ];
            pathsToLink = [ "/bin" ];
          };
          
          # Docker configuration
          config = {
            Cmd = [ "/bin/stellar_live_source_datalake" ];
            ExposedPorts = {
              "50052/tcp" = {}; # Main gRPC port
              "8088/tcp" = {};  # Health check port
            };
            Env = [
              "STORAGE_TYPE=FS"   # Default to filesystem storage
              "PATH=/bin"
            ];
            WorkingDir = "/";
          };
        };
        
      in {
        # Packages for 'nix build'
        packages = {
          stellar-live-source-datalake = stellar-live-source-datalake;
          docker = dockerImage;
          default = stellar-live-source-datalake;
        };
        
        # Apps for 'nix run'
        apps = {
          default = {
            type = "app";
            program = "${stellar-live-source-datalake}/bin/stellar_live_source_datalake";
          };
        };
        
        # Development environment
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            go
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