{
  description = "Stellar Arrow Source - Native Apache Arrow data source for Stellar ledger data";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        
        # Build inputs for Apache Arrow C++ libraries
        arrowInputs = with pkgs; [
          arrow-cpp
          parquet-cpp
          pkgconfig
        ];

        # Go development dependencies
        goInputs = with pkgs; [
          go_1_23
          golangci-lint
          gopls
          delve
          goimports
          protobuf
          protoc-gen-go
          protoc-gen-go-grpc
        ];

        # System dependencies
        systemInputs = with pkgs; [
          git
          curl
          jq
          grpcurl
        ];

      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = arrowInputs ++ goInputs ++ systemInputs;
          
          shellHook = ''
            echo "🏹 Welcome to Stellar Arrow Source development environment"
            echo "📦 Apache Arrow C++: $(pkg-config --modversion arrow)"
            echo "🐹 Go version: $(go version)"
            echo ""
            echo "Available commands:"
            echo "  make build          - Build the service"
            echo "  make test           - Run tests"
            echo "  make test-arrow-*   - Run Arrow-specific tests"
            echo "  make benchmark      - Run performance benchmarks"
            echo "  make nix-build      - Build with Nix"
            echo ""
            
            # Set up environment variables for Arrow
            export CGO_ENABLED=1
            export PKG_CONFIG_PATH="${pkgs.arrow-cpp}/lib/pkgconfig:${pkgs.parquet-cpp}/lib/pkgconfig:$PKG_CONFIG_PATH"
            export ARROW_HOME="${pkgs.arrow-cpp}"
            export PARQUET_HOME="${pkgs.parquet-cpp}"
            
            # Go environment
            export GOPROXY=https://proxy.golang.org,direct
            export GOSUMDB=sum.golang.org
            
            echo "🔧 Environment configured for native Arrow development"
          '';
        };

        packages.default = pkgs.buildGoModule {
          pname = "stellar-arrow-source";
          version = "1.0.0";
          
          src = ./.;
          
          modRoot = "./go";
          vendorHash = null; # Will be computed on first build
          
          buildInputs = arrowInputs;
          
          nativeBuildInputs = with pkgs; [
            pkgconfig
            protobuf
            protoc-gen-go
            protoc-gen-go-grpc
          ];
          
          preBuild = ''
            make gen-proto
          '';
          
          meta = with pkgs.lib; {
            description = "Native Apache Arrow data source for Stellar ledger data";
            homepage = "https://github.com/your-org/ttp-processor-demo";
            license = licenses.mit;
            maintainers = [ "ttp-processor-demo team" ];
          };
        };
      }
    );
}