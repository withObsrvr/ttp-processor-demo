{
  description = "TTP Processor Demo - Development Environment";

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
        devShells.default = pkgs.mkShell {
          name = "ttp-processor-dev";
          
          packages = with pkgs; [
            # Go development
            go
            gopls
            gotools
            
            # Protocol Buffers
            protobuf
            protoc-gen-go
            protoc-gen-go-grpc
            
            # Build tools
            gnumake
            curl
            
            # Node.js for consumer apps
            nodejs_22
            
            # Rust for WASM consumer
            rustc
            cargo
            wasm-pack
            
            # Container and deployment tools
            docker
            kubectl
            
            # Development utilities
            git
            jq
            yq
          ];

          shellHook = ''
            echo "🚀 TTP Processor Demo Development Environment"
            echo ""
            echo "Available tools:"
            echo "  Go: $(go version)"
            echo "  Node.js: $(node --version)"
            echo "  Rust: $(rustc --version)"
            echo "  Protocol Buffers: $(protoc --version)"
            echo ""
            echo "Quick start:"
            echo "  make check-deps           # Check build dependencies"
            echo "  make -C ttp-processor all # Build ttp-processor"
            echo "  make -C consumer_app all  # Build consumer apps"
            echo ""
            echo "Services:"
            echo "  stellar-live-source/              # RPC-based data source"
            echo "  stellar-live-source-datalake/     # Storage-based data source (has own flake)"
            echo "  ttp-processor/                    # TTP event processor"
            echo "  consumer_app/                     # Consumer applications"
            echo ""
          '';

          # Environment variables
          env = {
            # Go configuration
            GO111MODULE = "on";
            GOFLAGS = "-mod=readonly";
            
            # Protocol 23 configuration
            ENABLE_UNIFIED_EVENTS = "true";
            PROTOCOL_VERSION = "23";
            
            # Development settings
            STELLAR_NETWORK = "testnet";
            HEALTH_PORT = "8088";
          };
        };

        # Convenience aliases for different development scenarios
        devShells.minimal = pkgs.mkShell {
          name = "ttp-processor-minimal";
          packages = with pkgs; [
            go
            protobuf
            gnumake
            curl
          ];
          shellHook = ''
            echo "🔧 Minimal TTP Processor Development Environment"
            echo "Run 'make check-deps' to verify setup"
          '';
        };

        devShells.frontend = pkgs.mkShell {
          name = "ttp-processor-frontend";
          packages = with pkgs; [
            nodejs_22
            rustc
            cargo
            wasm-pack
            protobuf
            curl
          ];
          shellHook = ''
            echo "🎨 Frontend Development Environment (Node.js + Rust WASM)"
            echo "Ready for consumer app development"
          '';
        };
      }
    );
}