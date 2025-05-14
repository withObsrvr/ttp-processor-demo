# For compatibility with traditional nix-shell (without flakes)
# Use this with: nix-shell

{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
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
    echo "NOTE: For a better experience, consider using Nix flakes: nix develop"
    echo ""
    echo "Available commands:"
    echo "  make build       - Build the binary"
    echo "  make test        - Run tests"
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
}