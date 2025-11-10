{
  description = "DuckLake Ingestion Processor V2 - Appender API (534x faster)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        # Override DuckDB to version 1.4.1 for DuckLake extension support
        duckdb_1_4_1 = pkgs.duckdb.overrideAttrs (oldAttrs: rec {
          version = "1.4.1";

          src = pkgs.fetchFromGitHub {
            owner = "duckdb";
            repo = "duckdb";
            rev = "v${version}";
            hash = "sha256-w/mELyRs4B9hJngi1MLed0fHRq/ldkkFV+SDkSxs3O8=";
          };
        });

        # Build the Obsrvr-compliant application with Appender API
        ducklake-ingestion-obsrvr = pkgs.buildGoModule rec {
          pname = "ducklake-ingestion-obsrvr-v2";
          version = "2.1.0"; # V2 with Appender API

          src = ./.;

          # IMPORTANT: Update this hash after first build attempt
          # Run: nix build 2>&1 | grep "got:" | awk '{print $2}'
          vendorHash = "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="; # Placeholder

          sourceRoot = "source/go";

          nativeBuildInputs = [ duckdb_1_4_1 ];
          buildInputs = [ duckdb_1_4_1 ];

          # Enable CGO for DuckDB
          env.CGO_ENABLED = "1";

          ldflags = [
            "-s"
            "-w"
          ];

          preBuild = ''
            export CGO_ENABLED=1
            export CGO_CFLAGS="-I${duckdb_1_4_1}/include"
            export CGO_LDFLAGS="-L${duckdb_1_4_1}/lib"
          '';

          doCheck = false;

          meta = with pkgs.lib; {
            description = "Stellar ledger ingestion to DuckLake V2 - Appender API (534x faster)";
            license = licenses.mit;
            platforms = platforms.linux;
          };
        };

      in
      {
        # Development shell with DuckDB 1.4.1
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            go_1_24
            duckdb_1_4_1  # DuckDB with DuckLake extension support
            protobuf
            protoc-gen-go
            protoc-gen-go-grpc
          ];

          shellHook = ''
            echo "🚀 DuckLake Ingestion Processor V2 - Appender API (534x faster!)"
            echo ""
            echo "⚡ V2 Performance Improvements:"
            echo "  - DuckDB Appender API: 534x faster than V1 SQL INSERT"
            echo "  - Mainnet: 126s → 0.236s per batch (25 ledgers)"
            echo "  - Testnet: 8x larger batches (200 ledgers)"
            echo "  - No timeouts needed (flush in milliseconds)"
            echo ""
            echo "📊 Obsrvr Data Culture Features:"
            echo "  - Metadata tables: _meta_datasets, _meta_lineage, _meta_quality, _meta_changes"
            echo "  - Quality checks: 19 automated checks across all tables"
            echo "  - Lineage tracking: Full provenance from source to processed data"
            echo "  - Playbook naming: core.ledgers_row_v2, core.transactions_row_v2, etc."
            echo ""
            echo "DuckDB version: $(${duckdb_1_4_1}/bin/duckdb --version)"
            echo "Go version: $(go version)"
            echo ""
            echo "Available commands:"
            echo "  make build                                           - Build V2 processor"
            echo "  ./ducklake-ingestion-obsrvr -config config/[file]   - Run with config"
            echo "  duckdb                                               - Start DuckDB CLI"
            echo ""

            # Set up CGO environment for DuckDB
            export CGO_ENABLED=1
            export CGO_CFLAGS="-I${duckdb_1_4_1}/include"
            export CGO_LDFLAGS="-L${duckdb_1_4_1}/lib"

            # Set custom prompt
            export PS1="\[\033[1;35m\][nix:obsrvr-v2]\[\033[0m\] \[\033[1;32m\]\u@\h\[\033[0m\]:\[\033[1;34m\]\w\[\033[0m\]\$ "
          '';
        };

        # Package outputs
        packages = {
          default = ducklake-ingestion-obsrvr;
          ducklake-ingestion-obsrvr = ducklake-ingestion-obsrvr;
        };

        # Formatter
        formatter = pkgs.nixpkgs-fmt;
      });
}
