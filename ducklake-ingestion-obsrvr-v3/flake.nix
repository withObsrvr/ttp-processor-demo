{
  description = "DuckLake Ingestion Obsrvr v2 - Bronze layer data ingestion service for Stellar blockchain";

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
            pname = "ducklake-ingestion-obsrvr-v2";
            version = "2.3.0"; # Cycle 8: Effects & Trades tables
            src = ./.;

            # Use vendored dependencies for reproducible builds
            vendorHash = null;
            modVendorDir = "./go/vendor";
            allowVendorCheck = false;
            buildFlags = ["-mod=vendor" "-modcacherw"];

            # Set environment variables for go builds
            env = {
              GOPROXY = "off";
            };

            # Customize Go build to work with our project structure
            preBuild = ''
              # Verify vendor directory exists
              if [ -d "go/vendor" ]; then
                echo "Using existing vendor directory"
              else
                echo "ERROR: No vendor directory found. Run 'cd go && GOWORK=off go mod vendor' first."
                exit 1
              fi

              echo "Preparing build environment..."
              cd go

              # Disable workspace mode for reproducible builds
              export GOWORK=off
            '';

            buildPhase = ''
              runHook preBuild

              # Disable go workspace mode
              export GOWORK=off

              # Build using vendored deps
              echo "Building ducklake-ingestion-obsrvr-v2..."
              go build -mod=vendor -o ../ducklake-ingestion-obsrvr-v2 .

              # Build resolver-test CLI tool
              echo "Building resolver-test tool..."
              go build -mod=vendor -o ../resolver-test ./cmd/resolver-test

              runHook postBuild
            '';

            installPhase = ''
              runHook preInstall

              mkdir -p $out/bin

              # Install main binary
              cp ../ducklake-ingestion-obsrvr-v2 $out/bin/
              chmod +x $out/bin/ducklake-ingestion-obsrvr-v2

              # Install resolver-test tool
              cp ../resolver-test $out/bin/
              chmod +x $out/bin/resolver-test

              # Install documentation
              mkdir -p $out/share/doc/ducklake-ingestion-obsrvr-v2
              if [ -d "../docs" ]; then
                cp -r ../docs/* $out/share/doc/ducklake-ingestion-obsrvr-v2/
              fi

              runHook postInstall
            '';

            # Add native build dependencies
            nativeBuildInputs = [
              pkgs.go
              pkgs.gnumake
            ];

            # Runtime dependencies (for DuckDB native library)
            buildInputs = [
              pkgs.stdenv.cc.cc.lib
            ];

            # Metadata
            meta = with pkgs.lib; {
              description = "Bronze layer data ingestion service for Stellar blockchain using DuckLake";
              homepage = "https://github.com/withObsrvr/ttp-processor-demo";
              license = licenses.mit;
              maintainers = [ ];
              platforms = platforms.linux ++ platforms.darwin;
            };
          };

          # Docker image using Nix-built binary (follows article pattern)
          docker = pkgs.dockerTools.buildImage {
            name = "ducklake-ingestion-obsrvr-v2";
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
                # DuckDB runtime dependencies
                pkgs.stdenv.cc.cc.lib
              ];
              pathsToLink = [ "/bin" "/etc" "/share" "/lib" "/lib64" ];
            };

            # Configuration
            config = {
              Entrypoint = [ "/bin/ducklake-ingestion-obsrvr-v2" ];
              ExposedPorts = {
                "8088/tcp" = {}; # Health check port
              };
              Env = [
                "PATH=/bin"
                "LD_LIBRARY_PATH=/lib:/lib64"
              ];
              WorkingDir = "/app";
              User = "1000:1000";

              # OCI labels for flowctl component discovery
              Labels = {
                "io.flowctl.component.type" = "processor";
                "io.flowctl.component.api-version" = "v1";
                "io.flowctl.component.name" = "ducklake-ingestion-obsrvr-v2";
                "io.flowctl.component.description" = "Bronze layer ingestion processor for Stellar blockchain data";
                "io.flowctl.component.input-events" = "raw_ledger_service.RawLedgerChunk";
                "io.flowctl.component.output-events" = "bronze.parquet";
                "org.opencontainers.image.source" = "https://github.com/withObsrvr/ttp-processor-demo";
                "org.opencontainers.image.title" = "DuckLake Ingestion Obsrvr v2";
                "org.opencontainers.image.description" = "Bronze layer data ingestion service using DuckLake";
                "org.opencontainers.image.vendor" = "Obsrvr";
                "org.opencontainers.image.version" = "2.3.0";
              };
            };
          };
        };

        # Development shell for working on the project
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            go
            gopls
            delve
            git
            gnumake
            docker
            # DuckDB development
            stdenv.cc.cc.lib
          ];

          # Shell setup for development environment
          shellHook = ''
            # Set custom prompt
            export PS1="\[\033[1;32m\][nix:ducklake-obsrvr-v2]\[\033[0m\] \[\033[1;34m\]\w\[\033[0m\] \[\033[1;36m\]\$\[\033[0m\] "
            echo "🚀 DuckLake Ingestion Obsrvr v2 Development Environment"
            echo "Go version: $(go version)"

            # Disable Go workspace mode for reproducible builds
            export GOWORK=off
            export GO111MODULE="on"

            # Set LD_LIBRARY_PATH for DuckDB
            export LD_LIBRARY_PATH="${pkgs.stdenv.cc.cc.lib}/lib:$LD_LIBRARY_PATH"

            # Helper to vendor dependencies
            if [ ! -d go/vendor ]; then
              echo ""
              echo "⚠️  No vendor directory found."
              echo "Run: cd go && GOWORK=off go mod vendor"
              echo ""
            fi

            echo ""
            echo "Development environment ready!"
            echo ""
            echo "Available commands:"
            echo "  cd go && GOWORK=off go build -o ../ducklake-ingestion-obsrvr-v2 ."
            echo "  cd go && GOWORK=off go mod vendor      - Vendor dependencies"
            echo "  nix build                              - Build with Nix"
            echo "  nix build .#docker                     - Build Docker image"
            echo "  nix run                                - Run the binary"
            echo "  nix run .#resolver-test                - Run resolver-test tool"
            echo ""
            echo "Documentation:"
            echo "  docs/DEPLOYMENT_GUIDE.md"
            echo "  docs/PROTOCOL_25_UPGRADE_RUNBOOK.md"
            echo "  docs/STELLAR_EVENTS_RECONCILIATION_RUNBOOK.md"
            echo "  go/resolver/README.md"
            echo ""
          '';
        };

        # App for 'nix run'
        apps.default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/ducklake-ingestion-obsrvr-v2";
        };

        # App for resolver-test
        apps.resolver-test = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/resolver-test";
        };

        # Formatter
        formatter = pkgs.nixpkgs-fmt;
      }
    );
}
