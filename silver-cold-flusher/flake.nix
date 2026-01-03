{
  description = "Silver Cold Flusher - Periodic flush service from PostgreSQL hot buffer to DuckLake cold storage";

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
            pname = "silver-cold-flusher";
            version = "1.0.0"; # Cycle 4: Silver cold flusher
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
              echo "Building silver-cold-flusher..."
              go build -mod=vendor -o ../silver-cold-flusher .

              runHook postBuild
            '';

            installPhase = ''
              runHook preInstall

              mkdir -p $out/bin

              # Install main binary
              cp ../silver-cold-flusher $out/bin/
              chmod +x $out/bin/silver-cold-flusher

              # Install documentation
              mkdir -p $out/share/doc/silver-cold-flusher
              if [ -d "../docs" ]; then
                cp -r ../docs/* $out/share/doc/silver-cold-flusher/
              fi
              if [ -f "../README.md" ]; then
                cp ../README.md $out/share/doc/silver-cold-flusher/
              fi
              if [ -f "../SHAPE_UP_CYCLE4.md" ]; then
                cp ../SHAPE_UP_CYCLE4.md $out/share/doc/silver-cold-flusher/
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
              pkgs.duckdb
            ];

            # Metadata
            meta = with pkgs.lib; {
              description = "Silver layer cold flusher - Periodic flush from PostgreSQL to DuckLake";
              homepage = "https://github.com/withObsrvr/ttp-processor-demo";
              license = licenses.mit;
              maintainers = [ ];
              platforms = platforms.linux ++ platforms.darwin;
            };
          };

          # Docker image using Nix-built binary
          docker = pkgs.dockerTools.buildImage {
            name = "silver-cold-flusher";
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
                pkgs.duckdb
                pkgs.stdenv.cc.cc.lib
              ];
              pathsToLink = [ "/bin" "/etc" "/share" "/lib" "/lib64" ];
            };

            # Configuration
            config = {
              Entrypoint = [ "/bin/silver-cold-flusher" ];
              ExposedPorts = {
                "8095/tcp" = {}; # Health check port
              };
              Env = [
                "PATH=/bin"
                "LD_LIBRARY_PATH=/lib:/lib64"
              ];
              WorkingDir = "/app";
              User = "1000:1000";

              # OCI labels for flowctl component discovery
              Labels = {
                "io.flowctl.component.type" = "flusher";
                "io.flowctl.component.api-version" = "v1";
                "io.flowctl.component.name" = "silver-cold-flusher";
                "io.flowctl.component.description" = "Silver layer cold flusher service";
                "io.flowctl.component.input-events" = "postgresql.silver_hot";
                "io.flowctl.component.output-events" = "ducklake.silver.parquet";
                "org.opencontainers.image.source" = "https://github.com/withObsrvr/ttp-processor-demo";
                "org.opencontainers.image.title" = "Silver Cold Flusher";
                "org.opencontainers.image.description" = "Periodic flush service from PostgreSQL to DuckLake";
                "org.opencontainers.image.vendor" = "Obsrvr";
                "org.opencontainers.image.version" = "1.0.0";
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
            duckdb
            stdenv.cc.cc.lib
          ];

          # Shell setup for development environment
          shellHook = ''
            # Set custom prompt
            export PS1="\[\033[1;32m\][nix:silver-flusher]\[\033[0m\] \[\033[1;34m\]\w\[\033[0m\] \[\033[1;36m\]\$\[\033[0m\] "
            echo "🚀 Silver Cold Flusher Development Environment"
            echo "Go version: $(go version)"

            # Disable Go workspace mode for reproducible builds
            export GOWORK=off
            export GO111MODULE="on"

            # Set paths for DuckDB
            export LD_LIBRARY_PATH="${pkgs.duckdb}/lib:${pkgs.stdenv.cc.cc.lib}/lib:$LD_LIBRARY_PATH"
            export LIBRARY_PATH="${pkgs.duckdb}/lib:$LIBRARY_PATH"
            export PKG_CONFIG_PATH="${pkgs.duckdb}/lib/pkgconfig:$PKG_CONFIG_PATH"
            export CGO_LDFLAGS="-L${pkgs.duckdb}/lib"
            export CGO_CFLAGS="-I${pkgs.duckdb}/include"

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
            echo "  cd go && GOWORK=off go build -o ../bin/silver-cold-flusher ."
            echo "  cd go && GOWORK=off go mod vendor      - Vendor dependencies"
            echo "  nix build                              - Build with Nix"
            echo "  nix build .#docker                     - Build Docker image"
            echo "  nix run                                - Run the binary"
            echo "  ./scripts/start.sh                     - Start service"
            echo "  ./scripts/stop.sh                      - Stop service"
            echo ""
            echo "Documentation:"
            echo "  README.md"
            echo "  SHAPE_UP_CYCLE4.md"
            echo "  CYCLE4_PROGRESS.md"
            echo ""
          '';
        };

        # App for 'nix run'
        apps.default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/silver-cold-flusher";
        };

        # Formatter
        formatter = pkgs.nixpkgs-fmt;
      }
    );
}
