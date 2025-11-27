{
  description = "DuckLake API Gateway - Fast REST API for querying Stellar DuckLake data";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        # Python environment with DuckDB and Flask
        pythonEnv = pkgs.python311.withPackages (ps: [
          ps.flask
          ps.duckdb
        ]);

      in
      {
        packages = {
          default = pkgs.stdenv.mkDerivation {
            pname = "ducklake-api-gateway";
            version = "1.0.0";
            src = ./.;

            buildInputs = [ pythonEnv ];
            nativeBuildInputs = [ pkgs.makeWrapper ];

            installPhase = ''
              runHook preInstall

              mkdir -p $out/bin $out/lib/ducklake-api-gateway

              # Install Python application
              cp server.py $out/lib/ducklake-api-gateway/
              cp requirements.txt $out/lib/ducklake-api-gateway/
              cp README.md $out/lib/ducklake-api-gateway/

              # Create wrapper script
              makeWrapper ${pythonEnv}/bin/python3 $out/bin/ducklake-api-gateway \
                --add-flags "$out/lib/ducklake-api-gateway/server.py" \
                --set PYTHONUNBUFFERED 1

              runHook postInstall
            '';

            meta = with pkgs.lib; {
              description = "Fast REST API for querying Stellar DuckLake data with persistent connections";
              homepage = "https://github.com/withObsrvr/ttp-processor-demo";
              license = licenses.mit;
              maintainers = [ ];
              platforms = platforms.linux ++ platforms.darwin;
            };
          };

          # Docker image
          docker = pkgs.dockerTools.buildImage {
            name = "ducklake-api-gateway";
            tag = "latest";

            copyToRoot = pkgs.buildEnv {
              name = "image-root";
              paths = [
                self.packages.${system}.default
                pythonEnv
                pkgs.bash
                pkgs.coreutils
                pkgs.tzdata
                pkgs.cacert
              ];
              pathsToLink = [ "/bin" "/etc" "/share" "/lib" "/lib64" ];
            };

            config = {
              Entrypoint = [ "/bin/ducklake-api-gateway" ];
              ExposedPorts = {
                "8000/tcp" = {}; # API port
              };
              Env = [
                "PATH=/bin"
                "PYTHONUNBUFFERED=1"
              ];
              WorkingDir = "/app";
              User = "1000:1000";

              Labels = {
                "io.flowctl.component.type" = "api-gateway";
                "io.flowctl.component.api-version" = "v1";
                "io.flowctl.component.name" = "ducklake-api-gateway";
                "io.flowctl.component.description" = "REST API gateway for DuckLake queries";
                "org.opencontainers.image.source" = "https://github.com/withObsrvr/ttp-processor-demo";
                "org.opencontainers.image.title" = "DuckLake API Gateway";
                "org.opencontainers.image.description" = "Fast REST API for querying Stellar DuckLake data";
                "org.opencontainers.image.vendor" = "Obsrvr";
                "org.opencontainers.image.version" = "1.0.0";
              };
            };
          };
        };

        # Development shell
        devShells.default = pkgs.mkShell {
          buildInputs = [
            pythonEnv
            pkgs.git
            pkgs.curl
            pkgs.jq
          ];

          shellHook = ''
            export PS1="\[\033[1;32m\][nix:ducklake-api-gateway]\[\033[0m\] \[\033[1;34m\]\w\[\033[0m\] \[\033[1;36m\]\$\[\033[0m\] "
            echo "🚀 DuckLake API Gateway Development Environment"
            echo "Python version: $(python3 --version)"
            echo ""
            echo "Available commands:"
            echo "  python3 server.py                  - Start the API server"
            echo "  nix build                          - Build with Nix"
            echo "  nix build .#docker                 - Build Docker image"
            echo "  nix run                            - Run the API gateway"
            echo ""
            echo "API Endpoints (after starting server):"
            echo "  curl http://localhost:8000/health"
            echo "  curl http://localhost:8000/ledgers?limit=10"
            echo "  curl http://localhost:8000/balances/ACCOUNT_ID"
            echo ""
            echo "⚠️  First startup takes 5-10 minutes to initialize DuckDB connection"
            echo ""
          '';
        };

        # App for 'nix run'
        apps.default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/ducklake-api-gateway";
        };

        # Formatter
        formatter = pkgs.nixpkgs-fmt;
      }
    );
}
