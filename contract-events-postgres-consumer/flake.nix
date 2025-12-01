{
  description = "PostgreSQL Consumer for Contract Events - Saves Soroban contract events to PostgreSQL";

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
          default = pkgs.buildGoModule rec {
            pname = "contract-events-postgres-consumer";
            version = "0.1.0";
            src = ./.;

            # Vendor hash for reproducible builds
            vendorHash = "sha256-pWnV2XoDfOhikVMZACM4GZtZc+8eZW1FQIGEKMR8b2k=";

            # Set the Go module directory
            modRoot = "./go";

            # Disable go workspace mode
            env = {
              GOWORK = "off";
            };

            preBuild = ''
              echo "Setting up build environment for contract-events-postgres-consumer..."
            '';

            nativeBuildInputs = [
              pkgs.go
              pkgs.gnumake
            ];
          };

          # Docker image
          docker = pkgs.dockerTools.buildImage {
            name = "withobsrvr/contract-events-postgres-consumer";
            tag = "latest";

            copyToRoot = pkgs.buildEnv {
              name = "image-root";
              paths = [
                self.packages.${system}.default
                pkgs.bash
                pkgs.coreutils
                pkgs.tzdata
                pkgs.cacert
                pkgs.postgresql  # For psql client
              ];
              pathsToLink = [ "/bin" "/etc" "/share" ];
            };

            config = {
              Entrypoint = [ "/bin/contract-events-postgres-consumer" ];
              Cmd = [ "1000" "0" ];
              ExposedPorts = {
                "8090/tcp" = {};  # Health check port
              };
              Env = [
                "PATH=/bin"
                "POSTGRES_PORT=5432"
                "POSTGRES_HOST=localhost"
                "POSTGRES_DB=contract_events"
                "POSTGRES_USER=postgres"
                "POSTGRES_SSLMODE=disable"
                "CONTRACT_EVENTS_SERVICE_ADDRESS=localhost:50053"
              ];
              WorkingDir = "/";
              User = "1000:1000";
            };
          };
        };

        # Development shell
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            go
            gopls
            delve
            git
            gnumake
            docker
            postgresql  # For psql client
          ];

          shellHook = ''
            export PS1="\[\033[1;32m\][nix:postgres-consumer]\[\033[0m\] \[\033[1;34m\]\w\[\033[0m\] \[\033[1;36m\]\$\[\033[0m\] "
            echo "🐘 Contract Events PostgreSQL Consumer Development Environment"
            echo "Go version: $(go version)"

            # Disable Go workspace mode
            export GOWORK=off
            export GO111MODULE="on"

            # Vendor dependencies if needed
            if [ ! -d go/vendor ]; then
              echo "Vendoring dependencies..."
              cd go
              GOWORK=off go mod tidy
              GOWORK=off go mod vendor
              cd ..
            fi

            echo "Development environment ready!"
            echo ""
            echo "Available commands:"
            echo "  make build              - Build the binary"
            echo "  nix build               - Build with Nix"
            echo "  nix build .#docker      - Build Docker image"
            echo "  nix run                 - Run the binary"
            echo "  ./run.sh 1000 1100      - Quick test run"
          '';
        };

        # App for 'nix run'
        apps.default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/contract-events-postgres-consumer";
        };
      }
    );
}
