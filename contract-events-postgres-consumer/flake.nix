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

        # Path to contract-events-processor for proto files
        processorPath = ../contract-events-processor;
      in
      {
        packages = {
          default = pkgs.buildGoModule {
            pname = "contract-events-postgres-consumer";
            version = "0.1.0";
            src = ./.;

            # Use vendored dependencies for improved build reliability
            vendorHash = null;
            modVendorDir = "./go/vendor";
            allowVendorCheck = false;
            buildFlags = ["-mod=vendor" "-modcacherw"];

            env = {
              GOPROXY = "off";
            };

            preBuild = ''
              echo "Setting up build environment..."

              # Copy proto files from contract-events-processor
              echo "Copying proto files from contract-events-processor..."
              mkdir -p ../contract-events-processor/go/gen
              cp -r ${processorPath}/go/gen/* ../contract-events-processor/go/gen/ || true

              # Vendor dependencies if not present
              if [ ! -d "go/vendor" ]; then
                echo "Vendoring dependencies..."
                cd go
                GOWORK=off go mod tidy
                GOWORK=off go mod vendor
                cd ..
              else
                echo "Using existing vendor directory"
              fi

              echo "Build environment ready"
            '';

            buildPhase = ''
              runHook preBuild

              # Disable go workspace mode
              export GOWORK=off

              # Build the consumer
              cd go
              go build -mod=vendor -o ../contract-events-postgres-consumer
              cd ..

              runHook postBuild
            '';

            installPhase = ''
              runHook preInstall
              mkdir -p $out/bin
              cp contract-events-postgres-consumer $out/bin/
              chmod +x $out/bin/contract-events-postgres-consumer
              runHook postInstall
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
