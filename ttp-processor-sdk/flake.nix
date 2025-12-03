{
  description = "TTP Processor SDK - Token Transfer Processor for Stellar blockchain";

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
            pname = "ttp-processor-sdk";
            version = "2.0.0-sdk";
            src = ./.;

            # Use vendored dependencies for improved build reliability
            vendorHash = null;
            modVendorDir = "./go/vendor";

            # Skip the go module verification
            allowVendorCheck = false;

            # Add Go flags to bypass module checks
            buildFlags = ["-mod=vendor" "-modcacherw"];

            # Set environment variables for go builds
            env = {
              GOPROXY = "off";
            };

            # Customize Go build to work with our project structure
            preBuild = ''
              # Make sure vendor directory is properly set up before build
              if [ -d "go/vendor" ]; then
                echo "Using existing vendor directory"
              else
                echo "No vendor directory found, this will likely fail"
              fi

              echo "Updating go.mod with replace directives..."
              cd go
              GOWORK=off go mod tidy
            '';

            buildPhase = ''
              runHook preBuild
              # Disable go workspace mode
              export GOWORK=off

              # Build using vendored deps
              go build -mod=vendor -o ../ttp-processor *.go
              runHook postBuild
            '';

            installPhase = ''
              runHook preInstall
              mkdir -p $out/bin
              cp ../ttp-processor $out/bin/
              chmod +x $out/bin/ttp-processor
              runHook postInstall
            '';

            # Add any native build dependencies
            nativeBuildInputs = [
              pkgs.go_1_25
              pkgs.gnumake
            ];
          };

          # Docker image
          docker = pkgs.dockerTools.buildImage {
            name = "ttp-processor-sdk";
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
              ];
              pathsToLink = [ "/bin" "/etc" "/share" ];
            };

            # Configuration
            config = {
              Entrypoint = [ "/bin/ttp-processor" ];
              ExposedPorts = {
                "50051/tcp" = {};
                "8088/tcp" = {};
              };
              Env = [
                "NETWORK_PASSPHRASE=Test SDF Network ; September 2015"
                "PATH=/bin"
              ];
              WorkingDir = "/";
              User = "1000:1000";
            };
          };
        };

        # Development shell for working on the project
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            go_1_25
            gopls
            delve
            git
            gnumake
            docker
          ];

          # Shell setup for development environment
          shellHook = ''
            # Set custom prompt
            export PS1="\[\033[1;32m\][nix:ttp-processor]\[\033[0m\] \[\033[1;34m\]\w\[\033[0m\] \[\033[1;36m\]\$\[\033[0m\] "
            echo "🚀 TTP Processor SDK Development Environment"
            echo "Go version: $(go version)"

            # Disable Go workspace mode
            export GOWORK=off
            export GO111MODULE="on"

            # Helper to vendor dependencies - improves build reliability
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
            echo "  nix build              - Build the binary"
            echo "  nix build .#docker     - Build Docker image"
            echo "  nix run                - Run the processor"
            echo "  cd go && go build      - Build manually"
            echo ""
            echo "Cycle 1 features implemented:"
            echo "  ✅ Event filtering"
            echo "  ✅ Batch processing"
            echo "  ✅ Enhanced error handling"
            echo "  ✅ Dimensional metrics"
            echo "  ✅ Configuration validation"
          '';
        };

        # App for 'nix run'
        apps.default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/ttp-processor";
        };
      }
    );
}
