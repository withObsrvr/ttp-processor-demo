{
  description = "Node.js Consumer App - TTP event processor client";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        nodeVersion = pkgs.nodejs_22;
      in
      {
        packages = {
          default = pkgs.buildNpmPackage {
            pname = "ttp-consumer-node";
            version = "1.0.0";
            src = ./.;
            
            nodejs = nodeVersion;
            
            # Use package-lock.json for consistent builds
            npmDepsHash = "sha256-lJ22kmH3KtGNSxFHLazdHy0eAeMv7g6bfx83jeLtsZc=";
            
            # Copy proto files before build
            preBuild = ''
              # Copy proto files from the consumer_app directory
              cp -r ${../protos}/* ./protos/
            '';
            
            # Build phase
            buildPhase = ''
              runHook preBuild
              
              # Generate protobuf files for Node.js
              echo "Generating protobuf files for Node.js..."
              mkdir -p gen
              
              # List what we have in the protos directory
              echo "Contents of protos directory:"
              ls -la ./protos/
              echo "Looking for event_service proto:"
              find ./protos -name "*.proto" | head -10
              
              # Generate TypeScript protobuf files
              echo "Generating protobuf files..."
              protoc \
                --plugin=protoc-gen-ts=./node_modules/.bin/protoc-gen-ts \
                --proto_path=./protos \
                --ts_out=grpc_js:./gen \
                --ts_opt=esModuleInterop=true \
                --experimental_allow_proto3_optional \
                event_service/event_service.proto \
                ingest/processors/token_transfer/token_transfer_event.proto \
                ingest/asset/asset.proto
              
              echo "Protobuf generation complete!"
              
              # TypeScript compilation
              echo "Compiling TypeScript..."
              npx tsc
              
              runHook postBuild
            '';
            
            # Install phase
            installPhase = ''
              runHook preInstall
              
              # Create output directory
              mkdir -p $out/bin $out/lib/node_modules/ttp-consumer-node
              
              # Copy built files
              cp -r dist/* $out/lib/node_modules/ttp-consumer-node/
              cp -r node_modules $out/lib/node_modules/ttp-consumer-node/
              cp package.json $out/lib/node_modules/ttp-consumer-node/
              
              # Create wrapper script
              cat > $out/bin/ttp-consumer-node << 'EOF'
#!/usr/bin/env bash
exec ${nodeVersion}/bin/node $out/lib/node_modules/ttp-consumer-node/index.js "$@"
EOF
              chmod +x $out/bin/ttp-consumer-node
              
              runHook postInstall
            '';
            
            # Add build dependencies
            nativeBuildInputs = with pkgs; [
              nodeVersion
              typescript
              protobuf
              protoc-gen-js
            ];
          };
          
          # Docker image
          docker = pkgs.dockerTools.buildImage {
            name = "ttp-consumer-node";
            tag = "latest";
            
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
            
            config = {
              Entrypoint = [ "/bin/ttp-consumer-node" ];
              ExposedPorts = {
                "8088/tcp" = {};
              };
              Env = [
                "NODE_ENV=production"
                "PATH=/bin"
              ];
              WorkingDir = "/";
              User = "1000:1000";
            };
          };
        };

        # Development shell
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            nodeVersion
            typescript
            protobuf
            protoc-gen-js
            git
            gnumake
            docker
          ];
          
          shellHook = ''
            export PS1="\[\033[1;32m\][nix:ttp-consumer-node]\[\033[0m\] \[\033[1;34m\]\w\[\033[0m\] \[\033[1;36m\]\$\[\033[0m\] "
            echo "🚀 TTP Consumer Node.js Development Environment"
            echo "Node.js version: $(node --version)"
            echo "TypeScript version: $(npx tsc --version)"
            
            # Install dependencies if not present
            if [ ! -d "node_modules" ]; then
              echo "Installing dependencies..."
              npm install
            fi
            
            echo "Development environment ready!"
            echo ""
            echo "Available commands:"
            echo "  make build           - Build the application"
            echo "  make nix-build       - Build with Nix"
            echo "  make docker-build    - Build Docker image"
            echo "  make nix-run         - Run the application"
            echo "  npm run build        - TypeScript compilation"
            echo "  npm run start        - Start the application"
            echo "  npm run dev          - Start in development mode"
          '';
        };
        
        # App for 'nix run'
        apps.default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/ttp-consumer-node";
        };
      }
    );
}