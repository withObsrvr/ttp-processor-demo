# Docker Build Notes

## Issue with Multi-Stage Build

The multi-stage Docker build failed due to linker errors with the precompiled DuckDB Go bindings. The static libraries in `github.com/duckdb/duckdb-go-bindings/linux-amd64@v0.1.24` are incompatible with the Debian bullseye environment in the `golang:1.23-bullseye` Docker image.

Error:
```
undefined reference to `std::__throw_bad_array_new_length()'
```

## Working Solution

Use `Dockerfile.binary` which copies the locally-built binary instead of building inside Docker:

### Build Process

1. Build locally on the host machine:
   ```bash
   cd index-plane-transformer
   make build
   ```

2. Build Docker image with the pre-built binary:
   ```bash
   docker build -f Dockerfile.binary -t withobsrvr/index-plane-transformer:latest .
   ```

3. Push to registry:
   ```bash
   docker push withobsrvr/index-plane-transformer:latest
   ```

### Why This Works

- The local build environment (likely a newer or different Linux distribution) has compatible C++ libraries
- The locally-built binary is dynamically linked against compatible system libraries
- The runtime image (debian:bullseye-slim) provides the necessary runtime libraries (libstdc++6)

### Image Size

- Built image: 157MB (includes 71MB binary + runtime dependencies)

### Future Improvements

If multi-stage builds are needed in CI/CD:
1. Build on the exact target runtime OS (debian:bullseye-slim)
2. Use DuckDB's official Docker build images if available
3. Build DuckDB from source instead of using precompiled bindings
4. Use a build cache or artifact storage for the binary

### Current Deployment

Image pushed to Docker Hub:
- Repository: `withobsrvr/index-plane-transformer`
- Tag: `latest`
- Digest: `sha256:04362be2e77d1df813fe8df6c5641df6b94c35bad2520dea6be062e7242c2249`
