FROM nixos/nix:latest AS builder

# Copy flake files
WORKDIR /build
COPY flake.nix flake.lock ./

# Build the application using Nix
RUN nix \
    --extra-experimental-features "nix-command flakes" \
    build

# Extract the binary
RUN mkdir -p /tmp/bin && \
    cp -vL result/bin/stellar_live_source_datalake /tmp/bin/

# Create a minimal final image
FROM alpine:3.18

# Install runtime dependencies
RUN apk --no-cache add \
    ca-certificates \
    bash \
    tzdata

# Copy the binary from the builder stage
COPY --from=builder /tmp/bin/stellar_live_source_datalake /bin/

# Create a non-root user to run the service
RUN addgroup -g 1000 -S app && \
    adduser -u 1000 -S app -G app

USER app
WORKDIR /home/app

# Expose ports
EXPOSE 50052 8088

# Environment variables
ENV STORAGE_TYPE=FS

# Command to run
ENTRYPOINT ["/bin/stellar_live_source_datalake"]

# Add labels
LABEL org.opencontainers.image.title="Stellar Live Source Datalake"
LABEL org.opencontainers.image.description="A service for processing data from the Stellar blockchain"
LABEL org.opencontainers.image.source="https://github.com/withObsrvr/ttp-processor-demo"