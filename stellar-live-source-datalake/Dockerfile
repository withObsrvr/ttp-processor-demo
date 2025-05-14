FROM alpine:3.19

# Install runtime dependencies
RUN apk --no-cache add \
    ca-certificates \
    bash \
    tzdata

# Create app directory and user
RUN mkdir -p /app/bin
RUN addgroup -g 1000 -S app && \
    adduser -u 1000 -S app -G app

# Copy the binary (built with Nix)
COPY ./result/bin/stellar_live_source_datalake /app/bin/

# Set permissions
RUN chmod +x /app/bin/stellar_live_source_datalake

# Set environment variables
ENV STORAGE_TYPE=FS
ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/app/bin

# Expose ports
EXPOSE 50052 8088

# Use the app user
USER app
WORKDIR /app

# Run the application
CMD ["/app/bin/stellar_live_source_datalake"]