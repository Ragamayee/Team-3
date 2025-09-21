# Start from official Golang image
FROM golang:1.22 AS builder

# Set working directory
WORKDIR /app

# Copy go mod files first (for caching deps)
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the application
COPY . .

# Build the Go binary
RUN go build -o main ./cmd/server

# -----------------------------
# Final stage: minimal runtime
# -----------------------------
FROM debian:bookworm-slim

# Install CA certificates for HTTPS
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create nonroot user and group
RUN addgroup --system nonroot && adduser --system --ingroup nonroot nonroot

# Create data directory and give ownership
RUN mkdir -p /srv/data && chown -R nonroot:nonroot /srv/data

# Set working directory
WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/main .

# Switch to nonroot user
USER nonroot

# Expose port (optional, depends on your app)
EXPOSE 8080

# Run the binary
CMD ["./main"]
