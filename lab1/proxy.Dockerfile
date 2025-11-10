# Build stage
FROM golang:1.25 AS builder
WORKDIR /src

COPY . .

# Build a static linux binary from proxy/main.go
WORKDIR /src/proxy
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /proxy/bin/proxy .

# Production stage: minimal image using scratch
FROM scratch
# Copy the compiled binary into the final image
COPY --from=builder /proxy/bin/proxy /proxy

# Expose the port your proxy listens on (adjust if different)
EXPOSE 8080

ENTRYPOINT ["/proxy"]