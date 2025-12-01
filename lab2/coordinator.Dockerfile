# Build stage
FROM golang:1.25 AS builder
WORKDIR /src

COPY . .

# Build a static linux binary from proxy/main.go
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /bin/coordinator /src/main/mrcoordinator.go

# Production stage minimal image with a shell that supports globbing
FROM alpine:3.22.2
# Copy the compiled binary into the final image
COPY --from=builder /bin/coordinator /coordinator
COPY --from=builder /src/main/pg-*   /input/

# Expose the port your proxy listens on (adjust if different)
EXPOSE 1234

WORKDIR /output

ENTRYPOINT ["/bin/sh", "-c", "/coordinator /input/pg*"]
