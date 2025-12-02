# Build stage
FROM golang:1.25 AS builder
WORKDIR /src

COPY . .

# Build a static linux binary from proxy/main.go
WORKDIR /src
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /bin/chord .

# Production stage: minimal image using scratch
FROM scratch
# Copy the compiled binary into the final image
COPY --from=builder /bin/chord /chord

ENTRYPOINT ["/chord"]