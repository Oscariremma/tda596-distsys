# TDA596 Distributed Systems Lab

This repository contains lab assignments for the TDA596 Distributed Systems course at Chalmers University of Technology.

## Lab 1: HTTP Server and Proxy

A from-scratch implementation of an HTTP server and HTTP proxy using Go's low-level networking APIs.

### Components

- **HTTP Server** (`lab1/http_server/`): A simple HTTP server that serves static files from a `data/` directory
  - Supports GET requests for files with extensions: txt, html, png, jpg, jpeg, css, gif
  - Implements connection limiting (max 10 concurrent connections)
  - Uses raw TCP connections and 'manual' HTTP parsing

- **HTTP Proxy** (`lab1/proxy/`): A forwarding HTTP proxy server
  - Handles GET requests only
  - Forwards requests to target servers
  - Returns responses back to clients

- **Common HTTP Library** (`lab1/httpCommon/`): Shared utilities
  - HTTP response constants
  - Server setup and connection handling
  - Port configuration

### Prerequisites

- Go 1.25+ (or use the provided Nix flake)
- Docker (optional, for containerized deployment)
### Running Locally

**HTTP Server:**
```bash
cd lab1/http_server
go run main.go [port]  # default port: 8080
```

**Proxy:**
```bash
cd lab1/proxy
go run main.go [port]  # default port: 8080
```

### Running with Docker

Build and run the HTTP server:
```bash
docker build -f lab1/server.Dockerfile -t http-server .
docker run -p 8080:8080 http-server # Prebuilt image also available at ghcr.io/oscariremma/tda596-lab1-http-server:edge
```

Build and run the proxy:
```bash
docker build -f lab1/proxy.Dockerfile -t http-proxy .
docker run -p 8080:8080 http-proxy # Prebuilt image also available at ghcr.io/oscariremma/tda596-lab1-proxy:edge
```

### Testing

Run tests for each component:
```bash
cd lab1/http_server && go test
cd lab1/proxy && go test
```

### Development with Nix

This project includes a Nix flake for reproducible development environments:
```bash
nix develop
```


