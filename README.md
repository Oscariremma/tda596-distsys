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

## Lab 2: MapReduce

A distributed MapReduce implementation inspired by the original Google MapReduce paper. The system consists of a coordinator that distributes work and workers that execute map and reduce tasks in parallel.
Requires that no NAT/Firewall is blocking communication between coordinator and workers (including between workers).

### Components

- **Coordinator** (`lab2/mr/coordinator.go`): Central coordinator for MapReduce jobs
  - Manages task distribution and scheduling
  - Tracks worker health and handles failures
  - Implements task timeout and retry mechanisms
  - Coordinates the map and reduce phases
  - Exposes RPC interface for worker communication

- **Worker** (`lab2/mr/worker.go`): MapReduce worker process
  - Executes map and reduce tasks assigned by coordinator
  - Handles intermediate file I/O
  - Implements fault tolerance with periodic heartbeats
  - Supports pluggable map and reduce functions
  - Automatic retry on task failures

- **Main Programs** (`lab2/main/`):
  - `mrcoordinator.go`: Entry point for starting the coordinator
  - `mrworker.go`: Entry point for starting worker processes

### Prerequisites

- Go 1.25+ (or use the provided Nix flake)
- Docker (optional, for containerized deployment)

### Configuration

The coordinator and worker can be configured using environment variables:

**Coordinator:**
- `MR_COORDINATOR_PORT`: Port for the coordinator to listen on (default: `1234`)

**Worker:**
- `MR_WORKER_PORT`: Port for the worker to listen on (default: `1235`)
- `MR_COORDINATOR_ADDR`: Address of the coordinator to connect to (default: `127.0.0.1:1234`)

Example:
```bash
# Start coordinator on custom port
MR_COORDINATOR_PORT=8000 go run main/mrcoordinator.go main/pg-*.txt

# Start worker connecting to custom coordinator
MR_COORDINATOR_ADDR=192.168.1.100:8000 go run main/mrworker.go mrapps/wc.so
```

### Running Locally

**Start the Coordinator:**
```bash
cd lab2
go run main/mrcoordinator.go main/pg-*.txt
```

**Start Workers:**
```bash
cd lab2
go run main/mrworker.go mrapps/wc.so
```

You can start multiple workers in separate terminals. The word count plugin (`wc.so`) must be built first:
```bash
cd lab2
go build -buildmode=plugin mrapps/wc.go
```

### Running with Docker

Build and run the coordinator:
```bash
docker build -f lab2/coordinator.Dockerfile -t mr-coordinator .
docker run -p 1234:1234 mr-coordinator
```

Build and run workers:
```bash
docker build -f lab2/worker.Dockerfile -t mr-worker .
docker run --network host mr-worker
```

### Testing

Run the MapReduce tests:
```bash
cd lab2/main
bash test-mr.sh
```

For stress testing multiple times:
```bash
cd lab2/main
bash test-mr-many.sh 20
```

### Architecture

The MapReduce implementation follows a coordinator-worker architecture:

1. **Map Phase**: Workers request map tasks, process input files, and write intermediate key-value pairs partitioned by hash
2. **Reduce Phase**: After all map tasks complete, workers request reduce tasks and merge intermediate values to produce final output
3. **Fault Tolerance**: Coordinator detects failed workers via timeouts and reschedules tasks as needed
4. **Coordination**: All communication between coordinator and workers happens via RPC

Output is written to 'mr-out-1'

## Lab 3: Chord DHT

A distributed hash table implementation based on the Chord protocol. Provides consistent hashing for distributed key-value storage with fault tolerance through replication.

### Features

- **Chord Protocol**: Implements the core Chord lookup algorithm with finger tables for O(log N) lookups
- **File Storage**: Store and retrieve files by name across the distributed ring
- **Replication**: Files are automatically replicated to successor nodes for fault tolerance
- **TLS Security**: Optional TLS encryption for all node communication
- **Per-File Encryption**: Optional AES-256-GCM encryption for individual files

### Command Line Arguments

| Flag | Description |
|------|-------------|
| `-a` | IP address to bind to (required) |
| `-p` | Port to bind to (required) |
| `--ja` | IP address of existing node to join |
| `--jp` | Port of existing node to join |
| `--ts` | Stabilize interval in ms (required, 1-60000) |
| `--tff` | Fix fingers interval in ms (required, 1-60000) |
| `--tcp` | Check predecessor interval in ms (required, 1-60000) |
| `-r` | Number of successors to maintain (required, 1-32) |
| `-i` | Custom node ID (40 hex characters) |
| `-tls` | Enable TLS for secure communication |

### CLI Commands

- `Lookup <filename>` - Look up and display a file
- `StoreFile <path>` - Store a file in the ring
- `StoreFileEnc <path>` - Store a file with encryption (prompts for password)
- `Delete <filename>` - Delete a file from the ring
- `PrintState` - Display node state (self, successors, fingers, stored files)
- `Quit` - Exit the program

### Running Locally

**Create a new ring:**
```bash
cd lab3
go build .
./lab3 -a 127.0.0.1 -p 8000 --ts 1000 --tff 1000 --tcp 1000 -r 3
```

**Join an existing ring:**
```bash
./lab3 -a 127.0.0.1 -p 8001 --ja 127.0.0.1 --jp 8000 --ts 1000 --tff 1000 --tcp 1000 -r 3
```

### Running with Docker

```bash
docker build -f lab3/Dockerfile -t chord .
docker run --net=host chord -a 0.0.0.0 -p 8000 --ts 1000 --tff 1000 --tcp 1000 -r 3
```

### Testing with Multiple Nodes

A test script is provided to easily start multiple nodes in a tmux session:

```bash
cd lab3
./test-chord.sh 5        # Start 5 nodes on ports 8000-8004
./test-chord.sh --tls 3  # Start 3 nodes with TLS enabled
./test-chord.sh 10 9000  # Start 10 nodes starting at port 9000
```

Use tmux controls to navigate between nodes (`Ctrl+b, n/p` to switch windows). Each node runs in its own window where you can issue CLI commands.

### Architecture

1. **Ring Structure**: Nodes are arranged in a logical ring using consistent hashing (SHA-1)
2. **Finger Tables**: Each node maintains O(log N) pointers for efficient routing
3. **Stabilization**: Background tasks maintain ring consistency as nodes join/leave
4. **Replication**: Files are stored on the responsible node and replicated to successors



