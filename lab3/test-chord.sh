#!/bin/bash

# Chord DHT Test Script
# Usage: ./test-chord.sh [options] <num_nodes> [base_port]
#
# This script starts multiple Chord nodes in a tmux session, allowing you to
# interact with each node separately.
#
# Controls:
#   Ctrl+b, n     - Next node window
#   Ctrl+b, p     - Previous node window
#   Ctrl+b, 0-9   - Switch to specific node window
#   Ctrl+b, d     - Detach from session (nodes keep running)
#   Ctrl+b, x     - Kill current pane
#   Type 'quit'   - Gracefully stop a node
#
# To reattach: tmux attach -t chord-test
# To kill all: tmux kill-session -t chord-test

set -e

# Parse options
ENABLE_TLS=false
while [[ $# -gt 0 ]]; do
  case $1 in
    --tls)
      ENABLE_TLS=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      break
      ;;
  esac
done

# Default values
NUM_NODES=${1:-10}
BASE_PORT=${2:-8000}
SESSION_NAME="chord-test"
IP="127.0.0.1"

# Timing settings (in ms)
TS=1000      # Stabilize interval
TFF=500      # Fix fingers interval
TCP=2000     # Check predecessor interval
R=3          # Number of successors

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check dependencies
check_dependencies() {
    if ! command -v tmux &> /dev/null; then
        echo -e "${RED}Error: tmux is not installed${NC}"
        echo "Install it with: sudo apt install tmux (Debian/Ubuntu)"
        echo "                 brew install tmux (macOS)"
        echo "                 nix-shell -p tmux (NixOS)"
        exit 1
    fi
    

    echo -e "${GREEN}Building chord binary...${NC}"
    go build -o chord .
    if [[ ! -f "./chord" ]]; then
        echo -e "${RED}Error: Failed to build chord binary${NC}"
        exit 1
    fi

}

# Print usage
usage() {
    echo "Usage: $0 [options] <num_nodes> [base_port]"
    echo ""
    echo "Arguments:"
    echo "  num_nodes   Number of Chord nodes to start (default: 10)"
    echo "  base_port   Starting port number (default: 8000)"
    echo ""
    echo "Options:"
    echo "  --tls        Enable TLS for secure communication"
    echo ""
    echo "Example:"
    echo "  $0 5 9000    # Start 5 nodes on ports 9000-9004"
    echo "  $0 --tls 3   # Start 3 nodes with TLS enabled"
    echo ""
    echo "tmux Controls:"
    echo "  Ctrl+b, n     - Next node window"
    echo "  Ctrl+b, p     - Previous node window"
    echo "  Ctrl+b, 0-9   - Switch to specific node (0=control, 1+=nodes)"
    echo "  Ctrl+b, d     - Detach (nodes keep running)"
    echo ""
    echo "To reattach:    tmux attach -t ${SESSION_NAME}"
    echo "To kill all:    tmux kill-session -t ${SESSION_NAME}"
}

# Kill existing session if it exists
cleanup_existing() {
    if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
        echo -e "${YELLOW}Killing existing session '${SESSION_NAME}'...${NC}"
        tmux kill-session -t "$SESSION_NAME"
        sleep 1
    fi
}

# Start the chord nodes in tmux
start_nodes() {
    echo -e "${GREEN}Starting Chord DHT test cluster${NC}"
    echo "  Nodes: $NUM_NODES"
    echo "  Ports: $BASE_PORT - $((BASE_PORT + NUM_NODES - 1))"
    echo ""

    # Create a temporary banner file
    banner_file=$(mktemp)
    cat > "$banner_file" << EOF
╔════════════════════════════════════════════════════════════════╗
║                    Chord DHT Test Cluster                      ║
╠════════════════════════════════════════════════════════════════╣
║  Nodes: $NUM_NODES (ports $BASE_PORT - $((BASE_PORT + NUM_NODES - 1)))
║                                                                ║
║  tmux Controls:                                                ║
║    Ctrl+b, n     - Next node window                            ║
║    Ctrl+b, p     - Previous node window                        ║
║    Ctrl+b, 1-9   - Switch to specific node                     ║
║    Ctrl+b, d     - Detach (nodes keep running)                 ║
║                                                                ║
║  Chord Commands (in node windows):                             ║
║    PrintState              - Show node state                   ║
║    Lookup <filename>       - Find file                         ║
║    StoreFile <path>        - Store a file                      ║
║    StoreFileEnc <path>     - Store encrypted file              ║
║    Delete <filename>       - Delete a file                     ║
║    Quit                    - Stop the node                     ║
║                                                                ║
║  To kill all nodes: tmux kill-session -t $SESSION_NAME           
╚════════════════════════════════════════════════════════════════╝

Press Ctrl+b, then 1 to go to first node
EOF

    # Create new tmux session with control window
    tmux new-session -d -s "$SESSION_NAME" -n "control"

    # Set up the control window with info
    tmux send-keys -t "$SESSION_NAME:control" "cat '$banner_file'; rm '$banner_file'" Enter

    # Start the first node (creates new ring)
    local first_port=$BASE_PORT
    tmux new-window -t "$SESSION_NAME" -n "node1:$first_port"
    tmux send-keys -t "$SESSION_NAME:node1:$first_port" \
        "./chord -a $IP -p $first_port --ts $TS --tff $TFF --tcp $TCP -r $R ${ENABLE_TLS:+--tls} && echo 'Exited. Closing terminal in 10 seconds' && sleep 10 && exit" Enter

    echo -e "  ${GREEN}✓${NC} Node 1 started on port $first_port (ring creator)"
    
    # Give the first node time to start
    sleep 1

    # Start remaining nodes (join the ring)
    for ((i = 2; i <= NUM_NODES; i++)); do
        local port=$((BASE_PORT + i - 1))
        local window_name="node$i:$port"
        
        tmux new-window -t "$SESSION_NAME" -n "$window_name"
        tmux send-keys -t "$SESSION_NAME:$window_name" \
            "./chord -a $IP -p $port --ja $IP --jp $first_port --ts $TS --tff $TFF --tcp $TCP -r $R ${ENABLE_TLS:+--tls} && echo 'Exited. Closing terminal in 10 seconds' && sleep 10 && exit" Enter

        echo -e "  ${GREEN}✓${NC} Node $i started on port $port (joining via $first_port)"
        
        # Small delay between node starts to prevent overwhelming the ring
        sleep 0.5
    done

    echo ""
    echo -e "${GREEN}All nodes started!${NC}"
    echo ""
    echo "Attaching to tmux session in 5 sec..."
    echo "  - Use Ctrl+b, n/p to switch between nodes"
    echo "  - Use Ctrl+b, d to detach (nodes keep running)"
    echo "  - Use 'tmux kill-session -t $SESSION_NAME' to stop all nodes"
    echo ""
    
    # Give nodes time to stabilize before attaching
    sleep 5
    
    # Attach to the session
    tmux select-window -t "$SESSION_NAME:control"
    tmux attach -t "$SESSION_NAME"
}

# Main
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    usage
    exit 0
fi

if [[ $NUM_NODES -lt 1 ]]; then
    echo -e "${RED}Error: Number of nodes must be at least 1${NC}"
    exit 1
fi

if [[ $NUM_NODES -gt 20 ]]; then
    echo -e "${YELLOW}Warning: Starting $NUM_NODES nodes. This may use significant resources.${NC}"
    read -p "Continue? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 0
    fi
fi

cleanup_existing
check_dependencies
start_nodes
