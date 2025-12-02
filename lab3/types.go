package main

import (
	"math/big"
	"net"
	"sync"
)

// NodeInfo contains all information about a node
type NodeInfo struct {
	ID      *big.Int // Node ID as big.Int
	Address string   // IP:Port
}

// IDHex returns the hex string representation of the node ID (for display only)
func (n *NodeInfo) IDHex() string {
	if n == nil || n.ID == nil {
		return ""
	}
	return intToHex(n.ID)
}

// FileData stores file content with metadata
type FileData struct {
	Filename  string
	Content   []byte
	Encrypted bool
}

// Node represents a Chord node
type Node struct {
	mu          sync.RWMutex
	Info        NodeInfo
	Predecessor *NodeInfo
	Successors  []*NodeInfo          // Successor list
	FingerTable []*NodeInfo          // Finger table
	Bucket      map[string]*FileData // Key as hex string
	R           int                  // Number of successors to maintain
	NextFinger  int                  // Next finger to fix
	listener    net.Listener         // Network listener
}

// NewNode creates a new Chord node with the given configuration
func NewNode(cfg *Config) *Node {
	nodeID := hashString(cfg.NodeAddr)
	if cfg.CustomID != "" {
		nodeID = hexToInt(cfg.CustomID)
	}
	return &Node{
		Info: NodeInfo{
			ID:      nodeID,
			Address: cfg.NodeAddr,
		},
		Successors:  make([]*NodeInfo, cfg.NumSuccessors),
		FingerTable: make([]*NodeInfo, FingerSize),
		Bucket:      make(map[string]*FileData),
		R:           cfg.NumSuccessors,
		NextFinger:  0,
	}
}
