package main

import (
	"fmt"
	"log"
	"math/big"
)

// Replication factor for fault tolerance
const ReplicationFactor = 3

// Create initializes a new Chord ring
func (n *Node) Create() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.Predecessor = nil
	// Single node ring: first successor points to self, rest are nil
	n.Successors[0] = &n.Info
	for i := 1; i < n.R; i++ {
		n.Successors[i] = nil
	}

	n.Bucket = make(map[string]*FileData)
}

// Join joins an existing Chord ring via the node at existingAddr
func (n *Node) Join(existingAddr string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.Predecessor = nil
	n.Bucket = make(map[string]*FileData)

	// Find our successor through the existing node
	successor, err := n.findSuccessorRemote(existingAddr, n.Info.ID)
	if err != nil {
		return fmt.Errorf("failed to find successor: %v", err)
	}

	// Initialize successor list with our immediate successor
	n.Successors[0] = &successor

	// Get the successor's successor list to populate ours
	successors, err := RemoteGetSuccessors(successor.Address)
	if err == nil {
		for i := 1; i < n.R && i-1 < len(successors); i++ {
			if successors[i-1] != nil {
				n.Successors[i] = successors[i-1]
			}
		}
	}

	go n.requestKeyTransfer(successor)

	return nil
}

// requestKeyTransfer requests keys that should belong to this node from the successor
func (n *Node) requestKeyTransfer(successor NodeInfo) {
	reply, err := RemoteTransferKeys(successor.Address, n.Info)
	if err != nil {
		log.Printf("Warning: Failed to transfer keys from successor: %v", err)
		return
	}

	n.mu.Lock()
	for keyHex, fileData := range reply.Files {
		n.Bucket[keyHex] = fileData
		log.Printf("Received key %s from successor", keyHex[:16]+"...")
	}
	n.mu.Unlock()
}

// findSuccessorRemote finds the successor of an ID by querying a remote node
func (n *Node) findSuccessorRemote(addr string, id *big.Int) (NodeInfo, error) {
	currentAddr := addr
	for i := 0; i < MaxSteps; i++ {
		reply, err := RemoteFindSuccessorRPC(currentAddr, id)
		if err != nil {
			return NodeInfo{}, err
		}
		if reply.Found {
			return reply.Node, nil
		}
		if reply.Node.Address == currentAddr {
			// Stuck in a loop
			return reply.Node, nil
		}
		currentAddr = reply.Node.Address
	}
	return NodeInfo{}, fmt.Errorf("max steps exceeded in findSuccessorRemote")
}

// FindSuccessor finds the successor of an ID (local operation)
func (n *Node) FindSuccessor(id *big.Int) (bool, NodeInfo) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	// Must have at least one successor
	if n.Successors[0] == nil {
		return true, n.Info
	}

	// Check if id is in (n, successor]
	if between(n.Info.ID, id, n.Successors[0].ID, true) {
		return true, *n.Successors[0]
	}

	// Otherwise, find the closest preceding node
	return false, n.closestPrecedingNode(id)
}

// closestPrecedingNode finds the closest preceding node for an ID
func (n *Node) closestPrecedingNode(id *big.Int) NodeInfo {
	// Check finger table from highest to lowest (0-indexed)
	for i := FingerSize - 1; i >= 0; i-- {
		if n.FingerTable[i] == nil {
			continue
		}
		if between(n.Info.ID, n.FingerTable[i].ID, id, false) {
			return *n.FingerTable[i]
		}
	}

	// Check successor list
	for i := n.R - 1; i >= 0; i-- {
		if n.Successors[i] == nil {
			continue
		}
		if between(n.Info.ID, n.Successors[i].ID, id, false) {
			return *n.Successors[i]
		}
	}

	// Return first non-nil successor or self
	if n.Successors[0] != nil {
		return *n.Successors[0]
	}
	return n.Info
}

// Lookup performs a complete lookup for a key
func (n *Node) Lookup(key string) (NodeInfo, error) {
	keyID := hashString(key)
	return n.LookupByID(keyID)
}

// LookupByID performs a lookup by ID
func (n *Node) LookupByID(id *big.Int) (NodeInfo, error) {
	// First check if we are responsible
	found, node := n.FindSuccessor(id)
	if found {
		if node.ID.Cmp(n.Info.ID) == 0 {
			return n.Info, nil
		}
		return node, nil
	}

	n.mu.RLock()
	var startAddr string
	if n.Successors[0] != nil {
		startAddr = n.Successors[0].Address
	}
	n.mu.RUnlock()

	if startAddr == "" {
		return n.Info, nil
	}

	return n.findSuccessorRemote(startAddr, id)
}
