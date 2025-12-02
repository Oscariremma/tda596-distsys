package main

import (
	"log"
)

// Stabilize updates the successor and notifies it
func (n *Node) Stabilize() {
	n.mu.Lock()
	if n.Successors[0] == nil {
		n.mu.Unlock()
		return
	}
	successor := n.Successors[0]
	n.mu.Unlock()

	// Get predecessor of our successor
	pred, err := RemoteGetPredecessor(successor.Address)
	if err != nil {
		// Successor may be dead, try next in list
		n.handleSuccessorFailure()
		return
	}

	n.mu.Lock()
	if pred != nil && n.Successors[0] != nil {
		// If predecessor of successor is between us and our successor, update
		if between(n.Info.ID, pred.ID, n.Successors[0].ID, false) {
			n.Successors[0] = pred
		}
	}
	var currentSuccessor *NodeInfo
	if n.Successors[0] != nil {
		currentSuccessor = n.Successors[0]
	}
	n.mu.Unlock()

	if currentSuccessor == nil {
		return
	}

	// Notify our successor
	RemoteNotify(currentSuccessor.Address, n.Info)

	// Update successor list from successor
	successors, err := RemoteGetSuccessors(currentSuccessor.Address)
	if err == nil {
		n.mu.Lock()
		for i := 1; i < n.R && i-1 < len(successors); i++ {
			if successors[i-1] != nil {
				n.Successors[i] = successors[i-1]
			}
		}
		n.mu.Unlock()
	}
}

// handleSuccessorFailure handles the case when the successor is dead
func (n *Node) handleSuccessorFailure() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for i := 1; i < n.R; i++ {
		if n.Successors[i] != nil && n.Successors[i].Address != "" {
			// Skip if same as failed successor
			if n.Successors[0] != nil && n.Successors[i].Address == n.Successors[0].Address {
				continue
			}
			// Check if this successor is alive
			err := RemotePing(n.Successors[i].Address)
			if err == nil {
				log.Printf("Successor failed, switching to backup: %s", n.Successors[i].Address)
				// Move this successor up, set remaining to nil
				n.Successors[0] = n.Successors[i]
				for j := 1; j < n.R; j++ {
					if i+j < n.R {
						n.Successors[j] = n.Successors[i+j]
					} else {
						n.Successors[j] = nil
					}
				}
				return
			}
		}
	}
}

// FixFingers updates the finger table (0-indexed)
func (n *Node) FixFingers() {
	n.mu.Lock()
	n.NextFinger++
	if n.NextFinger >= FingerSize {
		n.NextFinger = 0
	}
	nextFinger := n.NextFinger
	nodeID := n.Info.ID
	n.mu.Unlock()

	// Calculate the ID for this finger entry: (n + 2^nextFinger) mod 2^m
	fingerID := jump(nodeID, nextFinger)

	// Find successor for this finger
	n.mu.RLock()
	var startAddr string
	if n.Successors[0] != nil {
		startAddr = n.Successors[0].Address
	}
	n.mu.RUnlock()

	if startAddr == "" {
		return
	}

	successor, err := n.findSuccessorRemote(startAddr, fingerID)
	if err != nil {
		return
	}

	n.mu.Lock()
	n.FingerTable[nextFinger] = &successor
	n.mu.Unlock()
}

// CheckPredecessor checks if the predecessor is still alive
func (n *Node) CheckPredecessor() {
	n.mu.RLock()
	pred := n.Predecessor
	n.mu.RUnlock()

	if pred == nil {
		return
	}

	err := RemotePing(pred.Address)
	if err != nil {
		// Predecessor is dead
		n.mu.Lock()
		n.Predecessor = nil
		n.mu.Unlock()
		log.Printf("Predecessor %s is dead, cleared", pred.Address)
	}
}

// isPrimaryForKey checks if this node is the primary (responsible) node for a key
func (n *Node) isPrimaryForKey(keyHex string) bool {
	n.mu.RLock()
	predecessor := n.Predecessor
	n.mu.RUnlock()

	keyInt := hexToInt(keyHex)

	// If we have no predecessor, we're the only node - we're primary for everything
	if predecessor == nil {
		return true
	}

	// We are primary if key is in (predecessor, us]
	return between(predecessor.ID, keyInt, n.Info.ID, true)
}

// RepairReplication checks files where we are the PRIMARY and ensures they have enough replicas
func (n *Node) RepairReplication() {
	n.mu.RLock()

	if len(n.Bucket) == 0 {
		n.mu.RUnlock()
		return
	}

	// Get a snapshot of our files and successors
	files := make(map[string]*FileData)
	for k, v := range n.Bucket {
		files[k] = v
	}

	successors := make([]*NodeInfo, len(n.Successors))
	copy(successors, n.Successors)

	n.mu.RUnlock()

	// Get unique, alive successors
	aliveSuccessors := make([]*NodeInfo, 0, ReplicationFactor-1)
	seen := make(map[string]bool)
	seen[n.Info.Address] = true // Don't include self

	for _, s := range successors {
		if s == nil || s.Address == "" || seen[s.Address] {
			continue
		}
		// Check if successor is alive
		err := RemotePing(s.Address)
		if err == nil {
			aliveSuccessors = append(aliveSuccessors, s)
			seen[s.Address] = true
		}
		if len(aliveSuccessors) >= ReplicationFactor-1 {
			break
		}
	}

	// For each file, check if we're the primary and if replication is needed
	for keyHex, data := range files {
		// Only the primary node should manage replication for a key
		if !n.isPrimaryForKey(keyHex) {
			continue
		}

		key := hexToInt(keyHex)
		replicaCount := 0
		needsReplication := make([]*NodeInfo, 0)

		for _, s := range aliveSuccessors {
			hasFile, err := RemoteHasFile(s.Address, key)
			if err != nil {
				// Node unreachable, might need replication
				continue
			}
			if hasFile {
				replicaCount++
			} else {
				needsReplication = append(needsReplication, s)
			}
		}

		// If under-replicated, replicate to nodes that don't have it
		needed := ReplicationFactor - 1 - replicaCount
		if needed > 0 && len(needsReplication) > 0 {
			log.Printf("File %s is under-replicated (%d/%d), repairing...",
				keyHex[:16]+"...", replicaCount+1, ReplicationFactor)

			for i := 0; i < needed && i < len(needsReplication); i++ {
				success, err := RemoteStoreFile(
					needsReplication[i].Address,
					key,
					data.Filename,
					data.Content,
					data.Encrypted,
					true,
				)
				if err == nil && success {
					log.Printf("Repaired replication: %s -> %s",
						keyHex[:16]+"...", needsReplication[i].Address)
				}
			}
		}
	}
}
