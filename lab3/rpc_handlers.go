package main

import (
	"log"
	"math/big"
)

// FindSuccessorRPC is the RPC handler for find_successor
func (n *Node) FindSuccessorRPC(args *FindSuccessorArgs, reply *FindSuccessorReply) error {
	reply.Found, reply.Node = n.FindSuccessor(args.ID)
	return nil
}

// GetPredecessor returns this node's predecessor
func (n *Node) GetPredecessor(args *GetPredecessorArgs, reply *GetPredecessorReply) error {
	n.mu.RLock()
	defer n.mu.RUnlock()
	reply.Predecessor = n.Predecessor
	return nil
}

// Notify is called when a node thinks it might be our predecessor
func (n *Node) Notify(args *NotifyArgs, reply *NotifyReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.Predecessor == nil {
		n.Predecessor = &args.Node
		return nil
	}

	if between(n.Predecessor.ID, args.Node.ID, n.Info.ID, false) {
		n.Predecessor = &args.Node
	}
	return nil
}

// Ping checks if this node is alive
func (n *Node) Ping(args *PingArgs, reply *PingReply) error {
	return nil
}

// GetSuccessors returns this node's successor list (only non-nil entries)
func (n *Node) GetSuccessors(args *GetSuccessorsArgs, reply *GetSuccessorsReply) error {
	n.mu.RLock()
	defer n.mu.RUnlock()
	// Filter out nil entries - gob can't encode nil pointers in arrays
	reply.Successors = make([]*NodeInfo, 0, len(n.Successors))
	for _, s := range n.Successors {
		if s != nil {
			reply.Successors = append(reply.Successors, s)
		}
	}
	return nil
}

// StoreFile stores a file on this node
func (n *Node) StoreFile(args *StoreFileArgs, reply *StoreFileReply) error {
	keyHex := intToHex(args.Key)

	n.mu.Lock()
	n.Bucket[keyHex] = &FileData{
		Filename:  args.Filename,
		Content:   args.Content,
		Encrypted: args.Encrypted,
	}
	n.mu.Unlock()

	reply.Success = true
	log.Printf("Stored file '%s' with key %s (%d bytes)", args.Filename, keyHex[:16]+"...", len(args.Content))

	// Replicate to successors if this is not already a replica request
	if !args.IsReplica {
		go n.replicateToSuccessors(args.Key, &FileData{
			Filename:  args.Filename,
			Content:   args.Content,
			Encrypted: args.Encrypted,
		})
	}

	return nil
}

// replicateToSuccessors replicates data to successor nodes for fault tolerance
func (n *Node) replicateToSuccessors(key *big.Int, data *FileData) {
	keyHex := intToHex(key)

	n.mu.RLock()
	successors := make([]*NodeInfo, len(n.Successors))
	copy(successors, n.Successors)
	n.mu.RUnlock()

	replicated := 0
	seen := make(map[string]bool)
	seen[n.Info.Address] = true

	for i := 0; i < len(successors) && replicated < ReplicationFactor-1; i++ {
		if successors[i] == nil || successors[i].Address == "" || seen[successors[i].Address] {
			continue
		}
		seen[successors[i].Address] = true

		success, err := RemoteStoreFile(successors[i].Address, key, data.Filename, data.Content, data.Encrypted, true)
		if err == nil && success {
			replicated++
			log.Printf("Replicated key %s to %s", keyHex[:16]+"...", successors[i].Address)
		}
	}
}

// HasFile checks if this node has a specific file
func (n *Node) HasFile(args *HasFileArgs, reply *HasFileReply) error {
	keyHex := intToHex(args.Key)
	n.mu.RLock()
	defer n.mu.RUnlock()
	_, reply.HasFile = n.Bucket[keyHex]
	return nil
}

// GetFile retrieves a file from this node
func (n *Node) GetFile(args *GetFileArgs, reply *GetFileReply) error {
	keyHex := intToHex(args.Key)
	n.mu.RLock()
	defer n.mu.RUnlock()

	data, ok := n.Bucket[keyHex]
	if !ok {
		reply.Found = false
		return nil
	}

	reply.Found = true
	reply.Content = data.Content
	reply.Filename = data.Filename
	reply.Encrypted = data.Encrypted
	return nil
}

// DeleteFile deletes a file from this node and its replicas
func (n *Node) DeleteFile(args *DeleteFileArgs, reply *DeleteFileReply) error {
	keyHex := intToHex(args.Key)

	n.mu.Lock()
	_, exists := n.Bucket[keyHex]
	if exists {
		delete(n.Bucket, keyHex)
	}
	// Get successors for replica deletion
	successors := make([]*NodeInfo, len(n.Successors))
	copy(successors, n.Successors)
	n.mu.Unlock()

	if !exists {
		reply.Success = false
		return nil
	}

	reply.Success = true
	log.Printf("Deleted file with key %s", keyHex[:16]+"...")

	if !args.IsReplica {
		go func() {
			deleted := 0
			seen := make(map[string]bool)
			seen[n.Info.Address] = true

			for i := 0; i < len(successors) && deleted < ReplicationFactor-1; i++ {
				if successors[i] == nil || successors[i].Address == "" || seen[successors[i].Address] {
					continue
				}
				seen[successors[i].Address] = true
				// Tell replica to delete
				RemoteDeleteFileReplica(successors[i].Address, args.Key)
				deleted++
			}
		}()
	}

	return nil
}

// TransferKeys transfers keys that should belong to a new node
func (n *Node) TransferKeys(args *TransferKeysArgs, reply *TransferKeysReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply.Keys = make([]*big.Int, 0)
	reply.Files = make(map[string]*FileData)

	for keyHex, content := range n.Bucket {
		keyInt := hexToInt(keyHex)

		if n.Predecessor != nil {
			if between(n.Predecessor.ID, keyInt, args.NewNode.ID, true) {
				reply.Keys = append(reply.Keys, keyInt)
				reply.Files[keyHex] = content
			}
		} else if !between(args.NewNode.ID, keyInt, n.Info.ID, true) {
			reply.Keys = append(reply.Keys, keyInt)
			reply.Files[keyHex] = content
		}
	}

	// Remove transferred keys
	for keyHex := range reply.Files {
		delete(n.Bucket, keyHex)
		log.Printf("Transferred key %s to new node", keyHex[:16]+"...")
	}

	return nil
}

// GetNodeInfo returns this node's information
func (n *Node) GetNodeInfo(args *struct{}, reply *NodeInfo) error {
	n.mu.RLock()
	defer n.mu.RUnlock()
	*reply = n.Info
	return nil
}
