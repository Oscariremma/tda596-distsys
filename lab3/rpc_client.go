package main

import (
	"crypto/tls"
	"fmt"
	"math/big"
	"net"
	"net/rpc"
)

// RPC Client Wrappers - Typed functions for making RPC calls to remote nodes

// RemoteFindSuccessor finds the successor of an ID on a remote node (iterative lookup)
func RemoteFindSuccessor(n *Node, id *big.Int) (NodeInfo, error) {
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

// RemoteFindSuccessorRPC calls FindSuccessorRPC on a remote node (single hop)
func RemoteFindSuccessorRPC(address string, id *big.Int) (*FindSuccessorReply, error) {
	args := &FindSuccessorArgs{ID: id}
	reply := &FindSuccessorReply{}
	if err := call(address, "Node.FindSuccessorRPC", args, reply); err != nil {
		return nil, err
	}
	return reply, nil
}

// RemoteGetPredecessor gets the predecessor of a remote node
func RemoteGetPredecessor(address string) (*NodeInfo, error) {
	args := &GetPredecessorArgs{}
	reply := &GetPredecessorReply{}
	if err := call(address, "Node.GetPredecessor", args, reply); err != nil {
		return nil, err
	}
	return reply.Predecessor, nil
}

// RemoteNotify notifies a remote node about us
func RemoteNotify(address string, node NodeInfo) error {
	args := &NotifyArgs{Node: node}
	reply := &NotifyReply{}
	return call(address, "Node.Notify", args, reply)
}

// RemotePing pings a remote node to check if it's alive
func RemotePing(address string) error {
	args := &PingArgs{}
	reply := &PingReply{}
	return call(address, "Node.Ping", args, reply)
}

// RemoteGetSuccessors gets the successor list from a remote node
func RemoteGetSuccessors(address string) ([]*NodeInfo, error) {
	args := &GetSuccessorsArgs{}
	reply := &GetSuccessorsReply{}
	if err := call(address, "Node.GetSuccessors", args, reply); err != nil {
		return nil, err
	}
	return reply.Successors, nil
}

// RemoteStoreFile stores a file on a remote node
func RemoteStoreFile(address string, key *big.Int, filename string, content []byte, encrypted, isReplica bool) (bool, error) {
	args := &StoreFileArgs{
		Key:       key,
		Filename:  filename,
		Content:   content,
		Encrypted: encrypted,
		IsReplica: isReplica,
	}
	reply := &StoreFileReply{}
	if err := call(address, "Node.StoreFile", args, reply); err != nil {
		return false, err
	}
	return reply.Success, nil
}

// RemoteGetFile gets a file from a remote node
func RemoteGetFile(address string, key *big.Int) (*GetFileReply, error) {
	args := &GetFileArgs{Key: key}
	reply := &GetFileReply{}
	if err := call(address, "Node.GetFile", args, reply); err != nil {
		return nil, err
	}
	return reply, nil
}

// RemoteDeleteFile deletes a file from a remote node
func RemoteDeleteFile(address string, key *big.Int) (bool, error) {
	args := &DeleteFileArgs{Key: key, IsReplica: false}
	reply := &DeleteFileReply{}
	if err := call(address, "Node.DeleteFile", args, reply); err != nil {
		return false, err
	}
	return reply.Success, nil
}

// RemoteDeleteFileReplica deletes a file replica from a remote node (no cascade)
func RemoteDeleteFileReplica(address string, key *big.Int) (bool, error) {
	args := &DeleteFileArgs{Key: key, IsReplica: true}
	reply := &DeleteFileReply{}
	if err := call(address, "Node.DeleteFile", args, reply); err != nil {
		return false, err
	}
	return reply.Success, nil
}

// RemoteTransferKeys requests key transfer from a remote node
func RemoteTransferKeys(address string, newNode NodeInfo) (*TransferKeysReply, error) {
	args := &TransferKeysArgs{NewNode: newNode}
	reply := &TransferKeysReply{}
	if err := call(address, "Node.TransferKeys", args, reply); err != nil {
		return nil, err
	}
	return reply, nil
}

// RemoteReplicate replicates a file to a remote node
func RemoteReplicate(address string, key *big.Int, data *FileData) (bool, error) {
	args := &ReplicateArgs{
		Key:  key,
		Data: data,
	}
	reply := &ReplicateReply{}
	if err := call(address, "Node.Replicate", args, reply); err != nil {
		return false, err
	}
	return reply.Success, nil
}

// RemoteHasFile checks if a remote node has a specific file
func RemoteHasFile(address string, key *big.Int) (bool, error) {
	args := &HasFileArgs{Key: key}
	reply := &HasFileReply{}
	if err := call(address, "Node.HasFile", args, reply); err != nil {
		return false, err
	}
	return reply.HasFile, nil
}

// GetFileWithFaultTolerance attempts to get a file from primary node,
// and if that fails, tries successor replicas. Returns the reply, the node that served the file, and whether it was a replica.
func GetFileWithFaultTolerance(n *Node, key *big.Int) (*GetFileReply, NodeInfo, bool, error) {
	// First, find the primary responsible node
	successor, err := RemoteFindSuccessor(n, key)
	if err != nil {
		return nil, NodeInfo{}, false, fmt.Errorf("failed to find successor: %v", err)
	}

	reply, err := RemoteGetFile(successor.Address, key)
	if err == nil && reply.Found {
		return reply, successor, false, nil
	}

	// Primary failed or didn't have the file, try to find replicas
	successors, err := RemoteGetSuccessors(successor.Address)
	if err != nil {
		// Primary might be completely dead, find its successors via another route
		n.mu.RLock()
		for _, s := range n.Successors {
			if s != nil && s.Address != "" && s.Address != successor.Address {
				successors, _ = RemoteGetSuccessors(s.Address)
				if len(successors) > 0 {
					break
				}
			}
		}
		n.mu.RUnlock()
	}

	// Try each successor (they should have replicas)
	for _, s := range successors {
		if s == nil || s.Address == "" {
			continue
		}
		reply, err := RemoteGetFile(s.Address, key)
		if err == nil && reply.Found {
			return reply, *s, true, nil
		}
	}

	// Also check our own successors as potential replica holders
	n.mu.RLock()
	nodeSuccessors := make([]*NodeInfo, len(n.Successors))
	copy(nodeSuccessors, n.Successors)
	n.mu.RUnlock()

	for _, s := range nodeSuccessors {
		if s == nil || s.Address == "" || s.Address == successor.Address {
			continue
		}
		reply, err := RemoteGetFile(s.Address, key)
		if err == nil && reply.Found {
			return reply, *s, true, nil
		}
	}

	return nil, NodeInfo{}, false, fmt.Errorf("file not found: key=%s", intToHex(key))
}

// StoreFileWithReplication stores a file and ensures it's replicated
func StoreFileWithReplication(n *Node, filename string, content []byte, encrypted bool) (*big.Int, NodeInfo, error) {
	key := hashString(filename)

	successor, err := RemoteFindSuccessor(n, key)
	if err != nil {
		return nil, NodeInfo{}, fmt.Errorf("failed to find successor: %v", err)
	}

	success, err := RemoteStoreFile(successor.Address, key, filename, content, encrypted, false)
	if err != nil || !success {
		return nil, NodeInfo{}, fmt.Errorf("failed to store file on primary node: %v", err)
	}

	return key, successor, nil
}

// DeleteFileWithReplication deletes a file from primary and all replicas
func DeleteFileWithReplication(n *Node, filename string) (NodeInfo, error) {
	key := hashString(filename)

	successor, err := RemoteFindSuccessor(n, key)
	if err != nil {
		return NodeInfo{}, fmt.Errorf("failed to find successor: %v", err)
	}

	// Primary will handle replica deletion
	success, err := RemoteDeleteFile(successor.Address, key)
	if err != nil || !success {
		return NodeInfo{}, fmt.Errorf("failed to delete file from primary node: %v", err)
	}

	return successor, nil
}

// call makes an RPC call to the specified address
func call(address string, method string, args interface{}, reply interface{}) error {
	var conn net.Conn
	var err error

	if securityConfig.EnableTLS {
		conn, err = tls.Dial("tcp", address, securityConfig.TLSConfig)
	} else {
		conn, err = net.Dial("tcp", address)
	}

	if err != nil {
		return err
	}

	client := rpc.NewClient(conn)
	defer client.Close()
	return client.Call(method, args, reply)
}
