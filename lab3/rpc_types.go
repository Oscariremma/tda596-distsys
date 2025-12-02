package main

import "math/big"

// RPC Request/Response Types for Chord protocol

// FindSuccessorArgs is the argument for FindSuccessor RPC
type FindSuccessorArgs struct {
	ID *big.Int
}

// FindSuccessorReply is the reply for FindSuccessor RPC
type FindSuccessorReply struct {
	Found bool
	Node  NodeInfo
}

// GetPredecessorArgs is the argument for GetPredecessor RPC
type GetPredecessorArgs struct{}

// GetPredecessorReply is the reply for GetPredecessor RPC
type GetPredecessorReply struct {
	Predecessor *NodeInfo
}

// NotifyArgs is the argument for Notify RPC
type NotifyArgs struct {
	Node NodeInfo
}

// NotifyReply is the reply for Notify RPC
type NotifyReply struct{}

// PingArgs is the argument for Ping RPC
type PingArgs struct{}

// PingReply is the reply for Ping RPC
type PingReply struct {
}

// GetSuccessorsArgs is the argument for GetSuccessors RPC
type GetSuccessorsArgs struct{}

// GetSuccessorsReply is the reply for GetSuccessors RPC
type GetSuccessorsReply struct {
	Successors []*NodeInfo
}

// StoreFileArgs is the argument for StoreFile RPC
type StoreFileArgs struct {
	Key       *big.Int
	Filename  string
	Content   []byte
	Encrypted bool
	IsReplica bool // Indicates if this is a replication request
}

// StoreFileReply is the reply for StoreFile RPC
type StoreFileReply struct {
	Success bool
}

// GetFileArgs is the argument for GetFile RPC
type GetFileArgs struct {
	Key *big.Int
}

// GetFileReply is the reply for GetFile RPC
type GetFileReply struct {
	Found     bool
	Content   []byte
	Filename  string
	Encrypted bool
}

// DeleteFileArgs is the argument for DeleteFile RPC
type DeleteFileArgs struct {
	Key       *big.Int
	IsReplica bool // Indicates if this is a replica deletion (don't cascade)
}

// DeleteFileReply is the reply for DeleteFile RPC
type DeleteFileReply struct {
	Success bool
}

// TransferKeysArgs is the argument for TransferKeys RPC
type TransferKeysArgs struct {
	NewNode NodeInfo
}

// TransferKeysReply is the reply for TransferKeys RPC
type TransferKeysReply struct {
	Keys  []*big.Int
	Files map[string]*FileData // keyed by hex string for serialization
}

// ReplicateArgs is the argument for Replicate RPC
type ReplicateArgs struct {
	Key  *big.Int
	Data *FileData
}

// ReplicateReply is the reply for Replicate RPC
type ReplicateReply struct {
	Success bool
}

// HasFileArgs is the argument for HasFile RPC
type HasFileArgs struct {
	Key *big.Int
}

// HasFileReply is the reply for HasFile RPC
type HasFileReply struct {
	HasFile bool
}
