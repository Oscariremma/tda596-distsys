package main

import (
	"encoding/gob"
	"log"
	"math/big"
	"os"
)

func init() {
	// Register types with gob for RPC encoding
	gob.Register(&big.Int{})
}

func main() {
	cfg, err := ParseConfig()
	if err != nil {
		log.Fatalf("Configuration error: %v", err)
	}

	if err := cfg.InitSecurity(); err != nil {
		log.Fatalf("Security initialization error: %v", err)
	}

	node := NewNode(cfg)

	if err := startServer(node, cfg.NodeAddr); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
	log.Printf("Chord node started at %s with ID %s", cfg.NodeAddr, node.Info.ID)

	// Create or join ring
	if cfg.IsJoining() {
		if err := node.Join(cfg.JoinAddr()); err != nil {
			log.Fatalf("Failed to join ring: %v", err)
		}
		log.Printf("Joined Chord ring via %s", cfg.JoinAddr())
	} else {
		node.Create()
		log.Println("Created new Chord ring")
	}

	// Periodic stabilization tasks
	stopChan := make(chan struct{})
	go runPeriodicTasks(node, cfg, stopChan)

	cli := NewCLI(node, stopChan)
	cli.Run()

	os.Exit(0)
}
