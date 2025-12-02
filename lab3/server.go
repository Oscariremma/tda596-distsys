package main

import (
	"crypto/tls"
	"log"
	"net"
	"net/rpc"
	"time"
)

// startServer starts the RPC server
func startServer(node *Node, addr string) error {
	if err := rpc.Register(node); err != nil {
		return err
	}

	var listener net.Listener
	var err error

	if securityConfig.EnableTLS {
		listener, err = tls.Listen("tcp", addr, securityConfig.TLSConfig)
		if err != nil {
			return err
		}
		log.Println("TLS enabled for RPC connections")
	} else {
		listener, err = net.Listen("tcp", addr)
		if err != nil {
			return err
		}
	}

	node.SetListener(listener)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				// Server might be shutting down
				log.Printf("Accept error: %v", err)
				continue
			}
			go rpc.ServeConn(conn)
		}
	}()

	return nil
}

// runPeriodicTasks runs the stabilization tasks
func runPeriodicTasks(node *Node, cfg *Config, stop chan struct{}) {
	tsTicker := time.NewTicker(time.Duration(cfg.StabilizeInterval) * time.Millisecond)
	tffTicker := time.NewTicker(time.Duration(cfg.FixFingersInterval) * time.Millisecond)
	tcpTicker := time.NewTicker(time.Duration(cfg.CheckPredecessorInterval) * time.Millisecond)
	// Repair replication less frequently (every 5 * stabilize interval)
	repairTicker := time.NewTicker(time.Duration(cfg.StabilizeInterval*5) * time.Millisecond)

	for {
		select {
		case <-tsTicker.C:
			node.Stabilize()
		case <-tffTicker.C:
			node.FixFingers()
		case <-tcpTicker.C:
			node.CheckPredecessor()
		case <-repairTicker.C:
			node.RepairReplication()
		case <-stop:
			tsTicker.Stop()
			tffTicker.Stop()
			tcpTicker.Stop()
			repairTicker.Stop()
			return
		}
	}
}

// SetListener sets the network listener for the node
func (n *Node) SetListener(l net.Listener) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.listener = l
}

// GetListener returns the network listener
func (n *Node) GetListener() net.Listener {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.listener
}

// Close shuts down the node
func (n *Node) Close() {
	if l := n.GetListener(); l != nil {
		l.Close()
	}
}
