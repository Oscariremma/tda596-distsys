package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"syscall"

	"golang.org/x/term"
)

// CLI handles the command-line interface for the Chord node
type CLI struct {
	node   *Node
	reader *bufio.Reader
	stop   chan struct{}
}

// NewCLI creates a new CLI instance
func NewCLI(node *Node, stop chan struct{}) *CLI {
	return &CLI{
		node:   node,
		reader: bufio.NewReader(os.Stdin),
		stop:   stop,
	}
}

// Run starts the command-line interface loop
func (c *CLI) Run() {
	fmt.Println("\nChord Client Ready. Commands: Lookup, StoreFile, StoreFileEnc, PrintState, Quit, Help")

	for {
		fmt.Print("> ")
		input, err := c.reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				log.Println("STDIN closed, continuing in background...")
				select {}
			}
			log.Printf("Error reading input: %v", err)
			continue
		}

		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		parts := strings.SplitN(input, " ", 2)
		command := strings.ToLower(parts[0])

		switch command {
		case "lookup":
			c.handleLookup(parts)
		case "storefile":
			c.handleStoreFile(parts)
		case "storefileenc":
			c.handleStoreFileEnc(parts)
		case "delete":
			c.handleDelete(parts)
		case "printstate":
			c.node.PrintState()
		case "quit", "exit":
			c.handleQuit()
			return
		case "help":
			c.printHelp()
		default:
			fmt.Printf("Unknown command: %s. Type 'help' for available commands.\n", command)
		}
	}
}

func (c *CLI) handleLookup(parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: Lookup <filename>")
		return
	}
	filename := parts[1]
	key := hashString(filename)

	reply, sourceNode, isReplica, err := GetFileWithFaultTolerance(c.node, key)
	if err != nil {
		fmt.Printf("Error during lookup: %v\n", err)
		return
	}

	fmt.Printf("Key: %s\n", intToHex(key))

	if reply.Found {
		// Print which node served the file
		nodeType := "primary"
		if isReplica {
			nodeType = "replica"
		}
		fmt.Printf("Retrieved from %s node %s (%s)\n", nodeType, sourceNode.IDHex()[:16]+"...", sourceNode.Address)

		content := reply.Content

		// Decrypt if file is encrypted
		if reply.Encrypted {
			fmt.Print("File is encrypted. Enter decryption password: ")
			password, err := term.ReadPassword(int(syscall.Stdin))
			fmt.Println()
			if err != nil {
				fmt.Printf("Error reading password: %v\n", err)
				return
			}
			fileKey := deriveKey(string(password))
			decrypted, err := decrypt(content, fileKey)
			if err != nil {
				fmt.Printf("Error decrypting file: %v\n", err)
				return
			}
			content = decrypted
		}

		fmt.Printf("Filename: %s\n", reply.Filename)
		fmt.Printf("Content:\n%s\n", string(content))
	} else {
		fmt.Println("File not found")
	}
}

func (c *CLI) handleStoreFile(parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: StoreFile <filepath>")
		return
	}
	filepath := parts[1]

	content, err := os.ReadFile(filepath)
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		return
	}

	filename := filepath
	if idx := strings.LastIndex(filepath, "/"); idx != -1 {
		filename = filepath[idx+1:]
	}

	key, primary, err := StoreFileWithReplication(c.node, filename, content, false)
	if err != nil {
		fmt.Printf("Error storing file: %v\n", err)
		return
	}

	fmt.Printf("Stored '%s' with key %s on primary node %s (%s)\n", filename, intToHex(key)[:16]+"...", primary.IDHex()[:16]+"...", primary.Address)
}

// handleStoreFileEnc stores a file with per-file encryption
func (c *CLI) handleStoreFileEnc(parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: StoreFileEnc <filepath>")
		return
	}
	filepath := parts[1]

	content, err := os.ReadFile(filepath)
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		return
	}

	filename := filepath
	if idx := strings.LastIndex(filepath, "/"); idx != -1 {
		filename = filepath[idx+1:]
	}

	fmt.Print("Enter encryption password: ")
	password, err := term.ReadPassword(int(syscall.Stdin))
	fmt.Println()
	if err != nil {
		fmt.Printf("Error reading password: %v\n", err)
		return
	}

	fmt.Print("Confirm password: ")
	password2, err := term.ReadPassword(int(syscall.Stdin))
	fmt.Println()
	if err != nil {
		fmt.Printf("Error reading password: %v\n", err)
		return
	}

	if string(password) != string(password2) {
		fmt.Println("Passwords do not match")
		return
	}

	fileKey := deriveKey(string(password))
	encContent, err := encrypt(content, fileKey)
	if err != nil {
		fmt.Printf("Error encrypting file: %v\n", err)
		return
	}

	key, primary, err := StoreFileWithReplication(c.node, filename, encContent, true)
	if err != nil {
		fmt.Printf("Error storing file: %v\n", err)
		return
	}

	fmt.Printf("Stored '%s' with key %s on primary node %s (%s) (encrypted with file-specific password)\n", filename, intToHex(key)[:16]+"...", primary.IDHex()[:16]+"...", primary.Address)
}

func (c *CLI) handleDelete(parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: Delete <filename>")
		return
	}
	filename := parts[1]

	// Use the high-level delete function with replication
	primary, err := DeleteFileWithReplication(c.node, filename)
	if err != nil {
		fmt.Printf("Error deleting file: %v\n", err)
		return
	}

	fmt.Printf("Deleted '%s' from the Chord ring via primary node %s (%s)\n", filename, primary.IDHex()[:16]+"...", primary.Address)
}

func (c *CLI) handleQuit() {
	fmt.Println("Shutting down...")
	close(c.stop)
	c.node.Close()
}

func (c *CLI) printHelp() {
	fmt.Println("Commands:")
	fmt.Println("  Lookup <filename>      - Look up a file by name and display its content")
	fmt.Println("  StoreFile <path>       - Store a local file in the Chord ring")
	fmt.Println("  StoreFileEnc <path>    - Store a file with encryption (prompts for password)")
	fmt.Println("  Delete <filename>      - Delete a file from the Chord ring")
	fmt.Println("  PrintState             - Display node state (self, successors, fingers)")
	fmt.Println("  Quit                   - Exit the program")
	fmt.Println("\nSecurity options (command line):")
	fmt.Println("  -tls                   - Enable TLS for secure communication")
	fmt.Println("\nFault tolerance:")
	fmt.Println("  Files are automatically replicated to successor nodes.")
	fmt.Println("  If the primary node fails, files can be retrieved from replicas.")
}

// PrintState prints the current state of the node
func (n *Node) PrintState() {
	n.mu.RLock()
	defer n.mu.RUnlock()

	fmt.Println("\n=== Node State ===")
	fmt.Printf("Self: %s (%s)\n", n.Info.IDHex()[:16]+"...", n.Info.Address)

	fmt.Println("\nStored Files:")
	if len(n.Bucket) == 0 {
		fmt.Println("  (none)")
	} else {
		for key, data := range n.Bucket {
			encStr := ""
			if data.Encrypted {
				encStr = " [encrypted]"
			}
			fmt.Printf("  %s -> %s (%d bytes)%s\n", key[:16]+"...", data.Filename, len(data.Content), encStr)
		}
	}

	fmt.Println("\nPredecessor:")
	if n.Predecessor == nil {
		fmt.Println("  (none)")
	} else {
		fmt.Printf("  %s (%s)\n", n.Predecessor.IDHex()[:16]+"...", n.Predecessor.Address)
	}

	fmt.Println("\nSuccessors:")
	for i, succ := range n.Successors {
		if succ == nil {
			fmt.Printf("  [%d] (nil)\n", i)
		} else {
			fmt.Printf("  [%d] %s (%s)\n", i, succ.IDHex()[:16]+"...", succ.Address)
		}
	}

	fmt.Println("\nFinger Table:")
	printed := make(map[string]bool)
	for i := 0; i < FingerSize; i++ {
		finger := n.FingerTable[i]
		if finger == nil || finger.Address == "" {
			continue
		}
		idHex := finger.IDHex()
		if !printed[idHex] {
			fmt.Printf("  [%d] %s (%s)\n", i, idHex[:16]+"...", finger.Address)
			printed[idHex] = true
		}
	}
	fmt.Println()
}
