package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
)

// Config holds all configuration for a Chord node
type Config struct {
	// Network configuration
	Address  string // IP address to bind to
	Port     int    // Port to bind to
	NodeAddr string // Full address (IP:Port)

	// Join configuration
	JoinAddress string // IP address of existing node to join
	JoinPort    int    // Port of existing node to join

	// Timing configuration (in milliseconds)
	StabilizeInterval        int // Time between stabilize calls
	FixFingersInterval       int // Time between fix_fingers calls
	CheckPredecessorInterval int // Time between check_predecessor calls

	// Chord configuration
	NumSuccessors int    // Number of successors to maintain (r)
	CustomID      string // Optional custom node ID (40 hex chars)

	// Security configuration
	EnableTLS bool // Enable TLS for communication
}

// ParseConfig parses command line arguments and returns a validated Config
func ParseConfig() (*Config, error) {
	cfg := &Config{}

	// Define flags
	flag.StringVar(&cfg.Address, "a", "", "IP address to bind to (required)")
	flag.IntVar(&cfg.Port, "p", 0, "Port to bind to (required)")
	flag.StringVar(&cfg.JoinAddress, "ja", "", "IP address of existing Chord node to join")
	flag.IntVar(&cfg.JoinPort, "jp", 0, "Port of existing Chord node to join")
	flag.IntVar(&cfg.StabilizeInterval, "ts", 0, "Stabilize interval in ms (required, 1-60000)")
	flag.IntVar(&cfg.FixFingersInterval, "tff", 0, "Fix fingers interval in ms (required, 1-60000)")
	flag.IntVar(&cfg.CheckPredecessorInterval, "tcp", 0, "Check predecessor interval in ms (required, 1-60000)")
	flag.IntVar(&cfg.NumSuccessors, "r", 0, "Number of successors (required, 1-32)")
	flag.StringVar(&cfg.CustomID, "i", "", "Custom node ID (40 hex characters)")
	flag.BoolVar(&cfg.EnableTLS, "tls", false, "Enable TLS for secure communication")

	flag.Parse()

	// Validate configuration
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	// Set computed fields
	cfg.NodeAddr = fmt.Sprintf("%s:%d", cfg.Address, cfg.Port)

	return cfg, nil
}

// validate checks that all required configuration is present and valid
func (cfg *Config) validate() error {
	var errors []string

	// Required fields
	if cfg.Address == "" {
		errors = append(errors, "-a (IP address) is required")
	}
	if cfg.Port == 0 {
		errors = append(errors, "-p (port) is required")
	}

	// Timing validation
	if cfg.StabilizeInterval < 1 || cfg.StabilizeInterval > 60000 {
		errors = append(errors, "--ts must be specified and in range [1, 60000]")
	}
	if cfg.FixFingersInterval < 1 || cfg.FixFingersInterval > 60000 {
		errors = append(errors, "--tff must be specified and in range [1, 60000]")
	}
	if cfg.CheckPredecessorInterval < 1 || cfg.CheckPredecessorInterval > 60000 {
		errors = append(errors, "--tcp must be specified and in range [1, 60000]")
	}

	// Successor count validation
	if cfg.NumSuccessors < 1 || cfg.NumSuccessors > 32 {
		errors = append(errors, "-r must be in range [1, 32]")
	}

	// Join arguments validation
	if (cfg.JoinAddress != "" && cfg.JoinPort == 0) || (cfg.JoinAddress == "" && cfg.JoinPort != 0) {
		errors = append(errors, "--ja and --jp must both be specified or both omitted")
	}

	// Custom ID validation
	if cfg.CustomID != "" {
		if err := cfg.validateCustomID(); err != nil {
			errors = append(errors, err.Error())
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("validation errors:\n  - %s", strings.Join(errors, "\n  - "))
	}

	return nil
}

// validateCustomID validates the custom node ID format
func (cfg *Config) validateCustomID() error {
	if len(cfg.CustomID) != 40 {
		return fmt.Errorf("-i must be exactly 40 hex characters")
	}
	for _, c := range cfg.CustomID {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return fmt.Errorf("-i must contain only hex characters [0-9a-fA-F]")
		}
	}
	return nil
}

// IsJoining returns true if this node should join an existing ring
func (cfg *Config) IsJoining() bool {
	return cfg.JoinAddress != ""
}

// JoinAddr returns the full address of the node to join
func (cfg *Config) JoinAddr() string {
	return fmt.Sprintf("%s:%d", cfg.JoinAddress, cfg.JoinPort)
}

// InitSecurity initializes security features based on configuration
func (cfg *Config) InitSecurity() error {
	if cfg.EnableTLS {
		if err := initTLS(); err != nil {
			return fmt.Errorf("failed to initialize TLS: %v", err)
		}
		log.Println("TLS security enabled")
	}

	return nil
}
