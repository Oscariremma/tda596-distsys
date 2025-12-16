package main

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"math/big"
)

const (
	KeySize    = sha1.Size * 8 // 160 bits for SHA1
	MaxSteps   = 32            // Maximum number of hops for lookup
	FingerSize = 160           // Size of finger table
)

var (
	two     = big.NewInt(2)
	hashMod = new(big.Int).Exp(two, big.NewInt(KeySize), nil)
)

// hashString computes the SHA1 hash of a string and returns it as a big.Int
func hashString(s string) *big.Int {
	hasher := sha1.New()
	hasher.Write([]byte(s))
	return new(big.Int).SetBytes(hasher.Sum(nil))
}

// hexToInt converts a hex string to a big.Int
func hexToInt(hexStr string) *big.Int {
	bytes, _ := hex.DecodeString(hexStr)
	return new(big.Int).SetBytes(bytes)
}

// intToHex converts a big.Int to a 40-character hex string
func intToHex(i *big.Int) string {
	return fmt.Sprintf("%040x", i)
}

// between checks if elt is in (start, end] (if inclusive) or (start, end) (if not inclusive)
// Handles the circular nature of the Chord ring
func between(start, elt, end *big.Int, inclusive bool) bool {
	if end.Cmp(start) > 0 {
		// Normal case: start < end
		result := start.Cmp(elt) < 0 && elt.Cmp(end) < 0
		if inclusive {
			result = result || elt.Cmp(end) == 0
		}
		return result
	}
	// Wrap-around case: end <= start
	result := start.Cmp(elt) < 0 || elt.Cmp(end) < 0
	if inclusive {
		result = result || elt.Cmp(end) == 0
	}
	return result
}

// jump computes the finger table entry for 0-indexed finger: (n + 2^fingerIndex) mod 2^m
func jump(nodeID *big.Int, fingerIndex int) *big.Int {
	jumpVal := new(big.Int).Exp(two, big.NewInt(int64(fingerIndex)), nil)
	sum := new(big.Int).Add(nodeID, jumpVal)
	return new(big.Int).Mod(sum, hashMod)
}
