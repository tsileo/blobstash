package hashutil // import "a4.io/blobstash/pkg/hashutil"

import (
	"fmt"

	"github.com/dchest/blake2b"
)

// ComputeRaw returns the Blake2B hash hex-encoded
func ComputeRaw(data []byte) [32]byte {
	return blake2b.Sum256(data)
}

// Compute returns the Blake2B hash hex-encoded
func Compute(data []byte) string {
	return fmt.Sprintf("%x", blake2b.Sum256(data))
}
