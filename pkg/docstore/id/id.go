/*

Package id implements a MongoDB ObjectId like object.

Cursor stored the timestamp and a hash.

	(<timestamp encoded in big endian uint64> 8 bytes) + (4 random bytes) = 12 bytes

	24 bytes hex-encoded
*/
package id // import "a4.io/blobstash/pkg/docstore/id"

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

// ID hold a hex/byte representation of a timestamp and a hash
type ID struct {
	data    []byte
	hash    string // The hash is not part of the ID but it can be attached to the ID
	flag    byte   // Same here, not part of the ID but can be attched to it for convenience
	version int64  // not part of the ID too
}

// New initializes an ID for the given timestamp
func New(ts int64) (*ID, error) {
	b := make([]byte, 12)
	binary.BigEndian.PutUint64(b[:], uint64(ts))
	randomCompoment := make([]byte, 4)
	if _, err := rand.Read(randomCompoment); err != nil {
		return nil, err
	}
	copy(b[8:], randomCompoment[:])
	return &ID{data: b}, nil
}

// SetFlag allows to temporarily attach the index flag to the ID
func (id *ID) SetFlag(flag byte) {
	id.flag = flag
}

// Flag returns the attached index flag
func (id *ID) Flag() byte {
	return id.flag
}

// SetVersion ties a version to the ID (not stored anywhere else)
func (id *ID) SetVersion(v int64) {
	id.version = v
}

// Version returns the version tied to the ID (if any)
func (id *ID) Version() int64 {
	return id.version
}

// SetHash allow to temporarily attach the document hash to the ID
func (id *ID) SetHash(hash string) {
	id.hash = hash
}

// Hash returns the ataached hash
func (id *ID) Hash() string {
	return id.hash
}

// Raw returns the raw cursor
func (id *ID) Raw() []byte {
	return id.data
}

// String implements Stringer interface
func (id *ID) String() string {
	return hex.EncodeToString(id.data)
}

// Ts returns the timestamp component
func (id *ID) Ts() int64 {
	return int64(binary.BigEndian.Uint64(id.data[0:8]))
}

// MarshalJSON implements the necessary interface to allow an `ID` object to be encoded
// using the standard `encoding/json` package.
func (id *ID) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%v"`, hex.EncodeToString(id.data))), nil
}

// UnmarshalJSON implements the necessary interface to allow an `ID` object to be encoded
// using the standard `encoding/json` package.
func (id *ID) UnmarshalJSON(data []byte) error {
	if len(data) != 26 {
		return fmt.Errorf("invalid Cursor data: %v", string(data))
	}
	b := make([]byte, 12)
	if _, err := hex.Decode(b, data[1:25]); err != nil {
		return fmt.Errorf("invalid Cursor data: %v", string(data))
	}
	*id = ID{data: b}
	return nil
}

// FromHex build an `ID` from an hex encoded string
func FromHex(data string) (*ID, error) {
	if len(data) != 24 {
		return nil, fmt.Errorf("invalid Cursor data: %v", string(data))
	}
	b, err := hex.DecodeString(data)
	if err != nil {
		return nil, fmt.Errorf("invalid Cursor data: %v", string(data))
	}
	return &ID{data: b}, err
}
