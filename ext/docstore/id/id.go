/*

package id implements a MongoDB ObjectId like object.

Cursor stored the timestamp and a hash.

	(<timestamp encoded in big endian uint32> 4 bytes) + (<hash> 32 bytes) = 36 bytes

*/
package id

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
)

// Cursor hold a hex/byte representation of a timestamp and a hash
type ID struct {
	data []byte
}

func New(ts int, hash string) (*ID, error) {
	b := make([]byte, 36)
	bhash, err := hex.DecodeString(hash)
	if err != nil {
		return nil, err
	}
	binary.BigEndian.PutUint32(b[:], uint32(ts))
	if hash != "" {
		copy(b[4:], bhash[:])
	}
	return &ID{b}, nil
}

// Raw returns the raw cursor
func (id *ID) Raw() []byte {
	return id.data
}

// String implements Stringer interface
func (id *ID) String() string {
	return hex.EncodeToString(id.data)
}

// Hash returns the hash component
func (id *ID) Hash() (string, error) {
	if len(id.data) == 36 {
		return hex.EncodeToString(id.data[4:]), nil
	}
	return "", errors.New("bad data")
}

// Ts returns the timestamp component
func (id *ID) Ts() int {
	return int(binary.BigEndian.Uint32(id.data[0:4]))
}

func (id *ID) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%v"`, hex.EncodeToString(id.data))), nil
}

// FIXME(ts) finish the port from uuid to hash
func (id *ID) UnmarshalJSON(data []byte) error {
	if len(data) != 74 {
		return fmt.Errorf("invalid Cursor data: %v", string(data))
	}
	b := make([]byte, 36)
	if _, err := hex.Decode(b, data[1:73]); err != nil {
		return fmt.Errorf("invalid Cursor data: %v", string(data))
	}
	*id = ID{b}
	return nil
}

// FromHex build an `ID` from an hex encoded string
func FromHex(data string) (*ID, error) {
	if len(data) != 72 {
		return nil, fmt.Errorf("invalid Cursor data: %v", string(data))
	}
	b, err := hex.DecodeString(data)
	if err != nil {
		return nil, fmt.Errorf("invalid Cursor data: %v", string(data))
	}
	return &ID{b}, err
}
