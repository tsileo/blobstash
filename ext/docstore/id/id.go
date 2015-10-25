/*

package id implements a MongoDB ObjectId like object.

Cursor stored the timestamp and a hash.

*/
package cursor

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
	binary.BigEndian.PutUint32(b[:], uint32(ts))
	if hash != "" {
		copy(b[4:], []byte(hash))
	}
	return &ID{b}, nil
}

func (id *ID) String() string {
	return hex.EncodeToString(id.data)
}

func (id *ID) Hash() (string, error) {
	if len(id.data) == 36 {
		return string(id.data[4:])
	}
	return "", errors.New("bad data")
}

func (id *ID) Ts() int {
	return int(binary.BigEndian.Uint32(c.data[0:4]))
}

func (c *Cursor) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%v"`, hex.EncodeToString(c.data))), nil
}

// FIXME(ts) finish the port from uuid to hash
func (c *Cursor) UnmarshalJSON(data []byte) error {
	if len(data) != 42 {
		return fmt.Errorf("invalid Cursor data: %v", string(data))
	}
	b := make([]byte, 20)
	if _, err := hex.Decode(b, data[1:41]); err != nil {
		return fmt.Errorf("invalid Cursor data: %v", string(data))
	}
	*c = Cursor{b}
	return nil
}

func FromHex(data string) (*Cursor, error) {
	if len(data) != 40 {
		return nil, fmt.Errorf("invalid Cursor data: %v", string(data))
	}
	b, err := hex.DecodeString(data)
	if err != nil {
		return nil, fmt.Errorf("invalid Cursor data: %v", string(data))
	}
	return &Cursor{b}, err
}
