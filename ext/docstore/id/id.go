/*

package id implements a MongoDB ObjectId like object.

Cursor stored the timestamp and a hash.

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

func (id *ID) String() string {
	return hex.EncodeToString(id.data)
}

func (id *ID) Hash() (string, error) {
	if len(id.data) == 36 {
		return hex.EncodeToString(id.data[4:]), nil
	}
	return "", errors.New("bad data")
}

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
