/*

package id implements a MongoDB ObjectId like object.

Cursor stored the timestamp and a hash.

	(<timestamp encoded in big endian uint32> 4 bytes) + (4 random bytes / 4 bytes counter starting with random value) = 12 bytes

	24 bytes hex-encoded
*/
package id

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

var counter = []byte("")

func init() {
	counter = make([]byte, 4)
	if _, err := rand.Read(counter); err != nil {
		panic(err)
	}
}

func nextCounter() []byte {
	i := len(counter)
	for i > 0 {
		i--
		counter[i]++
		if counter[i] != 0 {
			break
		}
	}
	return counter
}

// Cursor hold a hex/byte representation of a timestamp and a hash
type ID struct {
	data []byte
}

func New(ts int) (*ID, error) {
	b := make([]byte, 12)
	binary.BigEndian.PutUint32(b[:], uint32(ts))
	randomCompoment := make([]byte, 4)
	if _, err := rand.Read(randomCompoment); err != nil {
		return nil, err
	}
	copy(b[4:], randomCompoment[:])
	copy(b[8:], nextCounter())
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

// Ts returns the timestamp component
func (id *ID) Ts() int {
	return int(binary.BigEndian.Uint32(id.data[0:4]))
}

func (id *ID) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%v"`, hex.EncodeToString(id.data))), nil
}

// FIXME(ts) finish the port from uuid to hash
func (id *ID) UnmarshalJSON(data []byte) error {
	if len(data) != 26 {
		return fmt.Errorf("invalid Cursor data: %v", string(data))
	}
	b := make([]byte, 24)
	if _, err := hex.Decode(b, data[1:13]); err != nil {
		return fmt.Errorf("invalid Cursor data: %v", string(data))
	}
	*id = ID{b}
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
	return &ID{b}, err
}
