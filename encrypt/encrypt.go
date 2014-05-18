package encrypt

import (
	"io"
	"crypto/rand"
)

func GenerateNonce(nonce *[24]byte) (err error) {
	_, err = io.ReadFull(rand.Reader, nonce[:])
	return
}
