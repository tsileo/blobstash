package db

import (
	"crypto/rand"
	"encoding/base64"
	"io"
)

func NewId() string {
	c := 12
	b := make([]byte, c)
	n, err := io.ReadFull(rand.Reader, b)
	if n != len(b) || err != nil {
		panic(err)
	}
	return base64.StdEncoding.EncodeToString(b)
}
