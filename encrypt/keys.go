package encrypt

import (
	"crypto/rand"
	"errors"
	"os"
	"io"
	"io/ioutil"
)

// Secret key
var Key [32]byte

// LoadKey reads the key file and load the key into Key.
func LoadKey(keyPath string) error {
	data, err := ioutil.ReadFile(keyPath)
	if err != nil {
		return err
	}
	if len(data) != 32 {
		return errors.New("bad key length")
	}
	copy(Key[:], data[:])
	return nil
}

// GenerateKey generate a new random key and store it at keyPath. 
func GenerateKey(keyPath string) error {
	var buf [32]byte
	if _, err := io.ReadFull(rand.Reader, buf[:]); err != nil {
		return err
	}
	// Check that a key doesn't exists yet.
	if _, err := os.Stat(keyPath); err == nil {
    	return errors.New("A key already exists")
	}
	// Write the key
	if err := ioutil.WriteFile(keyPath, buf[:], 0400); err != nil {
		return err
	}
	return nil
}
