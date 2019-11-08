package crypto

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"io/ioutil"
	"os"
	"testing"
)

func TestCrypto(t *testing.T) {
	secretKeyBytes, err := hex.DecodeString("6368616e676520746869732070617373776f726420746f206120736563726574")
	if err != nil {
		panic(err)
	}

	dat := make([]byte, 3<<20)
	if _, err := rand.Reader.Read(dat[:]); err != nil {
		panic(err)
	}
	tmpfile, err := ioutil.TempFile("", "blobstash_secretbox")
	if err != nil {
		panic(err)
	}
	if _, err := tmpfile.Write(dat); err != nil {
		panic(err)
	}
	if err := tmpfile.Close(); err != nil {
		panic(err)
	}
	defer os.Remove(tmpfile.Name())

	var secretKey [32]byte
	copy(secretKey[:], secretKeyBytes)

	sealed, err := Seal(&secretKey, tmpfile.Name())
	if err != nil {
		panic(err)
	}
	defer os.Remove(sealed)
	t.Logf("sealed=%v", sealed)
	unsealed, err := Open(&secretKey, sealed)
	if err != nil {
		panic(err)
	}
	defer os.Remove(unsealed)
	t.Logf("unsealed=%v", unsealed)
	dat2, err := ioutil.ReadFile(unsealed)
	if err != nil {
		panic(err)
	}
	if !bytes.Equal(dat, dat2) {
		t.Errorf("failed to decrypt input")
	}
}
