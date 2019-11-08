package crypto // import "a4.io/blobstash/pkg/crypto"

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"golang.org/x/crypto/nacl/secretbox"
)

// The length of the nonce used for the secretbox implementation.
const nonceLength = 24

// The length of the encryption key for the secretbox implementation.
const keyLength = 32

var (
	header = []byte("#blobstash/encrypted_blobsfile\n")
)

func Seal(nkey *[32]byte, path string) (string, error) {
	var nonce [nonceLength]byte
	if _, err := rand.Reader.Read(nonce[:]); err != nil {
		return "", err
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	tmpfile, err := ioutil.TempFile("", "blobstash_secretbox")
	if err != nil {
		return "", err
	}
	if _, err := tmpfile.Write(header); err != nil {
		return "", err
	}

	buf := make([]byte, 16*1024)
	chunklen := make([]byte, 4)
L:
	for {
		n, err := f.Read(buf)
		switch err {
		case nil:
		case io.EOF:
			break L
		default:
			return "", err
		}
		dat := secretbox.Seal(nonce[:], buf[:n], &nonce, nkey)
		binary.BigEndian.PutUint32(chunklen[:], uint32(len(dat)))
		if _, err := tmpfile.Write(chunklen); err != nil {
			return "", err
		}
		if _, err := tmpfile.Write(dat); err != nil {
			return "", err
		}
	}
	if err := tmpfile.Close(); err != nil {
		return "", err
	}

	return tmpfile.Name(), nil
}

func Open(nkey *[32]byte, path string) (string, error) {
	var nonce [nonceLength]byte
	// Actually decrypt the cipher text

	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := make([]byte, len(header))
	if _, err := f.Read(h); err != nil {
		return "", err
	}
	if !bytes.Equal(header, h) {
		return "", fmt.Errorf("invalid header %v", h)
	}
	tmpfile, err := ioutil.TempFile("", "blobstash_secretbox")
	if err != nil {
		return "", err
	}

	chunklenBytes := make([]byte, 4)
	var chunklen uint32
L:
	for {
		_, err = f.Read(chunklenBytes)
		switch err {
		case nil:
		case io.EOF:
			break L
		default:
			return "", err
		}
		chunklen = binary.BigEndian.Uint32(chunklenBytes[:])
		chunk := make([]byte, chunklen)
		_, err = f.Read(chunk)
		if err != nil {
			return "", err
		}

		copy(nonce[:], chunk[:24])
		decrypted, ok := secretbox.Open(nil, chunk[24:], &nonce, nkey)
		if !ok {
			panic("decryption error")
		}

		if _, err = tmpfile.Write(decrypted); err != nil {
			return "", err
		}

	}

	if err = tmpfile.Close(); err != nil {
		return "", err
	}

	return tmpfile.Name(), nil
}
