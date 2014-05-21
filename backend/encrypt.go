/*

Package encrypt implement a backend that encrypt/decrypt on the fly (using nacl/secretbox [1])
and store blobs in the "dest" backend.

Links

	[1] godoc.org/code.google.com/p/go.crypto/nacl/secretbox

*/
package backend

import (
	"sync"
	"errors"
	"bytes"
	"fmt"
	"crypto/sha1"
	"bufio"
	"log"

	"github.com/tsileo/datadatabase/encrypt"

	"code.google.com/p/go.crypto/nacl/secretbox"
	"code.google.com/p/snappy-go/snappy"
)

var headerSize = 59

type EncryptBackend struct {
	dest BlobHandler
	// index map the plain text hash to encrypted hash
	index map[string]string

	// holds the encryption key
	key *[32]byte

	sync.Mutex
}

// NewEncryptBackend return a backend that encrypt/decrypt blobs on the fly,
// blobs are compressed with snappy before encryption with nacl/secretbox.
// At startup it scan encrypted blobs to discover the plain hash (the hash of the plain-text/unencrypted data).
// Blobs are stored in the following format:
//
// #datadb/secretbox\n
// [plain hash]\n
// [encrypted data]
//
func NewEncryptBackend(keyPath string, dest BlobHandler) *EncryptBackend {
	log.Println("EncryptBackend: starting")
	if err := encrypt.LoadKey(keyPath); err != nil {
		panic(err)
	}
	b := &EncryptBackend{dest: dest, index:make(map[string]string), key:&encrypt.Key}
	log.Println("EncryptBackend: scanning blobs to discover plain-text blobs hashes")
	// Scan the blobs to discover the plain text blob hashes and build the in-memory index
	hashes := make(chan string)
	errs := make(chan error)
	go func() {
		errs <- b.dest.Enumerate(hashes)
	
	}()
	for hash := range hashes {
		scanner := b.scanner(hash)
		plainHash, err := scanHash(scanner)
		if err != nil {
			panic(err)
			//return errors.New(fmt.Sprintf("Error reading plain hash from %v, %v", hash, err))
		}
		b.index[plainHash] = hash
	}
	if err := <-errs; err != nil {
		panic(err)
		//return err
	}
	return b
}

func (b *EncryptBackend) Put(hash string, rawData []byte) (err error) {
	// #datadb/secretbox\n
	// data hash\n
	// data
	var nonce [24]byte
	//out := make([]byte, len(data) + secretbox.Overhead + 24 + headerSize)
	if err := encrypt.GenerateNonce(&nonce); err != nil {
		return err
	}
	// First we compress the data with snappy
	data, err := snappy.Encode(nil, rawData)
	if err != nil {
		return
	}
	var out bytes.Buffer
	out.WriteString("#datadb/secretbox\n")
	out.WriteString(fmt.Sprintf("%v\n", hash))
	encData := make([]byte, len(data) + secretbox.Overhead)
	secretbox.Seal(encData[0:0], data, &nonce, b.key)	
	out.Write(nonce[:])
	out.Write(encData)
	encSha1 := sha1.New()
	encSha1.Write(out.Bytes())
	encHash := fmt.Sprintf("%x", encSha1.Sum(nil))
	b.dest.Put(encHash ,out.Bytes())
	b.Lock()
	b.index[hash] = encHash
	defer b.Unlock()
	return
}

func (b *EncryptBackend) Exists(hash string) bool {
	b.Lock()
	defer b.Unlock()
	_, exists := b.index[hash]
	return exists
}

func (b *EncryptBackend) scanner(hash string) *bufio.Scanner {
	enc, err := b.dest.Get(hash)
	if err != nil {
		return nil
	}
	buf := bytes.NewBuffer(enc)
	return bufio.NewScanner(buf)
}

func scanHash(scanner *bufio.Scanner) (hash string, err error) {
	if !scanner.Scan() {
		return "", errors.New("No line to read")
	}
	if scanner.Text() != "#datadb/secretbox" {
		return "", errors.New("bad header")
	}
	if !scanner.Scan() {
		return "", errors.New("ref not found")
	}
	return scanner.Text(), nil
}

func (b *EncryptBackend) Get(hash string) (data []byte, err error) {
	ref, _ := b.index[hash]
	enc, err := b.dest.Get(ref)
	if err != nil {
		return data, err
	}
	box := enc[headerSize:]
	var nonce [24]byte
	encData := make([]byte, len(box) - 24)
	copy(nonce[:], box[:24])
	copy(encData[:], box[24:])
	out := make([]byte, len(box) - 24)
	out, success := secretbox.Open(nil, encData, &nonce, b.key)
	if !success {
		return data, fmt.Errorf("failed to decrypt blob %v/%v", hash, ref)
	}
	// Decode snappy data
	data, err = snappy.Decode(nil, out)
	if err != nil {
		return data, fmt.Errorf("failed to decode blob %v/%v", hash, ref)
	}
	return 
}

func (b *EncryptBackend) Enumerate(blobs chan<- string) error {
	defer close(blobs)
	for plainHash, _ := range b.index {
		blobs <- plainHash
	}
	return nil
}

func (b *EncryptBackend) Close() {
	b.dest.Close()
}
