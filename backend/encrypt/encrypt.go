/*

Package encrypt implement a backend that encrypt/decrypt on the fly (using nacl/secretbox [1])
and store blobs in the "dest" backend.

Links

	[1] godoc.org/code.google.com/p/go.crypto/nacl/secretbox

*/
package encrypt

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"errors"
	"expvar"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/tsileo/blobstash/backend"

	"github.com/dchest/blake2b"

	"code.google.com/p/snappy-go/snappy"
	"golang.org/x/crypto/nacl/secretbox"
)

var (
	bytesUploaded   = expvar.NewMap("encrypt-bytes-uploaded")
	bytesDownloaded = expvar.NewMap("encrypt-bytes-downloaded")
	blobsUploaded   = expvar.NewMap("encrypt-blobs-uploaded")
	blobsDownloaded = expvar.NewMap("encrypt-blobs-downloaded")
)

var headerSize = 86

func GenerateNonce(nonce *[24]byte) (err error) {
	_, err = io.ReadFull(rand.Reader, nonce[:])
	return
}

type EncryptBackend struct {
	dest backend.BlobHandler
	// index map the plain text hash to encrypted hash
	index map[string]string

	// holds the encryption key
	key *[32]byte

	sync.Mutex
}

// New return a backend that encrypt/decrypt blobs on the fly,
// blobs are compressed with snappy before encryption with nacl/secretbox.
// At startup it scan encrypted blobs to discover the plain hash (the hash of the plain-text/unencrypted data).
// Blobs are stored in the following format:
//
// #blobstash/secretbox\n
// [plain hash]\n
// [encrypted data]
//
func New(keyPath string, dest backend.BlobHandler) *EncryptBackend {
	log.Printf("EncryptBackend: starting with dest %v", dest.String())
	if err := LoadKey(keyPath); err != nil {
		panic(err)
	}
	log.Printf("EncryptBackend: loaded key at %v", keyPath)
	b := &EncryptBackend{dest: dest, index: make(map[string]string), key: &Key}
	log.Printf("EncryptBackend: backend id => %v", b.String())
	log.Println("EncryptBackend: scanning blobs to discover plain-text blobs hashes")
	blobsCnt := 0
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
		blobsCnt++
	}
	if err := <-errs; err != nil {
		if err == backend.ErrWriteOnly {
			log.Printf("EncryptBackend: no scan in write-only mode")
		} else {
			panic(err)
		}
	}
	log.Printf("EncryptBackend: %v blobs successfully scanned", blobsCnt)
	return b
}

func (backend *EncryptBackend) String() string {
	return fmt.Sprintf("encrypt-%v", backend.dest.String())
}

func (b *EncryptBackend) Put(hash string, rawData []byte) (err error) {
	// #blobstash/secretbox\n
	// data hash\n
	// data
	var nonce [24]byte
	//out := make([]byte, len(data) + secretbox.Overhead + 24 + headerSize)
	if err := GenerateNonce(&nonce); err != nil {
		return err
	}
	// First we compress the data with snappy
	data, err := snappy.Encode(nil, rawData)
	if err != nil {
		return
	}
	var out bytes.Buffer
	out.WriteString("#blobstash/secretbox\n")
	out.WriteString(fmt.Sprintf("%v\n", hash))
	encData := make([]byte, len(data)+secretbox.Overhead)
	secretbox.Seal(encData[0:0], data, &nonce, b.key)
	out.Write(nonce[:])
	out.Write(encData)
	encHash := fmt.Sprintf("%x", blake2b.Sum256(out.Bytes()))
	b.dest.Put(encHash, out.Bytes())
	b.Lock()
	b.index[hash] = encHash
	defer b.Unlock()
	blobsUploaded.Add(b.dest.String(), 1)
	bytesUploaded.Add(b.dest.String(), int64(len(out.Bytes())))
	return
}

func (b *EncryptBackend) Exists(hash string) bool {
	b.Lock()
	defer b.Unlock()
	_, exists := b.index[hash]
	return exists
}

func (b *EncryptBackend) Done() error {
	return nil
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
	if scanner.Text() != "#blobstash/secretbox" {
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
	encData := make([]byte, len(box)-24)
	copy(nonce[:], box[:24])
	copy(encData[:], box[24:])
	out := make([]byte, len(box)-24)
	out, success := secretbox.Open(nil, encData, &nonce, b.key)
	if !success {
		return data, fmt.Errorf("failed to decrypt blob %v/%v", hash, ref)
	}
	// Decode snappy data
	data, err = snappy.Decode(nil, out)
	if err != nil {
		return data, fmt.Errorf("failed to decode blob %v/%v", hash, ref)
	}
	blobsDownloaded.Add(b.dest.String(), 1)
	bytesDownloaded.Add(b.dest.String(), int64(len(enc)))
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
