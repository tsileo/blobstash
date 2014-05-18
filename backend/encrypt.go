package backend

import (
	"sync"
	"errors"
	"bytes"
	"fmt"
	"crypto/sha1"
	"bufio"

	"github.com/tsileo/datadatabase/encrypt"

	"code.google.com/p/go.crypto/nacl/secretbox"
)

var headerSize = 59

type EncryptBackend struct {
	dest BlobHandler
	// index map the plain text hash to encrypted hash
	index map[string]string

	key *[32]byte

	sync.Mutex
}

func NewEncryptBackend(keyPath string, dest BlobHandler) *EncryptBackend {
	if err := encrypt.LoadKey(keyPath); err != nil {
		panic(err)
	}
	b := &EncryptBackend{dest: dest, index:make(map[string]string), key:&encrypt.Key}
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

func (b *EncryptBackend) Put(hash string, data []byte) (err error) {
	// #datadb/secretbox\n
	// data hash\n
	// data
	var nonce [24]byte
	//out := make([]byte, len(data) + secretbox.Overhead + 24 + headerSize)
	if err := encrypt.GenerateNonce(&nonce); err != nil {
		return err
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
	return out, nil
}

func (b *EncryptBackend) Enumerate(blobs chan<- string) error {
	defer close(blobs)
	for plainHash, _ := range b.index {
		blobs <- plainHash
	}
	return nil
}
