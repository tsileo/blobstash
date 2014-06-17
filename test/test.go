/*

Package tests implements helpers to run integration tests.

*/

package test

import (
	"crypto/rand"
	"github.com/bradfitz/iter"
	"io"
	"io/ioutil"
	"log"
	mrand "math/rand"
	"os"
	"fmt"
	"path/filepath"
	"sync"
	"crypto/sha1"
	"testing"
)

const MaxRandomFileSize = 2 << 19

// SHA1 is a helper to quickly compute the SHA1 hash of a []byte.
func SHA1(data []byte) string {
	h := sha1.New()
	h.Write(data)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func NewRandomFile(path string) string {
	filename := NewRandomName()
	buf := make([]byte, mrand.Intn(MaxRandomFileSize))
	rand.Read(buf)
	f := filepath.Join(path, filename)
	ioutil.WriteFile(f, buf, 0700)
	return f
}

func NewRandomDir(path string) string {
	dirname := NewRandomName()
	p := filepath.Join(path, dirname)
	os.Mkdir(p, 0700)
	return p
}

func NewRandomName() string {
	c := 12
	b := make([]byte, c)
	n, err := io.ReadFull(rand.Reader, b)
	if n != len(b) || err != nil {
		panic(err)
	}
	return SHA1(b)
}

func NewRandomFileWg(path string, wg *sync.WaitGroup) string {
	defer wg.Done()
	return NewRandomFile(path)
}

func CreateRandomTree(t *testing.T, path string, rec, maxrec int) (string, int) {
	p := NewRandomDir(path)
	if rec == 0 {
		log.Printf("Creating a new random tree at %v", p)
	}
	nfiles := 0
	for {
		nfiles = mrand.Intn(10)
		if nfiles >= 3 {
			break
		}
	}
	cnt := 0
	var wg sync.WaitGroup
	for _ = range iter.N(nfiles) {
		wg.Add(1)
		go NewRandomFileWg(p, &wg)
		cnt++
		if rec < maxrec && mrand.Intn(10) < 5 {
			_, ncnt := CreateRandomTree(t, p, rec+1, maxrec)
			cnt += ncnt
		}
		// Break at 50 to spend less time
		if cnt > 30 {
			return p, cnt
		}
	}
	wg.Wait()
	if rec == 0 {
		log.Printf("Done")
	}
	return p, cnt
}

func NewRandomTree(t *testing.T, path string, maxrec int) string {
	tpath, _ := CreateRandomTree(t, path, 0, maxrec)
	return tpath
}
