package main

import (
	"log"
	"os"
	_ "math"
	"bytes"
	"crypto/sha1"
	"path/filepath"
	_ "reflect"
	"fmt"
	"io/ioutil"
	"github.com/tsileo/silokv/rolling"
)


type Blob struct {
	buf *bytes.Buffer
}

func (b *Blob) Write(data []byte) (n int, err error) {
	return b.buf.Write(data)
}

func (b *Blob) Bytes() []byte {
	return b.buf.Bytes()
}

func (b *Blob) Reset() {
	b.buf.Reset()
}

func (b *Blob) Len() int {
	return b.buf.Len()
}

func (b *Blob) Hash() string {
	h := sha1.New()
	h.Write(b.Bytes())
	return fmt.Sprintf("%x", h.Sum(nil))
}

func NewBlob() *Blob {
	var buf bytes.Buffer
	return &Blob{buf:&buf}
}

type BlobHandler struct {
	Backend Backend
}

func NewBlobHandler(backend Backend) *BlobHandler {
	bh := &BlobHandler{backend}
	return bh
}

func (bh *BlobHandler) Put(blob *Blob) (hash string, exists bool, err error) {
	hash = blob.Hash()
	exists = bh.Backend.Exists(hash)
	if !exists {
		err = bh.Backend.Put(hash, blob.Bytes())	
	}
	return
}

func (bh *BlobHandler)  Get(hash string) (blob *Blob, err error) {
	data, err := bh.Backend.Get(hash)
	blob = NewBlob()
	blob.Write(data)
	return
}

func main() {
	log.Println("Start")
	
	window := 64
	//var blockSize uint32 = 1024 * 32
	//var target uint32 = math.MaxUint32 / blockSize

	rs := rolling.New(window)

	f, _ := os.Open("/box/deluge/Asterix.Aux.Jeux.Olympiques.FRENCH.DVDRiP.REPACK.1CD.XViD-RePaCk.avi")

	i := 0
	cnt := 0
	buf := NewBlob()
	buf.Reset()
	lb := NewLocalBackend("/tmp/testrollsum4")
	bh := NewBlobHandler(lb)
	log.Printf("%+v", bh)
	for {
		//split := false
		b := make([]byte, 1)
		_, err := f.Read(b)
		//_, err := io.CopyN(rs, f, 1)
		rs.Write(b)
		buf.Write(b)
		onSplit := rs.OnSplit()
		if (onSplit && (buf.Len() > 64 << 10)) || buf.Len() >= 1 << 20 {
			//split := true
			log.Printf("sum at %v / buflen:%v\n", i, buf.Len())
			ch, cex, cerr := bh.Put(buf)
			log.Printf("%+v\n%+v\n%+v\n", ch, cex, cerr)
			buf.Reset()
			cnt++
			i = 0
		} else {
			i++	
		}
		
		if err != nil {
			break
		}
	}
	log.Printf("%+v\n", cnt)
}
