package models

import (
	"testing"
	"reflect"
	"os"
	"io"
	"io/ioutil"
	"path/filepath"
	"encoding/base64"
	"crypto/rand"
	mrand "math/rand"
	"log"
	"github.com/bradfitz/iter"
)

func NewRandomFile(path string, size int) string {
	filename := NewRandomName()
	buf := make([]byte, size)
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
	return base64.StdEncoding.EncodeToString(b)
}

func NewRandomTree(path string, rec, maxrec int) (string, int) {
	p := NewRandomDir(path)
	nfiles := 0
	for {
		nfiles = mrand.Intn(10)
		if nfiles >= 5 {
			break
		}
	}
	cnt := 0
	log.Printf("nfiles:%v,rec:%v", nfiles, rec)
	for _ = range iter.N(nfiles) {
    	NewRandomFile(p, mrand.Intn(2000000))
    	cnt++
    	if rec < maxrec && mrand.Intn(10) <= 5 {
    		_, ncnt := NewRandomTree(p, rec+1, maxrec)
    		cnt += ncnt
    	}
    	if cnt > 50 {
    		return p, cnt
    	}
	}
	return p, cnt
} 

func TestModelsMeta(t *testing.T) {
	pool, err := GetDbPool()
	check(err)
	f := &Meta{Name:"foo", Type:"Meta", Hash:"foo_meta"}
	err = f.Save(pool)
	check(err)

	fe, err := NewMetaFromDB(pool, "foo_meta")
	check(err)
	f.Hash = ""
	if !reflect.DeepEqual(f, fe) {
		t.Errorf("Error retrieving Meta from DB, expected %+v, get %+v", f, fe)
	}
	NewRandomTree(".", 1, 3)

	h, err := f.PutFile("./../test_data/file")
	check(err)
	if h == "" {
		t.Errorf("Hash shouldn't be nil")
	}
	err = f.GetFile(h, "./../test_data/restored_file")
	check(err)

	h2 := FullSHA1("./../test_data/restored_file")
	defer os.Remove("./../test_data/restored_file")
	
	if h != h2 {
		t.Error("File not restored successfully")
	}
}
