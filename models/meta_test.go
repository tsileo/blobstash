package models

import (
	"testing"
	"reflect"
	"os"
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"
	"encoding/base64"
	"crypto/rand"
	mrand "math/rand"
	"github.com/bradfitz/iter"
)

const MaxRandomFileSize = 2<<20

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
	return base64.StdEncoding.EncodeToString(b)
}

func CreateRandomTree(t *testing.T, path string, rec, maxrec int) (string, int) {
	p := NewRandomDir(path)
	t.Log("Creating a new random tree at %v", p)
	nfiles := 0
	for {
		nfiles = mrand.Intn(10)
		if nfiles >= 5 {
			break
		}
	}
	cnt := 0
	for _ = range iter.N(nfiles) {
    	go NewRandomFile(p)
    	cnt++
    	if rec < maxrec && mrand.Intn(10) < 5 {
    		_, ncnt := CreateRandomTree(t, p, rec+1, maxrec)
    		cnt += ncnt
    	}
    	// Break at 50 to spend less time
    	if cnt > 50 {
    		return p, cnt
    	}
	}
	return p, cnt
}

func NewRandomTree(t *testing.T, path string, maxrec int) (string, int) {
	return CreateRandomTree(t, path, 0, maxrec)
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
	//NewRandomTree(t, ".", 3)

	rfile := NewRandomFile(".")
	defer os.Remove(rfile)
	h, err := f.PutFile(rfile)
	check(err)
	if h == "" {
		t.Errorf("Hash shouldn't be nil")
	}
	rfile2 := fmt.Sprintf("%v%v", rfile, "_restored")
	err = f.GetFile(h, rfile2)
	check(err)

	h2 := FullSHA1(rfile2)
	defer os.Remove(rfile2)
	
	if h != h2 {
		t.Error("File not restored successfully")
	}
}
