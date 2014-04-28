package models

import (
	"testing"
	"log"
	"os"
)

func TestClientDir(t *testing.T) {
	c, err := NewClient()
 	check(err)
	tdir := NewRandomTree(t, ".", 1)
	defer os.RemoveAll(tdir) 
	meta, drw, err := c.PutDir(tdir)
	log.Printf("meta: %+v, dir rw: %+v", meta, drw)
	check(err)
	rr, err := c.GetDir(meta.Hash, meta.Name + "_restored")
	check(err)
	log.Printf("rr:%+v", rr)
}
