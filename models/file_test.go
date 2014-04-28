package models

import (
	"testing"
	"os"
	"fmt"
)

func TestClientFile(t *testing.T) {
	c, err := NewClient()
 	check(err)
	rfile := NewRandomFile(".")
	defer os.Remove(rfile)
	th := FullSHA1(rfile)
	_, h, err := c.PutFile(rfile)
	check(err)
	if h.Hash != th {
		t.Errorf("File not put successfully")
	}

	rfile2 := fmt.Sprintf("%v%v", rfile, "_restored")
	_, err = c.GetFile(h.Hash, rfile2)
	check(err)

	h2 := FullSHA1(rfile2)
	defer os.Remove(rfile2)
	
	if th != h2 {
		t.Error("File not restored successfully")
	}
}
