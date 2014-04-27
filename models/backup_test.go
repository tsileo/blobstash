package models

import (
	"testing"
	"reflect"
	"time"
	"github.com/tsileo/datadatabase/server"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func TestModelsBackup(t *testing.T) {
	go server.New()
	time.Sleep(3 * time.Second)
	pool, err := GetDbPool()
	check(err)
	f := &File{Name:"foo", Type:"file", Data:"bar", Ts:10}
	err = f.Save(pool, "foo")
	check(err)

	f2, err := NewFromDB(pool, "foo")
	check(err)
	if !reflect.DeepEqual(f, f2) {
		t.Errorf("Error retrieving file from DB, expected %+v, get %+v", f, f2)
	}
}
