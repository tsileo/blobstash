package models

import (
	"testing"
	"reflect"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func TestModelsBackup(t *testing.T) {
	pool, err := GetDbPool()
	check(err)
	//f := &Backup{Name:"foo", Type:"file", Ref:"bar"}
	f := NewBackup("foo", "file", "bar")
	h, err := f.Save(pool)
	check(err)

	f2, err := NewBackupFromDB(pool, h)
	check(err)
	if !reflect.DeepEqual(f, f2) {
		t.Errorf("Error retrieving file from DB, expected %+v, get %+v", f, f2)
	}
}
