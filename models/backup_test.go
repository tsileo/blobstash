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
	f := &Backup{Name:"foo", Type:"file", Data:"bar", Ts:10}
	err = f.Save(pool, "foo")
	check(err)

	f2, err := NewBackupFromDB(pool, "foo")
	check(err)
	if !reflect.DeepEqual(f, f2) {
		t.Errorf("Error retrieving file from DB, expected %+v, get %+v", f, f2)
	}
}
