package db

import (
	"testing"
)

func TestDBListDataType(t *testing.T) {
	db, err := New("test_db_list")
	if err != nil {
		panic("db error")
	}
	defer func() {
		db.Close()
		db.Destroy()
	}()
}