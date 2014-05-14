package client

import (
	"testing"
	"reflect"
	"github.com/garyburd/redigo/redis"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func TestModelsBackup(t *testing.T) {
	pool, err := GetDbPool()
	check(err)
	con := pool.Get()
	defer con.Close()
	txId, err := redis.String(con.Do("TXINIT"))
	check(err)
	//f := &Backup{Name:"foo", Type:"file", Ref:"bar"}
	f := NewBackup("foo", "file", "bar")
	h, err := f.Save(txId, pool)
	check(err)
	_, err = con.Do("TXCOMMIT")
	check(err)

	f2, err := NewBackupFromDB(pool, h)
	check(err)
	if !reflect.DeepEqual(f, f2) {
		t.Errorf("Error retrieving file from DB, expected %+v, get %+v", f, f2)
	}
}
